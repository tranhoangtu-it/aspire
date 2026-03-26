// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

#pragma warning disable ASPIREEXTENSION001
#pragma warning disable ASPIRECERTIFICATES001
#pragma warning disable ASPIRECONTAINERSHELLEXECUTION001

using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Aspire.Dashboard.Model;
using Aspire.Hosting.ApplicationModel;
using Aspire.Hosting.Dcp.Model;
using Aspire.Hosting.Eventing;
using Aspire.Hosting.Utils;
using Json.Patch;
using k8s;
using k8s.Autorest;
using k8s.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;

namespace Aspire.Hosting.Dcp;

internal sealed partial class DcpExecutor : IDcpExecutor, IAsyncDisposable
{
    internal const string DebugSessionPortVar = "DEBUG_SESSION_PORT";

    // The base name for ephemeral container (Docker, Podman etc) networks
    internal const string DefaultAspireNetworkName = "aspire-session-network";

    // The base name for persistent container (Docker, Podman etc) networks
    internal const string DefaultAspirePersistentNetworkName = "aspire-persistent-network";

    // Disposal of the DcpExecutor means shutting down watches and log streams,
    // and asking DCP to start the shutdown process. If we cannot complete these tasks within 10 seconds,
    // it probably means DCP crashed and there is no point trying further.
    private static readonly TimeSpan s_disposeTimeout = TimeSpan.FromSeconds(10);

    // Regex for normalizing application names.
    [GeneratedRegex("""^(?<name>.+?)\.?AppHost$""", RegexOptions.ExplicitCapture | RegexOptions.IgnoreCase | RegexOptions.Singleline | RegexOptions.CultureInvariant)]
    private static partial Regex ApplicationNameRegex();

    private readonly ILogger<DistributedApplication> _distributedApplicationLogger;
    private readonly IKubernetesService _kubernetesService;
    private readonly IConfiguration _configuration;
    private readonly ResourceLoggerService _loggerService;
    private readonly IDcpDependencyCheckService _dcpDependencyCheckService;
    private readonly DcpNameGenerator _nameGenerator;
    private readonly ILogger<DcpExecutor> _logger;
    private readonly DistributedApplicationModel _model;
    private readonly DistributedApplicationOptions _distributedApplicationOptions;
    private readonly IDistributedApplicationEventing _distributedApplicationEventing;
    private readonly IOptions<DcpOptions> _options;
    private readonly DistributedApplicationExecutionContext _executionContext;
    private readonly ConcurrentBag<AppResource> _appResources = [];

    // Has an entry if we raised ResourceEndpointsAllocatedEvent for a resource with a given name.
    // We want to ensure we raise the event only once for each app model resource. 
    // There may be multiple physical replicas of the same app model resource 
    // which can result in the event being raised multiple times if we are not careful.
    private readonly HashSet<string> _endpointsAdvertised = new(StringComparers.ResourceName);

    private readonly CancellationTokenSource _shutdownCancellation = new();
    private readonly DcpExecutorEvents _executorEvents;
    private readonly Locations _locations;
    private readonly DcpResourceWatcher _resourceWatcher;

    private readonly string _normalizedApplicationName;
    private readonly ExecutableCreator _executableCreator;
    private readonly ContainerCreator _containerCreator;

    // Internal for testing.
    internal ResiliencePipeline<bool> DeleteResourceRetryPipeline { get; set; }

    private DcpInfo? _dcpInfo;
    private int _stopped;

    public DcpExecutor(ILogger<DcpExecutor> logger,
                       ILogger<DistributedApplication> distributedApplicationLogger,
                       DistributedApplicationModel model,
                       IHostEnvironment hostEnvironment,
                       IKubernetesService kubernetesService,
                       IConfiguration configuration,
                       IDistributedApplicationEventing distributedApplicationEventing,
                       DistributedApplicationOptions distributedApplicationOptions,
                       IOptions<DcpOptions> options,
                       DistributedApplicationExecutionContext executionContext,
                       ResourceLoggerService loggerService,
                       IDcpDependencyCheckService dcpDependencyCheckService,
                       DcpNameGenerator nameGenerator,
                       DcpExecutorEvents executorEvents,
                       Locations locations,
                       ExecutableCreator executableCreator,
                       ContainerCreator containerCreator)
    {
        _distributedApplicationLogger = distributedApplicationLogger;
        _kubernetesService = kubernetesService;
        _configuration = configuration;
        _loggerService = loggerService;
        _dcpDependencyCheckService = dcpDependencyCheckService;
        _nameGenerator = nameGenerator;
        _executorEvents = executorEvents;
        _logger = logger;
        _model = model;
        _distributedApplicationEventing = distributedApplicationEventing;
        _distributedApplicationOptions = distributedApplicationOptions;
        _options = options;
        _executionContext = executionContext;
        _normalizedApplicationName = NormalizeApplicationName(hostEnvironment.ApplicationName);
        _locations = locations;

        _resourceWatcher = new DcpResourceWatcher(logger, kubernetesService, loggerService, executorEvents, model, _appResources, _shutdownCancellation.Token);

        DeleteResourceRetryPipeline = DcpPipelineBuilder.BuildDeleteRetryPipeline(logger);

        _executableCreator = executableCreator;
        _containerCreator = containerCreator;
        _executableCreator.Initialize(this);
        _containerCreator.Initialize(this);
    }

    // ── IDcpExecutor new members ──

    ConcurrentBag<AppResource> IDcpExecutor.AppResources => _appResources;
    CancellationToken IDcpExecutor.ShutdownToken => _shutdownCancellation.Token;
    ResourceSnapshotBuilder IDcpExecutor.SnapshotBuilder => _resourceWatcher.SnapshotBuilder;

    private string ContainerHostName => _configuration["AppHost:ContainerHostname"] ??
        (_options.Value.EnableAspireContainerTunnel ? KnownHostNames.DefaultContainerTunnelHostName : _dcpInfo?.Containers?.HostName ?? KnownHostNames.DockerDesktopHostBridge);

    public async Task RunApplicationAsync(CancellationToken ct = default)
    {
        _dcpInfo = await _dcpDependencyCheckService.GetDcpInfoAsync(cancellationToken: ct).ConfigureAwait(false);

        Debug.Assert(_dcpInfo is not null, "DCP info should not be null at this point");

        // TODO: in the current Aspire implementation there a requirement that Executables and Containers backing Aspire resources
        // must be created only we created all AllocatedEndpoints these resource needed (e.g. for resolving environment variable values etc)
        // This is why we create objects in very specific order here.
        //
        // In future we should be able to make the model more flexible and streamline the DCP object creation logic by:
        // 1. Asynchronously publish AllocatedEndpoints as the Services associated with them transition to Ready state.
        // 2. Asynchronously create Executables and Containers as soon as all their dependencies are ready.

        try
        {
            AspireEventSource.Instance.DcpModelCreationStart();

            _containerCreator.PrepareContainerNetworks();

            AspireEventSource.Instance.DcpServiceObjectPreparationStart();
            try
            {
                PrepareServices();
            }
            finally
            {
                AspireEventSource.Instance.DcpServiceObjectPreparationStop();
            }

            _containerCreator.PrepareContainers();
            _containerCreator.PrepareContainerExecutables();
            _executableCreator.PrepareExecutables();

            await _executorEvents.PublishAsync(new OnResourcesPreparedContext(ct)).ConfigureAwait(false);

            _resourceWatcher.Start();

            var createServices = Task.Run(() => CreateAllDcpObjectsAsync<Service>(ct), ct);

            var getProxyAddresses = Task.Run(async () =>
            {
                await createServices.ConfigureAwait(false);

                var proxiedWithNoAddress = _appResources.Where(r => r.DcpResource is Service { }).Select(r => (Service)r.DcpResource)
                .Where(sr => !sr.HasCompleteAddress && sr.Spec.AddressAllocationMode != AddressAllocationModes.Proxyless);

                await UpdateWithEffectiveAddressInfo(proxiedWithNoAddress, ct, TimeSpan.FromMinutes(1)).ConfigureAwait(false);
            }, ct);

            var createContainerNetworks = Task.Run(() => CreateAllDcpObjectsAsync<ContainerNetwork>(ct), ct);

            var executables = _appResources.OfType<RenderedModelResource>().Where(ar => ar.DcpResource is Executable).ToArray();
            var containers = _appResources.OfType<RenderedModelResource>().Where(ar => ar.DcpResource is Container).ToArray();

            var createExecutableEndpoints = Task.Run(async () =>
            {
                await getProxyAddresses.ConfigureAwait(false);

                AddAllocatedEndpointInfo(executables, AllocatedEndpointsMode.Workload);
                await PublishEndpointAllocatedEventAsync(executables, ct).ConfigureAwait(false);
            }, ct);

            var createExecutables = Task.Run(async () =>
            {
                await createExecutableEndpoints.ConfigureAwait(false);

                await CreateRenderedResourcesAsync(_executableCreator.CreateExecutableAsync, executables, Model.Dcp.ExecutableKind, ct).ConfigureAwait(false);
            }, ct);

            Task createTunnelFunc(ContainerCreationContext cctx) => Task.Run(async () =>
            {
                await Task.WhenAll([getProxyAddresses, createContainerNetworks]).WaitAsync(ct).ConfigureAwait(false);

                // Container creation tasks need to figure out dependencies of each container 
                // and then create Service and TunnelConfiguration definitions for each of them.
                cctx.ContainerServicesSpecReady.Wait(ct);
                cctx.ContainerServicesChan.Writer.Complete();

                // Now create the container network services for the host resources, update the tunnel, and advertise AllocatedEndpoints.
                var containerNetworkServices = cctx.ContainerServicesChan.Reader.ReadAllAsync(ct).ToBlockingEnumerable(ct).ToArray();
                _appResources.AddRange(containerNetworkServices.Select(cns => cns.ServiceResource));
                var serviceObjects = containerNetworkServices.Select(cns => cns.ServiceResource.Service).ToArray();
                await CreateDcpObjectsAsync(serviceObjects, ct).ConfigureAwait(false);

                var tunnels = containerNetworkServices.Where(s => s.TunnelConfig is not null).Select(s => s.TunnelConfig!).ToList();
                Debug.Assert(tunnels.Count == containerNetworkServices.Length, "Each tunneled service should have a tunnel config");
                var tunnelAppResource = await _containerCreator.CreateTunnelProxyResourceAsync(tunnels, ct).ConfigureAwait(false);
                var tunnelProxy = (ContainerNetworkTunnelProxy)tunnelAppResource.DcpResource;
                await CreateAllDcpObjectsAsync<ContainerNetworkTunnelProxy>(ct).ConfigureAwait(false);

                // Container tunnel initialization can take a while if the container tunnel image needs to be built,
                // especially if the required image pull is slow, hence 10 minute timeout here.
                await UpdateWithEffectiveAddressInfo(serviceObjects, ct, TimeSpan.FromMinutes(10)).ConfigureAwait(false);

                AddAllocatedEndpointInfo(executables, AllocatedEndpointsMode.ContainerTunnel);

                // createExecutableEndpoints() is not really part of container tunnel initialization,
                // but configuring containers that use the tunnel require these host network-side endpoints to be ready,
                // so instead of having container creation tasks wait on two separate tasks (current one + createExecutableEndpoints), 
                // we just wait for createExecutableEndpoints here, and container creation tasks can then wait on this one.
                await createExecutableEndpoints.ConfigureAwait(false);
            }, ct);

            using var cctx = new ContainerCreationContext(containers.Length, createTunnelFunc);

            var createContainers = Task.Run(async () =>
            {
                await Task.WhenAll([getProxyAddresses, createContainerNetworks]).WaitAsync(ct).ConfigureAwait(false);

                await Task.WhenAll(containers.Select(c => Task.Run(async () =>
                {
                    AspireEventSource.Instance.DcpObjectCreationStart(((Container)c.DcpResource).Kind, c.DcpResourceName);
                    try
                    {
                        await _containerCreator.CreateSingleContainerAsync(c, cctx, ct).ConfigureAwait(false);
                    }
                    finally
                    {
                        AspireEventSource.Instance.DcpObjectCreationStop(((Container)c.DcpResource).Kind, c.DcpResourceName);
                    }
                }))).WaitAsync(ct).ConfigureAwait(false);
            }, ct);

            // Now wait for all "leaf" creations to complete.
            await Task.WhenAll(createExecutables, createContainers).WaitAsync(ct).ConfigureAwait(false);

            await _executorEvents.PublishAsync(new OnEndpointsAllocatedContext(ct)).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // This is here so hosting does not throw an exception when CTRL+C during startup.
            _logger.LogDebug("Cancellation received during application startup.");
        }
        catch
        {
            _shutdownCancellation.Cancel();
            throw;
        }
        finally
        {
            AspireEventSource.Instance.DcpModelCreationStop();
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (Interlocked.CompareExchange(ref _stopped, 1, 0) != 0)
        {
            return; // Already stopped/stop in progress.
        }

        _shutdownCancellation.Cancel();

        try
        {
            await _resourceWatcher.StopAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Ignore.
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "One or more monitoring tasks terminated with an error.");
        }

        try
        {
            if (_options.Value.WaitForResourceCleanup)
            {
                try
                {
                    AspireEventSource.Instance.DcpResourceCleanupStart();
                    await _kubernetesService.CleanupResourcesAsync(cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    AspireEventSource.Instance.DcpResourceCleanupStop();
                }
            }

            // The app orchestrator (represented by kubernetesService here) will perform a resource cleanup
            // (if not done already) when the app host process exits.
            // This is just a perf optimization, so we do not care that much if this call fails.
            // There is not much difference for single app run, but for tests that tend to launch multiple instances
            // of app host from the same process, the gain from programmatic orchestrator shutdown is significant
            // See https://github.com/microsoft/aspire/issues/6561 for more info.
            await _kubernetesService.StopServerAsync(Model.ResourceCleanup.Full, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Ignore.
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Application orchestrator could not be stopped programmatically.");
        }
    }

    public async ValueTask DisposeAsync()
    {
        var disposeCts = new CancellationTokenSource();
        disposeCts.CancelAfter(s_disposeTimeout);
        await StopAsync(disposeCts.Token).ConfigureAwait(false);
    }

    /// <summary>
    /// Normalizes the application name for use in physical container resource names (only guaranteed valid as a suffix).
    /// Removes the ".AppHost" suffix if present and takes only characters that are valid in resource names.
    /// Invalid characters are simply omitted from the name as the result doesn't need to be identical.
    /// </summary>
    /// <param name="applicationName">The application name to normalize.</param>
    /// <returns>The normalized application name with invalid characters removed.</returns>
    internal static string NormalizeApplicationName(string applicationName)
    {
        if (string.IsNullOrEmpty(applicationName))
        {
            return applicationName;
        }

        applicationName = ApplicationNameRegex().Match(applicationName) switch
        {
            Match { Success: true } match => match.Groups["name"].Value,
            _ => applicationName
        };

        if (string.IsNullOrEmpty(applicationName))
        {
            return applicationName;
        }

        var normalizedName = new StringBuilder();
        for (var i = 0; i < applicationName.Length; i++)
        {
            if ((applicationName[i] is >= 'a' and <= 'z') ||
                (applicationName[i] is >= 'A' and <= 'Z') ||
                (applicationName[i] is >= '0' and <= '9') ||
                (applicationName[i] is '_' or '-' or '.'))
            {
                normalizedName.Append(applicationName[i]);
            }
        }

        return normalizedName.ToString();
    }

    internal static string GetResourceType<T>(T resource, IResource appModelResource) where T : CustomResource
    {
        return resource switch
        {
            Container => KnownResourceTypes.Container,
            Executable => appModelResource is ProjectResource ? KnownResourceTypes.Project : KnownResourceTypes.Executable,
            ContainerExec => KnownResourceTypes.ContainerExec,
            _ => throw new InvalidOperationException($"Unknown resource type {resource.GetType().Name}")
        };
    }

    // Waits till provided set of Services have their addresses allocated by the orchestrator
    // and updates them with the allocated address information.
    private async Task UpdateWithEffectiveAddressInfo(IEnumerable<Service> services, CancellationToken cancellationToken, TimeSpan? timeout = null)
    {
        List<Service> needAddressAllocated = new(services.Where(s => !s.HasCompleteAddress));
        if (needAddressAllocated.Count == 0)
        {
            return;
        }

        var createServicePipeline = DcpPipelineBuilder.BuildCreateServiceRetryPipeline(_options.Value, _logger, timeout);
        var initialServiceCount = needAddressAllocated.Count;

        try
        {
            AspireEventSource.Instance.DcpServiceAddressAllocationStart(initialServiceCount);

            await createServicePipeline.ExecuteAsync(async (attemptCancellationToken) =>
            {
                // Note: a Kubernetes watch, when started, will return at least one event per existing object, so we won't miss any service state.
                var serviceChangeEnumerator = _kubernetesService.WatchAsync<Service>(cancellationToken: attemptCancellationToken);
                await foreach (var (evt, updated) in serviceChangeEnumerator.ConfigureAwait(false))
                {
                    if (evt == WatchEventType.Bookmark)
                    {
                        // Bookmarks do not contain any data.
                        continue;
                    }

                    var srvResource = needAddressAllocated.FirstOrDefault(sr => sr.Metadata.Name == updated.Metadata.Name);
                    if (srvResource == null)
                    {
                        // This service most likely already has full address information, so it is not on needAddressAllocated list.
                        continue;
                    }

                    if (updated.HasCompleteAddress)
                    {
                        srvResource.ApplyAddressInfoFrom(updated);
                        needAddressAllocated.Remove(srvResource);
                        AspireEventSource.Instance.DcpServiceAddressAllocated(srvResource.Metadata.Name);
                    }

                    if (needAddressAllocated.Count == 0)
                    {
                        return; // We are done
                    }
                }
            }, cancellationToken).ConfigureAwait(false);

            // If there are still services that need address allocated, try a final direct query in case the watch missed some updates.
            foreach (var sar in needAddressAllocated)
            {
                var dcpSvc = await _kubernetesService.GetAsync<Service>(sar.Metadata.Name, cancellationToken: cancellationToken).ConfigureAwait(false);
                if (dcpSvc.HasCompleteAddress)
                {
                    sar.ApplyAddressInfoFrom(dcpSvc);
                }
                else
                {
                    _distributedApplicationLogger.LogWarning("Unable to allocate a network port for service '{ServiceName}'; service may be unreachable and its clients may not work properly.", sar.Metadata.Name);
                    AspireEventSource.Instance.DcpServiceAddressAllocationFailed(sar.Metadata.Name);
                }
            }

            if (_options.Value.EnableAspireContainerTunnel)
            {
                // Tunnel endpoints will be enabled (and get their endpoints) on as-needed basis. We are done for now.
                return;
            }

            // Container services are services that "mirror" their primary (host) service counterparts, but expose addresses usable from container network.
            // Without the tunnel we rely on Docker Desktop host.docker.internal bridge,
            // which means we just need to update their ports from primary services, changing the address to container host.
            var containerServices = _appResources.Where(r => r.DcpResource is Service { }).Select(r => (
                Service: r.DcpResource as Service,
                PrimaryServiceName: r.DcpResource.Metadata.Annotations?.TryGetValue(CustomResource.PrimaryServiceNameAnnotation, out var psn) == true ? psn : null)
            )
            .Where(cs => !string.IsNullOrEmpty(cs.PrimaryServiceName) && cs.Service?.HasCompleteAddress is not true);

            foreach (var cs in containerServices)
            {
                var primaryService = _appResources.OfType<ServiceWithModelResource>().Select(sar => sar.Service)
                    .First(svc => svc.Metadata.Name.Equals(cs.PrimaryServiceName));
                cs.Service!.ApplyAddressInfoFrom(primaryService);
                cs.Service!.Status!.EffectiveAddress = ContainerHostName;
            }
        }
        finally
        {
            AspireEventSource.Instance.DcpServiceAddressAllocationStop(initialServiceCount - needAddressAllocated.Count);
        }
    }

    private Task CreateAllDcpObjectsAsync<RT>(CancellationToken cancellationToken) where RT : CustomResource, IKubernetesStaticMetadata
    {
        var objects = _appResources.Select(r => r.DcpResource).OfType<RT>();
        return CreateDcpObjectsAsync(objects, cancellationToken);
    }

    private async Task CreateDcpObjectsAsync<RT>(IEnumerable<RT> objects, CancellationToken cancellationToken) where RT : CustomResource, IKubernetesStaticMetadata
    {
        var toCreate = objects.ToImmutableArray();
        if (toCreate.Length == 0)
        {
            return;
        }

        AspireEventSource.Instance.DcpObjectSetCreationStart(RT.ObjectKind, toCreate.Length);
        try
        {
            var tasks = new List<Task>();
            foreach (var rtc in toCreate)
            {
                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        AspireEventSource.Instance.DcpObjectCreationStart(rtc.Kind, rtc.Metadata.Name);
                        await _kubernetesService.CreateAsync(rtc, cancellationToken).ConfigureAwait(false);
                    }
                    finally
                    {
                        AspireEventSource.Instance.DcpObjectCreationStop(rtc.Kind, rtc.Metadata.Name);
                    }

                }, cancellationToken));
            }
            await Task.WhenAll(tasks).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException ex)
        {
            // We catch and suppress the OperationCancelledException because the user may CTRL-C
            // during start up of the resources.
            _logger.LogDebug(ex, "Cancellation during creation of resources.");
        }
        finally
        {
            AspireEventSource.Instance.DcpObjectSetCreationStop(RT.ObjectKind, toCreate.Length);
        }
    }

    // Adds allocated endpoints for all relevant resources in the model
    void IDcpExecutor.AddAllocatedEndpointInfo(IEnumerable<RenderedModelResource> resources, AllocatedEndpointsMode mode)
        => AddAllocatedEndpointInfo(resources, mode);

    private void AddAllocatedEndpointInfo(IEnumerable<RenderedModelResource> resources, AllocatedEndpointsMode mode)
    {
        foreach (var appResource in resources)
        {
            if ((mode & AllocatedEndpointsMode.Workload) != 0)
            {
                foreach (var sp in appResource.ServicesProduced)
                {
                    var svc = (Service)sp.DcpResource;

                    if (!svc.HasCompleteAddress && sp.EndpointAnnotation.IsProxied)
                    {
                        // This should never happen; if it does, we have a bug without a workaround for the user.
                        // We should have waited for the service to have a complete address before getting here.
                        throw new InvalidDataException($"Service {svc.Metadata.Name} should have valid address at this point");
                    }

                    if (!sp.EndpointAnnotation.IsProxied && svc.AllocatedPort is null)
                    {
                        throw new InvalidOperationException($"Service '{svc.Metadata.Name}' needs to specify a port for endpoint '{sp.EndpointAnnotation.Name}' since it isn't using a proxy.");
                    }

                    var (targetHost, bindingMode) = NormalizeTargetHost(sp.EndpointAnnotation.TargetHost);

                    sp.EndpointAnnotation.AllocatedEndpoint = new AllocatedEndpoint(
                        sp.EndpointAnnotation,
                        targetHost,
                        (int)svc.AllocatedPort!,
                        bindingMode,
                        targetPortExpression: $$$"""{{- portForServing "{{{svc.Metadata.Name}}}" -}}""",
                        KnownNetworkIdentifiers.LocalhostNetwork);

                    if (appResource.DcpResource is Container ctr && ctr.Spec.Networks is not null)
                    {
                        // Once container networks are fully supported, this should allocate endpoints on those networks
                        var containerNetwork = ctr.Spec.Networks.FirstOrDefault(n => n.Name == KnownNetworkIdentifiers.DefaultAspireContainerNetwork.Value);

                        if (containerNetwork is not null)
                        {
                            var port = sp.EndpointAnnotation.TargetPort!;

                            var allocatedEndpoint = new AllocatedEndpoint(
                                sp.EndpointAnnotation,
                                $"{sp.ModelResource.Name}.dev.internal",
                                (int)port,
                                EndpointBindingMode.SingleAddress,
                                targetPortExpression: $$$"""{{- portForServing "{{{svc.Metadata.Name}}}" -}}""",
                                KnownNetworkIdentifiers.DefaultAspireContainerNetwork
                            );
                            sp.EndpointAnnotation.AllAllocatedEndpoints.AddOrUpdateAllocatedEndpoint(allocatedEndpoint.NetworkID, allocatedEndpoint);
                        }
                    }

                    // If we are not using the tunnel, we can project Executable endpoints into container networks via ContainerHostName.
                    // This really only works for Docker Desktop, but it is useful for testing too.
                    if (appResource.DcpResource is Executable && !_options.Value.EnableAspireContainerTunnel)
                    {
                        var port = sp.EndpointAnnotation.TargetPort!;
                        var allocatedEndpoint = new AllocatedEndpoint(
                            sp.EndpointAnnotation,
                            ContainerHostName,
                            (int)svc.AllocatedPort!,
                            EndpointBindingMode.SingleAddress,
                            targetPortExpression: $$$"""{{- portForServing "{{{svc.Metadata.Name}}}" -}}""",
                            KnownNetworkIdentifiers.DefaultAspireContainerNetwork
                        );
                        sp.EndpointAnnotation.AllAllocatedEndpoints.AddOrUpdateAllocatedEndpoint(KnownNetworkIdentifiers.DefaultAspireContainerNetwork, allocatedEndpoint);
                    }
                }
            }

            if ((mode & AllocatedEndpointsMode.ContainerTunnel) != 0 && _options.Value.EnableAspireContainerTunnel)
            {
                // If there are any additional services that are not directly produced by this resource,
                // but leverage its endpoints via container tunnel, we want to add allocated endpoint info for them as well.

                var tunnelServices = _appResources.Select(r => (
                    Service: r.DcpResource as Service,
                    ResourceName: r.DcpResource.Metadata.Annotations?.TryGetValue(CustomResource.ResourceNameAnnotation, out var resourceName) == true ? resourceName : null,
                    EndpointName: r.DcpResource.Metadata.Annotations?.TryGetValue(CustomResource.EndpointNameAnnotation, out var endpointName) == true ? endpointName : null,
                    TunnelInstanceName: r.DcpResource.Metadata.Annotations?.TryGetValue(CustomResource.ContainerTunnelInstanceName, out var tunnelInstanceName) == true ? tunnelInstanceName : null,
                    ContainerNetworkName: r.DcpResource.Metadata.Annotations?.TryGetValue(CustomResource.ContainerNetworkAnnotation, out var containerNetworkName) == true ? containerNetworkName : null
                ))
                .Where(ts =>
                    ts.Service is not null &&
                    string.Equals(ts.ResourceName, appResource.ModelResource.Name, StringComparisons.ResourceName) &&
                    !string.IsNullOrEmpty(ts.EndpointName) &&
                    !string.IsNullOrEmpty(ts.ContainerNetworkName)
                );

                foreach (var ts in tunnelServices)
                {
                    if (!TryGetEndpoint(appResource.ModelResource, ts.EndpointName, out var endpoint))
                    {
                        throw new InvalidDataException($"Service '{ts.Service!.Metadata.Name}' refers to endpoint '{ts.EndpointName}' that does not exist");
                    }

                    if (ts.Service?.HasCompleteAddress is not true)
                    {
                        // This should never happen; if it does, we have a bug without a workaround for the user.
                        throw new InvalidDataException($"Container tunnel service {ts.Service?.Metadata.Name} should have valid address at this point");
                    }

                    var serverSvc = _appResources.OfType<ServiceWithModelResource>().FirstOrDefault(swr =>
                        string.Equals(swr.ModelResource.Name, ts.ResourceName, StringComparisons.ResourceName) &&
                        string.Equals(swr.EndpointAnnotation.Name, endpoint.Name, StringComparisons.EndpointAnnotationName)
                    );
                    if (serverSvc is null)
                    {
                        // Should never happen -- we should have created a Service for every endpoint exposed from a resource.
                        throw new InvalidDataException($"The '{endpoint.Name}' on resource '{ts.ResourceName}' should have an associated DCP Service resource already set up");
                    }

                    var networkID = new NetworkIdentifier(ts.ContainerNetworkName!);
                    var address = string.IsNullOrEmpty(ts.TunnelInstanceName) ? ContainerHostName : KnownHostNames.DefaultContainerTunnelHostName;
                    var port = _options.Value.EnableAspireContainerTunnel ? (int)ts.Service!.AllocatedPort! : serverSvc.EndpointAnnotation.AllocatedEndpoint!.Port;

                    var tunnelAllocatedEndpoint = new AllocatedEndpoint(
                        endpoint,
                        address,
                        (int)port,
                        EndpointBindingMode.SingleAddress,
                        targetPortExpression: $$$"""{{- portForServing "{{{ts.Service.Name}}}" -}}""",
                        networkID
                    );
                    endpoint.AllAllocatedEndpoints.AddOrUpdateAllocatedEndpoint(networkID, tunnelAllocatedEndpoint);
                }
            }
        }

    }

    /// <summary>
    /// Creates DCP Service objects that represent services exposed by resources in the model via endpoints (EndpointAnnotations).
    /// </summary>
    private void PrepareServices()
    {
        _logger.LogDebug("Preparing services. Ports randomized: {RandomizePorts}", _options.Value.RandomizePorts);

        var serviceProducers = _model.Resources
            .Select(r => (ModelResource: r, Endpoints: r.Annotations.OfType<EndpointAnnotation>().ToArray()))
            .Where(sp => sp.Endpoints.Any());

        foreach (var sp in serviceProducers)
        {
            var endpoints = sp.Endpoints;

            foreach (var endpoint in endpoints)
            {
                var (serviceName, isNew) = _nameGenerator.GetServiceName(sp.ModelResource, endpoint, endpoint.DefaultNetworkID);
                if (!isNew)
                {
                    _logger.LogWarning("Encountered the same service-endpoint combination more than once for {EndpointName} on resource {ResourceName} when creating default endpoint services. This should never happen.", endpoint.Name, sp.ModelResource.Name);
                    continue;
                }

                var svc = Service.Create(serviceName);

                if (!sp.ModelResource.SupportsProxy())
                {
                    // If the resource shouldn't be proxied, we need to enforce that on the annotation
                    endpoint.IsProxied = false;
                }

                int? port;
                if (_options.Value.RandomizePorts && endpoint.IsProxied && endpoint.Port != null)
                {
                    port = null;
                    _logger.LogDebug("Randomizing port for {ServiceName}. Original port: {OriginalPort}", serviceName, endpoint.Port);
                }
                else
                {
                    port = endpoint.Port;
                }
                svc.Spec.Port = port;
                svc.Spec.Protocol = PortProtocol.FromProtocolType(endpoint.Protocol);
                if (string.Equals(KnownHostNames.Localhost, endpoint.TargetHost, StringComparison.OrdinalIgnoreCase))
                {
                    svc.Spec.Address = KnownHostNames.Localhost;
                }
                else
                {
                    svc.Spec.Address = endpoint.TargetHost;
                }

                if (!endpoint.IsProxied)
                {
                    svc.Spec.AddressAllocationMode = AddressAllocationModes.Proxyless;
                }

                // So we can associate the service with the resource that produced it and the endpoint it represents.
                svc.Annotate(CustomResource.ResourceNameAnnotation, sp.ModelResource.Name);
                svc.Annotate(CustomResource.EndpointNameAnnotation, endpoint.Name);

                var smr = new ServiceWithModelResource(sp.ModelResource, svc, endpoint);
                _appResources.Add(smr);
            }
        }

        var containers = _model.Resources.Where(r => r.IsContainer());
        if (!containers.Any())
        {
            return; // No container resources--no need bother with container-to-host connections.
        }

        if (_options.Value.EnableAspireContainerTunnel)
        {
            // Tunnel services and tunnel configuration is set up together with containers, dynamically.
            return;
        }

        // Legacy (no tunnel) mode: we are going to just proxy all host endpoint into the container network.
        var hostResources = _model.Resources.Select(HostResourceWithEndpoints.Create).OfType<HostResourceWithEndpoints>().ToList();

        foreach (var re in hostResources)
        {
            var containerNetworkServices = _containerCreator.CreateContainerNetworkServicesForHostResource(re);
            _appResources.AddRange(containerNetworkServices.Select(cns => cns.ServiceResource));
        }
    }

    internal static void SetInitialResourceState(IResource resource, IAnnotationHolder annotationHolder)
    {
        // Store the initial state of the resource
        if (resource.TryGetLastAnnotation<ResourceSnapshotAnnotation>(out var initial) &&
            initial.InitialSnapshot.State?.Text is string state && !string.IsNullOrEmpty(state))
        {
            annotationHolder.Annotate(CustomResource.ResourceStateAnnotation, state);
        }
    }

    async Task IDcpExecutor.CreateRenderedResourcesAsync(
       Func<RenderedModelResource, ILogger, CancellationToken, Task> createResourceFunc,
       IEnumerable<RenderedModelResource> executables,
       string executableKind,
       CancellationToken cancellationToken)
        => await CreateRenderedResourcesAsync(createResourceFunc, executables, executableKind, cancellationToken).ConfigureAwait(false);

    private async Task CreateRenderedResourcesAsync(
       Func<RenderedModelResource, ILogger, CancellationToken, Task> createResourceFunc,
       IEnumerable<RenderedModelResource> executables,
       string executableKind,
       CancellationToken cancellationToken)
    {
        var tasks = new List<Task>();
        var groups = executables.GroupBy(e => e.ModelResource).ToList();
        var executableCount = executables.Count();

        try
        {
            AspireEventSource.Instance.DcpObjectSetCreationStart(executableKind, executableCount);
            foreach (var group in groups)
            {
                var groupList = group.ToList();
                var groupKey = group.Key;
                // Materialize the group with ToList() to avoid issues with deferred execution of IGrouping.
                // Force this to be async so that blocking code does not stop other executables from being created.
                tasks.Add(Task.Run(() => CreateResourceExecutablesAsyncCore(groupKey, groupList, createResourceFunc, cancellationToken), cancellationToken));
            }

            await Task.WhenAll(tasks).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            AspireEventSource.Instance.DcpObjectSetCreationStop(executableKind, executableCount);
        }
    }

    async Task CreateResourceExecutablesAsyncCore(
        IResource resource,
        IEnumerable<RenderedModelResource> executables,
         Func<RenderedModelResource, ILogger, CancellationToken, Task> createResourceFunc,
        CancellationToken cancellationToken)
    {
        var resourceLogger = _loggerService.GetLogger(resource);
        var resourceType = resource is ProjectResource ? KnownResourceTypes.Project : KnownResourceTypes.Executable;

        try
        {
            AspireEventSource.Instance.CreateAspireExecutableResourcesStart(resource.Name);

            var explicitStartup = resource.TryGetAnnotationsOfType<ExplicitStartupAnnotation>(out _) is true;

            // Publish snapshots built from DCP resources. Do this now to populate more values from DCP (source) to ensure they're
            // available if the resource isn't immediately started because it's waiting or is configured for explicit start.
            foreach (var er in executables)
            {
                Func<CustomResourceSnapshot, CustomResourceSnapshot> snapshotBuild = er.DcpResource switch
                {
                    Executable exe => s => _resourceWatcher.SnapshotBuilder.ToSnapshot(exe, s),
                    ContainerExec exe => s => _resourceWatcher.SnapshotBuilder.ToSnapshot(exe, s),
                    _ => throw new NotImplementedException($"Does not support snapshots for resources of type like '{er.DcpResourceName}' is ")
                };

                await _executorEvents.PublishAsync(new OnResourceChangedContext(
                    _shutdownCancellation.Token, resourceType, resource,
                    er.DcpResourceName, new ResourceStatus(null, null, null),
                    snapshotBuild)
                ).ConfigureAwait(false);

                if (explicitStartup)
                {
                    // If explicit startup is configured, we need to set the resource state to NotStarted
                    await _executorEvents.PublishAsync(new OnResourceChangedContext(
                        cancellationToken, resourceType, resource,
                        er.DcpResource.Metadata.Name, new ResourceStatus(KnownResourceStates.NotStarted, null, null), s => s with { State = new ResourceStateSnapshot(KnownResourceStates.NotStarted, null) })
                    ).ConfigureAwait(false);
                }
            }

            if (explicitStartup)
            {
                // If explicit startup is configured, we aren't going to start the resource now.
                // We need to exit before we send the BeforeResourceStarted event or create any DCP resources.
                // The resource can be explicitly started later via user action or API at which point BeforeResourceStarted
                // will be published and the DCP resource created.
                return;
            }

            await _executorEvents.PublishAsync(new OnConnectionStringAvailableContext(cancellationToken, resource)).ConfigureAwait(false);
            await _executorEvents.PublishAsync(new OnResourceStartingContext(cancellationToken, resourceType, resource, DcpResourceName: null)).ConfigureAwait(false);
            foreach (var er in executables)
            {
                try
                {
                    AspireEventSource.Instance.DcpObjectCreationStart(er.DcpResource.Kind, er.DcpResourceName);
                    try
                    {
                        await createResourceFunc(er, resourceLogger, cancellationToken).ConfigureAwait(false);
                    }
                    finally
                    {
                        AspireEventSource.Instance.DcpObjectCreationStop(er.DcpResource.Kind, er.DcpResourceName);
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    // Expected cancellation during shutdown - propagate clean cancellation
                    throw;
                }
                catch (FailedToApplyEnvironmentException)
                {
                    // For this exception we don't want the noise of the stack trace, we've already
                    // provided more detail where we detected the issue (e.g. envvar name). To get
                    // more diagnostic information reduce logging level for DCP log category to Debug.
                    await _executorEvents.PublishAsync(new OnResourceFailedToStartContext(cancellationToken, resourceType, er.ModelResource, er.DcpResource.Metadata.Name)).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    // The purpose of this catch block is to ensure that if an individual executable resource fails
                    // to start that it doesn't tear down the entire app host AND that we route the error to the
                    // appropriate replica.
                    resourceLogger.LogError(ex, "Failed to create resource {ResourceName}", er.ModelResource.Name);
                    await _executorEvents.PublishAsync(new OnResourceFailedToStartContext(cancellationToken, resourceType, er.ModelResource, er.DcpResource.Metadata.Name)).ConfigureAwait(false);
                }
            }
        }
        catch (Exception ex)
        {
            // The purpose of this catch block is to ensure that if an error processing the overall
            // configuration of the executable resource files. This is different to the exception handling
            // block above because at this stage of processing we don't necessarily have any replicas
            // yet. For example if a dependency fails to start.
            resourceLogger.LogError(ex, "Failed to create resource {ResourceName}", resource.Name);
            await _executorEvents.PublishAsync(new OnResourceFailedToStartContext(cancellationToken, resourceType, resource, DcpResourceName: null)).ConfigureAwait(false);
        }
        finally
        {
            AspireEventSource.Instance.CreateAspireExecutableResourcesStop(resource.Name);
        }
    }

    /// <summary>
    /// Gets information about the resource's DCP instance. ReplicaInstancesAnnotation is added in BeforeStartEvent.
    /// </summary>
    internal static DcpInstance GetDcpInstance(IResource resource, int instanceIndex)
    {
        if (!resource.TryGetInstances(out var instances))
        {
            throw new DistributedApplicationException($"Couldn't find required {nameof(DcpInstancesAnnotation)} annotation on resource {resource.Name}.");
        }

        foreach (var instance in instances)
        {
            if (instance.Index == instanceIndex)
            {
                return instance;
            }
        }

        throw new DistributedApplicationException($"Couldn't find required instance ID for index {instanceIndex} on resource {resource.Name}.");
    }

    void IDcpExecutor.AddServicesProducedInfo(IResource modelResource, IAnnotationHolder dcpResource, RenderedModelResource appResource)
    {
        var modelResourceName = "(unknown)";
        try
        {
            modelResourceName = DcpNameGenerator.GetObjectNameForResource(modelResource, _options.Value);
        }
        catch { } // For error messages only, OK to fall back to (unknown)

        var servicesProduced = _appResources.OfType<ServiceWithModelResource>().Where(r => r.ModelResource == modelResource);
        foreach (var sp in servicesProduced)
        {
            var ea = sp.EndpointAnnotation;

            if (modelResource.IsContainer())
            {
                if (ea.TargetPort is null)
                {
                    throw new InvalidOperationException($"The endpoint '{ea.Name}' for container resource '{modelResourceName}' must specify the {nameof(EndpointAnnotation.TargetPort)} value");
                }
            }
            else if (!ea.IsProxied)
            {
                if (HasMultipleReplicas(appResource.DcpResource))
                {
                    throw new InvalidOperationException($"Resource '{modelResourceName}' uses multiple replicas and a proxy-less endpoint '{ea.Name}'. These features do not work together.");
                }

                if (ea.Port is int && ea.Port != ea.TargetPort)
                {
                    throw new InvalidOperationException($"The endpoint '{ea.Name}' for resource '{modelResourceName}' is not using a proxy, and it has a value of {nameof(EndpointAnnotation.Port)} property that is different from the value of {nameof(EndpointAnnotation.TargetPort)} property. For proxy-less endpoints they must match.");
                }
            }
            else
            {
                Debug.Assert(ea.IsProxied);

                if (ea.TargetPort is int && ea.Port is int && ea.TargetPort == ea.Port)
                {
                    throw new InvalidOperationException(
                        $"The endpoint '{ea.Name}' for resource '{modelResourceName}' requested a proxy ({nameof(ea.IsProxied)} is true). Non-container resources cannot be proxied when both {nameof(ea.TargetPort)} and {nameof(ea.Port)} are specified with the same value.");
                }

                if (HasMultipleReplicas(appResource.DcpResource) && ea.TargetPort is int)
                {
                    throw new InvalidOperationException(
                        $"Resource '{modelResourceName}' can have multiple replicas, and it uses endpoint '{ea.Name}' that has {nameof(ea.TargetPort)} property set. Each replica must have a unique port; setting {nameof(ea.TargetPort)} is not allowed.");
                }
            }

            var spAnn = new ServiceProducerAnnotation(sp.Service.Metadata.Name);
            (spAnn.Address, _) = NormalizeTargetHost(ea.TargetHost);
            spAnn.Port = ea.TargetPort;
            dcpResource.AnnotateAsObjectList(CustomResource.ServiceProducerAnnotation, spAnn);
            appResource.ServicesProduced.Add(sp);
        }

        static bool HasMultipleReplicas(CustomResource resource)
        {
            if (resource is Executable exe && exe.Metadata.Annotations.TryGetValue(CustomResource.ResourceReplicaCount, out var value) && int.TryParse(value, CultureInfo.InvariantCulture, out var replicas) && replicas > 1)
            {
                return true;
            }
            return false;
        }
    }

    /// <summary>
    /// Normalize the target host to a tuple of (address, binding mode) to a single valid address for
    /// service discovery purposes. A user may have configured an endpoint target host that isn't itself
    /// a valid IP address or hostname that can be resolved by other services or clients. For example,
    /// 0.0.0.0 is considered to mean that the service should bind to all IPv4 addresses. When the target
    /// host indicates that the service should bind to all IPv4 or IPv6 addresses, we instead return
    /// "localhost" as the address as that is a valid address for the .NET dev certificate. The binding mode
    /// is metdata that indicates whether an endpoint is bound to a single address or some set of multiple
    /// addresses on the system.
    /// </summary>
    /// <param name="targetHost">The target host from an EndpointAnnotation</param>
    /// <returns>A tuple of (address, binding mode).</returns>
    private static (string, EndpointBindingMode) NormalizeTargetHost(string targetHost)
    {
        return targetHost switch
        {
            null or "" => (KnownHostNames.Localhost, EndpointBindingMode.SingleAddress), // Default is localhost
            var s when EndpointHostHelpers.IsLocalhostOrLocalhostTld(s) => (KnownHostNames.Localhost, EndpointBindingMode.SingleAddress), // Explicitly set to localhost or .localhost subdomain

            var s when IPAddress.TryParse(s, out var ipAddress) => ipAddress switch // The host is an IP address
            {
                var ip when IPAddress.Any.Equals(ip) => (KnownHostNames.Localhost, EndpointBindingMode.IPv4AnyAddresses), // 0.0.0.0 (IPv4 all addresses)
                var ip when IPAddress.IPv6Any.Equals(ip) => (KnownHostNames.Localhost, EndpointBindingMode.IPv6AnyAddresses), // :: (IPv6 all addresses)
                _ => (s, EndpointBindingMode.SingleAddress), // Any other IP address is returned as-is as that will be the only address the service is bound to
            },
            _ => (KnownHostNames.Localhost, EndpointBindingMode.DualStackAnyAddresses), // Any other target host is treated as binding to all IPv4 AND IPv6 addresses
        };
    }

    /// <summary>
    /// Create a patch update using the specified resource.
    /// A copy is taken of the resource to avoid permanently changing it.
    /// </summary>
    private static V1Patch CreatePatch<T>(T obj, Action<T> change) where T : CustomResource
    {
        // This method isn't very efficient.
        // If mass or frequent patches are required then we may want to create patches manually.
        var current = JsonSerializer.SerializeToNode(obj);

        var copy = JsonSerializer.Deserialize<T>(current)!;
        change(copy);

        var changed = JsonSerializer.SerializeToNode(copy);

        var jsonPatch = current.CreatePatch(changed);
        return new V1Patch(jsonPatch, V1Patch.PatchType.JsonPatch);
    }

    public async Task StopResourceAsync(IResourceReference resourceReference, CancellationToken cancellationToken)
    {
        _logger.LogDebug("Stopping resource '{ResourceName}'...", resourceReference.DcpResourceName);
        var dcpResource = ((RenderedModelResource)resourceReference).DcpResource;
        bool stopped = false;

        AspireEventSource.Instance.StopResourceStart(dcpResource.Kind, dcpResource.Metadata.Name);
        try
        {
            stopped = await DeleteResourceRetryPipeline.ExecuteAsync(async (resourceName, attemptCancellationToken) =>
            {
                V1Patch patch;
                switch (dcpResource)
                {
                    case Container c:
                        patch = CreatePatch(c, obj => obj.Spec.Stop = true);
                        await _kubernetesService.PatchAsync(c, patch, attemptCancellationToken).ConfigureAwait(false);
                        var cu = await _kubernetesService.GetAsync<Container>(c.Metadata.Name, cancellationToken: attemptCancellationToken).ConfigureAwait(false);
                        if (cu.Status?.State == ContainerState.Exited)
                        {
                            _logger.LogDebug("Container '{ResourceName}' was stopped.", resourceReference.DcpResourceName);
                            return true;
                        }
                        else
                        {
                            _logger.LogDebug("Container '{ResourceName}' is still running; trying again to stop it...", resourceReference.DcpResourceName);
                            return false;
                        }

                    case Executable e:
                        patch = CreatePatch(e, obj => obj.Spec.Stop = true);
                        await _kubernetesService.PatchAsync(e, patch, attemptCancellationToken).ConfigureAwait(false);
                        var eu = await _kubernetesService.GetAsync<Executable>(e.Metadata.Name, cancellationToken: attemptCancellationToken).ConfigureAwait(false);
                        if (eu.Status?.State == ExecutableState.Finished || eu.Status?.State == ExecutableState.Terminated)
                        {
                            _logger.LogDebug("Executable '{ResourceName}' was stopped.", resourceReference.DcpResourceName);
                            return true;
                        }
                        else
                        {
                            _logger.LogDebug("Executable '{ResourceName}' is still running; trying again to stop it...", resourceReference.DcpResourceName);
                            return false;
                        }

                    default:
                        throw new InvalidOperationException($"Unexpected resource type: {dcpResource.Kind}");
                }
            }, resourceReference.DcpResourceName, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            AspireEventSource.Instance.StopResourceStop(dcpResource.Kind, dcpResource.Metadata.Name);
        }

        if (!stopped)
        {
            throw new InvalidOperationException($"Failed to stop resource '{resourceReference.DcpResourceName}'.");
        }
    }

    public IResourceReference GetResource(string resourceName)
    {
        var matchingResource = _appResources
            .OfType<RenderedModelResource>()
            .Where(r => r.DcpResource is not Service)
            .SingleOrDefault(r => string.Equals(r.DcpResource.Metadata.Name, resourceName, StringComparisons.ResourceName));
        if (matchingResource == null)
        {
            throw new InvalidOperationException($"Resource '{resourceName}' not found.");
        }

        return matchingResource;
    }

    public async Task StartResourceAsync(IResourceReference resourceReference, CancellationToken cancellationToken)
    {
        var appResource = (RenderedModelResource)resourceReference;
        var resourceType = GetResourceType(appResource.DcpResource, appResource.ModelResource);
        var resourceLogger = _loggerService.GetLogger(appResource.DcpResourceName);
        AspireEventSource.Instance.StartResourceStart(appResource.DcpResource.Kind, appResource.DcpResource.Metadata.Name);

        try
        {
            _logger.LogDebug("Starting {ResourceType} '{ResourceName}'.", resourceType, appResource.DcpResourceName);

            // Reset cached callback results so they are re-evaluated on restart.
            ForgetCachedCallbackResults(appResource.ModelResource);

            // Raise event after resource has been deleted. This is required because the event sets the status to "Starting" and resources being
            // deleted will temporarily override the status to a terminal state, such as "Exited".
            switch (appResource.DcpResource)
            {
                case Container c:
                    await EnsureResourceDeletedAsync<Container>(appResource.DcpResourceName).ConfigureAwait(false);

                    // Ensure we explicitly start the container
                    c.Spec.Start = true;

                    await _executorEvents.PublishAsync(new OnConnectionStringAvailableContext(cancellationToken, appResource.ModelResource)).ConfigureAwait(false);
                    await _executorEvents.PublishAsync(new OnResourceStartingContext(cancellationToken, resourceType, appResource.ModelResource, appResource.DcpResourceName)).ConfigureAwait(false);
                    await _containerCreator.CreateDcpContainerAsync(appResource, resourceLogger, cancellationToken).ConfigureAwait(false);
                    break;
                case Executable e:
                    await EnsureResourceDeletedAsync<Executable>(appResource.DcpResourceName).ConfigureAwait(false);

                    await _executorEvents.PublishAsync(new OnConnectionStringAvailableContext(cancellationToken, appResource.ModelResource)).ConfigureAwait(false);
                    await _executorEvents.PublishAsync(new OnResourceStartingContext(cancellationToken, resourceType, appResource.ModelResource, appResource.DcpResourceName)).ConfigureAwait(false);
                    await _executableCreator.CreateExecutableAsync(appResource, resourceLogger, cancellationToken).ConfigureAwait(false);
                    break;

                default:
                    throw new InvalidOperationException($"Unexpected resource type: {appResource.DcpResource.Kind}");
            }
        }
        catch (FailedToApplyEnvironmentException)
        {
            // For this exception we don't want the noise of the stack trace, we've already
            // provided more detail where we detected the issue (e.g. envvar name). To get
            // more diagnostic information reduce logging level for DCP log category to Debug.
            await _executorEvents.PublishAsync(new OnResourceFailedToStartContext(cancellationToken, resourceType, appResource.ModelResource, appResource.DcpResourceName)).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to start resource {ResourceName}", appResource.ModelResource.Name);
            await _executorEvents.PublishAsync(new OnResourceFailedToStartContext(cancellationToken, resourceType, appResource.ModelResource, appResource.DcpResourceName)).ConfigureAwait(false);
            throw;
        }
        finally
        {
            AspireEventSource.Instance.StartResourceStop(appResource.DcpResource.Kind, appResource.DcpResource.Metadata.Name);
        }

        async Task EnsureResourceDeletedAsync<T>(string resourceName) where T : CustomResource, IKubernetesStaticMetadata
        {
            _logger.LogDebug("Ensuring '{ResourceName}' is deleted.", resourceName);

            var result = await DeleteResourceRetryPipeline.ExecuteAsync(async (resourceName, attemptCancellationToken) =>
            {
                string? uid = null;

                // Make deletion part of the retry loop--we have seen cases during test execution when
                // the deletion request completed with success code, but it was never "acted upon" by DCP.

                try
                {
                    var r = await _kubernetesService.DeleteAsync<T>(resourceName, cancellationToken: attemptCancellationToken).ConfigureAwait(false);
                    uid = r.Uid();

                    _logger.LogDebug("Delete request for '{ResourceName}' successfully completed. Resource to delete has UID '{Uid}'.", resourceName, uid);
                }
                catch (HttpOperationException ex) when (ex.Response.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    _logger.LogDebug("Delete request for '{ResourceName}' returned NotFound.", resourceName);

                    // Not found means the resource is truly gone from the API server, which is our goal. Report success.
                    return true;
                }

                // Ensure resource is deleted. DeleteAsync returns before the resource is completely deleted so we must poll
                // to discover when it is safe to recreate the resource. This is required because the resources share the same name.
                // Deleting a resource might take a while (more than 10 seconds), because DCP tries to gracefully shut it down first
                // before resorting to more extreme measures.

                try
                {
                    _logger.LogDebug("Polling DCP to check if '{ResourceName}' is deleted...", resourceName);
                    var r = await _kubernetesService.GetAsync<T>(resourceName, cancellationToken: attemptCancellationToken).ConfigureAwait(false);
                    _logger.LogDebug("Get request for '{ResourceName}' returned resource with UID '{Uid}'.", resourceName, uid);

                    return false;
                }
                catch (HttpOperationException ex) when (ex.Response.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    _logger.LogDebug("Get request for '{ResourceName}' returned NotFound.", resourceName);

                    // Success.
                    return true;
                }
            }, resourceName, cancellationToken).ConfigureAwait(false);

            if (!result)
            {
                throw new DistributedApplicationException($"Failed to delete '{resourceName}' successfully before restart.");
            }
        }
    }

    private static bool TryGetEndpoint(IResource resource, string? endpointName, [NotNullWhen(true)] out EndpointAnnotation? endpoint)
    {
        endpoint = null;
        if (resource.TryGetAnnotationsOfType<EndpointAnnotation>(out var endpoints))
        {
            endpoint = endpoints.FirstOrDefault(e => string.Equals(e.Name, endpointName, StringComparisons.EndpointAnnotationName));
        }
        return endpoint is not null;
    }

    /// <summary>
    /// Clears cached callback results on resource annotations so they are re-evaluated on restart.
    /// </summary>
    private static void ForgetCachedCallbackResults(IResource resource)
    {
        if (resource.TryGetEnvironmentVariables(out var envCallbacks))
        {
            foreach (var callback in envCallbacks)
            {
                ((ICallbackResourceAnnotation<EnvironmentCallbackContext, Dictionary<string, object>>)callback).ForgetCachedResult();
            }
        }

        if (resource.TryGetAnnotationsOfType<CommandLineArgsCallbackAnnotation>(out var argsCallbacks))
        {
            foreach (var callback in argsCallbacks)
            {
                ((ICallbackResourceAnnotation<CommandLineArgsCallbackContext, IList<object>>)callback).ForgetCachedResult();
            }
        }
    }

    async Task IDcpExecutor.PublishEndpointAllocatedEventAsync(
        IEnumerable<RenderedModelResource> resource,
        CancellationToken ct)
        => await PublishEndpointAllocatedEventAsync(resource, ct).ConfigureAwait(false);

    private async Task PublishEndpointAllocatedEventAsync(
        IEnumerable<RenderedModelResource> resource,
        CancellationToken ct)
    {
        foreach (var r in resource)
        {
            lock (_endpointsAdvertised)
            {
                if (!_endpointsAdvertised.Add(r.ModelResource.Name))
                {
                    continue; // Already published for this resource
                }
            }

            var ev = new ResourceEndpointsAllocatedEvent(r.ModelResource, _executionContext.ServiceProvider);
            await _distributedApplicationEventing.PublishAsync(ev, EventDispatchBehavior.NonBlockingConcurrent, ct).ConfigureAwait(false);
        }
    }
}
