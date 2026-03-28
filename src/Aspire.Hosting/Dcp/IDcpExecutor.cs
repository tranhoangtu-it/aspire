// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Aspire.Hosting.Dcp.Model;

namespace Aspire.Hosting.Dcp;

/// <summary>
/// Specifies which endpoints to process when creating AllocatedEndpoint info.
/// </summary>
[Flags]
internal enum AllocatedEndpointsMode
{
    Workload = 0x1,
    ContainerTunnel = 0x2,
    All = 0xFF
}

internal interface IDcpExecutor
{
    Task RunApplicationAsync(CancellationToken cancellationToken);

    Task StopAsync(CancellationToken cancellationToken);

    IResourceReference GetResource(string resourceName);

    Task StartResourceAsync(IResourceReference resourceReference, CancellationToken cancellationToken);

    Task StopResourceAsync(IResourceReference resourceReference, CancellationToken cancellationToken);

    ConcurrentBag<IAppResource> AppResources { get; }

    CancellationToken ShutdownToken { get; }

    ResourceSnapshotBuilder SnapshotBuilder { get; }

    // Adds AllocatedEndpoint objects to Aspire EndpointAnnotations specified resources.
    // Called after DCP allocated endpoint addresses for Services implemented by resources.
    void AddAllocatedEndpointInfo<TDcpResource>(IEnumerable<RenderedModelResource<TDcpResource>> resources, AllocatedEndpointsMode mode = AllocatedEndpointsMode.Workload)
        where TDcpResource : CustomResource, IKubernetesStaticMetadata;

    // Examines the Aspire resource annotations and adds equivalent ServiceProducerAnnotations to corresponding DCP resources.
    void AddServicesProducedInfo<TDcpResource>(RenderedModelResource<TDcpResource> appResource)
        where TDcpResource : CustomResource, IKubernetesStaticMetadata;

    // Publishes ResourceEndpointsAllocatedEvent, ensuring each resource gets the event exactly once. 
    Task PublishEndpointAllocatedEventAsync<TDcpResource>(IEnumerable<RenderedModelResource<TDcpResource>> resources, CancellationToken cancellationToken)
        where TDcpResource : CustomResource, IKubernetesStaticMetadata;

    // Orchestrates DCP resource creation, raising Aspire model events as necessary.
    // The createResourceFunc is expected to take in the Aspire resource information and create the corresponding DCP resource.
    // This method is meant to be called by DCP object creator components that need to create "auxiliary" objects, such as creating
    // ContainerExec objects during Container object creation.
    // It should be called to create resources of a single kind at a time (do not mix different resource kinds in a single call).
    Task CreateRenderedResourcesAsync<TDcpResource>(
        Func<RenderedModelResource<TDcpResource>, ILogger, CancellationToken, Task> createResourceFunc,
        IEnumerable<RenderedModelResource<TDcpResource>> resources,
        CancellationToken cancellationToken)
        where TDcpResource : CustomResource, IKubernetesStaticMetadata;

    // Creates DCP custom resource objects via the Kubernetes API.
    // Handles AspireEventSource tracing and error handling internally.
    Task CreateDcpObjectsAsync<TDcpResource>(IEnumerable<TDcpResource> objects, CancellationToken cancellationToken)
        where TDcpResource : CustomResource, IKubernetesStaticMetadata;

    // Waits until the provided set of Services have their addresses allocated by the orchestrator
    // and updates them with the allocated address information.
    Task UpdateWithEffectiveAddressInfo(IEnumerable<Service> services, CancellationToken cancellationToken, TimeSpan? timeout = null);
}
