// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System.Collections.Concurrent;
using Aspire.Hosting.Dcp;
using Aspire.Hosting.Dcp.Model;
using Microsoft.Extensions.Logging;
using DcpSnapshotBuilder = Aspire.Hosting.Dcp.ResourceSnapshotBuilder;

namespace Aspire.Hosting.Tests.Utils;

internal sealed class TestDcpExecutor : IDcpExecutor
{
    public IResourceReference GetResource(string resourceName) => throw new NotImplementedException();

    public Task RunApplicationAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StartResourceAsync(IResourceReference resourceReference, CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StopResourceAsync(IResourceReference resource, CancellationToken cancellationToken) => Task.CompletedTask;

    public ConcurrentBag<IAppResource> AppResources { get; } = [];

    public CancellationToken ShutdownToken => CancellationToken.None;

    public DcpSnapshotBuilder SnapshotBuilder => throw new NotImplementedException();

    public void AddAllocatedEndpointInfo<TDcpResource>(IEnumerable<RenderedModelResource<TDcpResource>> resources, AllocatedEndpointsMode mode = AllocatedEndpointsMode.Workload) where TDcpResource : CustomResource, IKubernetesStaticMetadata { }

    public void AddServicesProducedInfo<TDcpResource>(RenderedModelResource<TDcpResource> appResource) where TDcpResource : CustomResource, IKubernetesStaticMetadata { }

    public Task PublishEndpointAllocatedEventAsync<TDcpResource>(IEnumerable<RenderedModelResource<TDcpResource>> resources, CancellationToken cancellationToken) where TDcpResource : CustomResource, IKubernetesStaticMetadata => Task.CompletedTask;

    public Task CreateRenderedResourcesAsync<TDcpResource>(Func<RenderedModelResource<TDcpResource>, ILogger, CancellationToken, Task> createResourceFunc, IEnumerable<RenderedModelResource<TDcpResource>> resources, CancellationToken cancellationToken) where TDcpResource : CustomResource, IKubernetesStaticMetadata => Task.CompletedTask;

    public Task CreateDcpObjectsAsync<TDcpResource>(IEnumerable<TDcpResource> objects, CancellationToken cancellationToken) where TDcpResource : CustomResource, IKubernetesStaticMetadata => Task.CompletedTask;

    public Task UpdateWithEffectiveAddressInfo(IEnumerable<Service> services, CancellationToken cancellationToken, TimeSpan? timeout = null) => Task.CompletedTask;
}
