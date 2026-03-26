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

    public Task StopResourceAsync(IResourceReference resourceReference, CancellationToken cancellationToken) => Task.CompletedTask;

    public ConcurrentBag<AppResource> AppResources { get; } = [];

    public CancellationToken ShutdownToken => CancellationToken.None;

    public DcpSnapshotBuilder SnapshotBuilder => throw new NotImplementedException();

    public void AddAllocatedEndpointInfo(IEnumerable<RenderedModelResource> resources, AllocatedEndpointsMode mode = AllocatedEndpointsMode.Workload) { }

    public void AddServicesProducedInfo(IResource modelResource, IAnnotationHolder dcpResource, RenderedModelResource appResource) { }

    public Task PublishEndpointAllocatedEventAsync(IEnumerable<RenderedModelResource> resources, CancellationToken cancellationToken) => Task.CompletedTask;

    public Task CreateRenderedResourcesAsync(Func<RenderedModelResource, ILogger, CancellationToken, Task> createResourceFunc, IEnumerable<RenderedModelResource> resources, string resourceKind, CancellationToken cancellationToken) => Task.CompletedTask;
}
