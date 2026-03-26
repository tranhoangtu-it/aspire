// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System.Collections.Concurrent;
using Aspire.Hosting.ApplicationModel;
using Aspire.Hosting.Dcp.Model;
using Microsoft.Extensions.Logging;

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
    // Existing members.
    Task RunApplicationAsync(CancellationToken cancellationToken);
    Task StopAsync(CancellationToken cancellationToken);
    IResourceReference GetResource(string resourceName);
    Task StartResourceAsync(IResourceReference resourceReference, CancellationToken cancellationToken);
    Task StopResourceAsync(IResourceReference resourceReference, CancellationToken cancellationToken);

    // Shared resource registry. Both ExecutableCreator and ContainerCreator
    // add entries during preparation and query during creation.
    ConcurrentBag<AppResource> AppResources { get; }

    // Cancellation token sourced from the DcpExecutor shutdown signal.
    // Used by ContainerCreator for OnResourceChangedContext events.
    CancellationToken ShutdownToken { get; }

    // Snapshot builder for creating resource snapshot update functions.
    // Used by ContainerCreator in CreateSingleContainerAsync.
    ResourceSnapshotBuilder SnapshotBuilder { get; }

    // Allocates endpoint information for resources. Uses AppResources, DcpOptions,
    // and ContainerHostName internally. Called by DcpExecutor (for executables and
    // tunnel endpoints) and by ContainerCreator (for container endpoints).
    void AddAllocatedEndpointInfo(
        IEnumerable<RenderedModelResource> resources,
        AllocatedEndpointsMode mode = AllocatedEndpointsMode.Workload);

    // Registers endpoints as services for a resource. Queries AppResources for
    // ServiceWithModelResource entries and validates endpoint configuration.
    // Called by both ExecutableCreator and ContainerCreator during preparation.
    void AddServicesProducedInfo(
        IResource modelResource,
        IAnnotationHolder dcpResource,
        RenderedModelResource appResource);

    // Publishes ResourceEndpointsAllocatedEvent, ensuring each resource gets
    // the event exactly once. Called by ContainerCreator from CreateDcpContainerAsync.
    Task PublishEndpointAllocatedEventAsync(
        IEnumerable<RenderedModelResource> resources,
        CancellationToken cancellationToken);

    // Orchestrates per-resource creation with lifecycle ceremony: AspireEventSource
    // tracing, DcpExecutorEvents (snapshots, connection strings, starting/failed events),
    // explicit startup handling, and error isolation per replica.
    // Creators pass a per-resource creation function as a delegate.
    Task CreateRenderedResourcesAsync(
        Func<RenderedModelResource, ILogger, CancellationToken, Task> createResourceFunc,
        IEnumerable<RenderedModelResource> resources,
        string resourceKind,
        CancellationToken cancellationToken);
}
