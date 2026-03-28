// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using Aspire.Hosting.ApplicationModel;
using Aspire.Hosting.Dcp.Model;
using System.Diagnostics;

namespace Aspire.Hosting.Dcp;

internal interface IAppResource
{
    CustomResource DcpResource { get; }
    string DcpResourceName { get; }
    string DcpResourceKind { get; }
    List<AppResource<Service>> ServicesProduced { get; }
}

[DebuggerDisplay("DcpResourceName = {DcpResourceName}, DcpResourceKind = {DcpResourceKind}")]
internal class AppResource<TDcpResource>: IAppResource, IEquatable<AppResource<TDcpResource>> where TDcpResource : CustomResource, IKubernetesStaticMetadata
{
    public TDcpResource DcpResource { get; }
    public string DcpResourceName => DcpResource.Metadata.Name;
    public string DcpResourceKind => TDcpResource.ObjectKind;

    CustomResource IAppResource.DcpResource => DcpResource;
    
    public AppResource(TDcpResource dcpResource)
    {
        DcpResource = dcpResource;
    }

    public virtual List<AppResource<Service>> ServicesProduced { get; } = [];

    public bool Equals(AppResource<TDcpResource>? other)
    {
        if (other is null)
        {
            return false;
        }
        var dr = DcpResource;
        var odr = other.DcpResource;
        return dr.GetType().Equals(odr.GetType()) &&
            dr.Metadata.Name == odr.Metadata.Name &&
            dr.Metadata.NamespaceProperty == odr.Metadata.NamespaceProperty;
    }
}   

[DebuggerDisplay("ModelResource = {ModelResource}, DcpResourceName = {DcpResourceName}, DcpResourceKind = {DcpResourceKind}")]
internal class RenderedModelResource<TDcpResource> : AppResource<TDcpResource>, IResourceReference where TDcpResource : CustomResource, IKubernetesStaticMetadata
{
    public IResource ModelResource { get; }
    
    public RenderedModelResource(IResource modelResource, TDcpResource dcpResource): base(dcpResource)
    {
        ModelResource = modelResource;
    }

    public new virtual List<ServiceWithModelResource> ServicesProduced { get; } = [];
    public virtual List<ServiceWithModelResource> ServicesConsumed { get; } = [];
}

internal sealed class ServiceWithModelResource : RenderedModelResource<Service>
{
    public Service Service => DcpResource;
    public EndpointAnnotation EndpointAnnotation { get; }

    public override List<ServiceWithModelResource> ServicesProduced
    {
        get { throw new InvalidOperationException("Service resources do not produce any services"); }
    }
    public override List<ServiceWithModelResource> ServicesConsumed
    {
        get { throw new InvalidOperationException("Service resources do not consume any services"); }
    }

    public ServiceWithModelResource(IResource modelResource, Service service, EndpointAnnotation sba) : base(modelResource, service)
    {
        EndpointAnnotation = sba;
    }
}

internal interface IResourceReference
{
    IResource ModelResource { get; }
    string DcpResourceName { get; }
}
