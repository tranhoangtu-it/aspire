// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using Aspire.Cli.Commands;
using Aspire.Cli.Resources;
using Aspire.Cli.Tests.TestServices;
using Aspire.Cli.Tests.Utils;
using Microsoft.AspNetCore.InternalTesting;
using Microsoft.Extensions.DependencyInjection;

namespace Aspire.Cli.Tests.Commands;

public class DashboardCommandTests(ITestOutputHelper outputHelper)
{
    [Fact]
    public async Task DashboardCommand_BundleNotAvailable_DisplaysError()
    {
        using var workspace = TemporaryWorkspace.Create(outputHelper);

        var testInteractionService = new TestInteractionService();

        var services = CliTestHelper.CreateServiceCollection(workspace, outputHelper, options =>
        {
            options.InteractionServiceFactory = _ => testInteractionService;
        });

        var provider = services.BuildServiceProvider();
        var command = provider.GetRequiredService<RootCommand>();
        var result = command.Parse("dashboard");

        var exitCode = await result.InvokeAsync().DefaultTimeout();

        Assert.Equal(ExitCodeConstants.DashboardFailure, exitCode);
        var errorMessage = Assert.Single(testInteractionService.DisplayedErrors);
        Assert.Equal(DashboardCommandStrings.BundleNotAvailable, errorMessage);
    }

    [Fact]
    public async Task DashboardCommand_Help_ReturnsSuccess()
    {
        using var workspace = TemporaryWorkspace.Create(outputHelper);
        var services = CliTestHelper.CreateServiceCollection(workspace, outputHelper);
        var provider = services.BuildServiceProvider();

        var command = provider.GetRequiredService<RootCommand>();
        var result = command.Parse("dashboard --help");

        var exitCode = await result.InvokeAsync().DefaultTimeout();

        Assert.Equal(ExitCodeConstants.Success, exitCode);
    }
}
