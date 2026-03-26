// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System.CommandLine;
using System.Globalization;
using Aspire.Cli.Configuration;
using Aspire.Cli.Interaction;
using Aspire.Cli.Layout;
using Aspire.Cli.Processes;
using Aspire.Cli.Resources;
using Aspire.Cli.Telemetry;
using Aspire.Cli.Utils;
using Microsoft.Extensions.Logging;

namespace Aspire.Cli.Commands;

/// <summary>
/// Command that starts a standalone Aspire Dashboard instance.
/// </summary>
internal sealed class DashboardCommand : BaseCommand
{
    internal override HelpGroup HelpGroup => HelpGroup.Monitoring;

    private readonly IInteractionService _interactionService;
    private readonly ILayoutDiscovery _layoutDiscovery;
    private readonly ILogger<DashboardCommand> _logger;

    private static readonly Option<bool> s_detachOption = new("--detach")
    {
        Description = DashboardCommandStrings.DetachOptionDescription
    };

    public DashboardCommand(
        IInteractionService interactionService,
        ILayoutDiscovery layoutDiscovery,
        IFeatures features,
        ICliUpdateNotifier updateNotifier,
        CliExecutionContext executionContext,
        ILogger<DashboardCommand> logger,
        AspireCliTelemetry telemetry)
        : base("dashboard", DashboardCommandStrings.Description, features, updateNotifier, executionContext, interactionService, telemetry)
    {
        _interactionService = interactionService;
        _layoutDiscovery = layoutDiscovery;
        _logger = logger;

        Options.Add(s_detachOption);
        TreatUnmatchedTokensAsErrors = false;
    }

    protected override async Task<int> ExecuteAsync(ParseResult parseResult, CancellationToken cancellationToken)
    {
        var layout = _layoutDiscovery.DiscoverLayout();
        if (layout is null)
        {
            _interactionService.DisplayError(DashboardCommandStrings.BundleNotAvailable);
            return ExitCodeConstants.DashboardFailure;
        }

        var managedPath = layout.GetManagedPath();
        if (managedPath is null || !File.Exists(managedPath))
        {
            _interactionService.DisplayError(DashboardCommandStrings.BundleNotAvailable);
            return ExitCodeConstants.DashboardFailure;
        }

        var dashboardArgs = new List<string> { "dashboard" };
        dashboardArgs.AddRange(parseResult.UnmatchedTokens);

        var detach = parseResult.GetValue(s_detachOption);

        if (detach)
        {
            return ExecuteDetached(managedPath, dashboardArgs);
        }

        return await ExecuteForegroundAsync(managedPath, dashboardArgs, cancellationToken).ConfigureAwait(false);
    }

    private int ExecuteDetached(string managedPath, List<string> dashboardArgs)
    {
        _logger.LogDebug("Starting dashboard in detached mode: {ManagedPath}", managedPath);

        var process = DetachedProcessLauncher.Start(managedPath, dashboardArgs, Directory.GetCurrentDirectory());

        _interactionService.DisplayMessage(KnownEmojis.Rocket,
            string.Format(CultureInfo.CurrentCulture, DashboardCommandStrings.DashboardStarted, process.Id));

        return ExitCodeConstants.Success;
    }

    private async Task<int> ExecuteForegroundAsync(string managedPath, List<string> dashboardArgs, CancellationToken cancellationToken)
    {
        _logger.LogDebug("Starting dashboard in foreground: {ManagedPath}", managedPath);

        using var process = LayoutProcessRunner.Start(managedPath, dashboardArgs, redirectOutput: false);

        try
        {
            await process.WaitForExitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
            }

            return ExitCodeConstants.Success;
        }

        if (process.ExitCode != 0)
        {
            _interactionService.DisplayError(
                string.Format(CultureInfo.CurrentCulture, DashboardCommandStrings.DashboardExitedWithError, process.ExitCode));
        }

        return process.ExitCode == 0 ? ExitCodeConstants.Success : ExitCodeConstants.DashboardFailure;
    }
}
