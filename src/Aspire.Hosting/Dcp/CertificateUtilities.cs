// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

#pragma warning disable ASPIRECERTIFICATES001

using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Aspire.Hosting.ApplicationModel;
using Aspire.Hosting.Dcp.Model;
using Aspire.Hosting.Utils;

namespace Aspire.Hosting.Dcp;

/// <summary>
/// Shared certificate utilities used by both ExecutableCreator and ContainerCreator.
/// </summary>
internal sealed class CertificateUtilities : IDisposable
{
    // Well-known location on disk where dev-cert key material is cached.
    private static readonly string s_macOSUserDevCertificateLocation = Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), ".aspire", "dev-certs", "https");

    private readonly SemaphoreSlim _cacheSemaphore = new(1, 1);

    /// <summary>
    /// Returns the certificate PEM format key and/or PFX bytes based on the provided configuration.
    /// Only the formats referenced in resource configuration will be returned.
    /// </summary>
    internal async Task<(char[]? keyPem, byte[]? pfxBytes)> GetCertificateKeyMaterialAsync(
        HttpsCertificateExecutionConfigurationData configuration, CancellationToken cancellationToken)
    {
        var certificate = configuration.Certificate;
        var lookup = certificate.Thumbprint;
        if (configuration.Password is not null)
        {
            lookup += $"-{configuration.Password}";
        }

        lookup = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(lookup)));

        char[]? pemKey = null;
        byte[]? pfxBytes = null;
        // Ensure only one thread at a time is resolving certificates to avoid concurrent cache misses all trying to update
        // the cache at the same time.
        await _cacheSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (configuration.IsKeyPathReferenced)
            {
                var keyFileName = Path.Join(s_macOSUserDevCertificateLocation, $"{lookup}.key");
                if (OperatingSystem.IsMacOS() && certificate.IsAspNetCoreDevelopmentCertificate())
                {
                    // On MacOS, we cache development certificate key material to avoid triggering repeated keychain prompts
                    // when referencing the development certificate key. We don't do this for other OSes or other certificates.
                    try
                    {
                        // Attempt to read the cached development certificate key
                        if (File.Exists(keyFileName))
                        {
                            var keyCandidate = File.ReadAllText(keyFileName);

                            if (!string.IsNullOrEmpty(keyCandidate))
                            {
                                pemKey = keyCandidate.ToCharArray();
                            }
                        }
                    }
                    catch
                    {
                        // Ignore errors and retrieve the key from the certificate
                    }
                }

                if (pemKey is null)
                {
                    // See: https://github.com/dotnet/aspnetcore/blob/main/src/Shared/CertificateGeneration/CertificateManager.cs
                    using var privateKey = certificate.GetRSAPrivateKey();
                    if (privateKey is null)
                    {
                        throw new InvalidOperationException("The certificate does not have an associated RSA private key.");
                    }

                    var keyBytes = privateKey.ExportEncryptedPkcs8PrivateKey(
                        configuration.Password ?? string.Empty,
                        new PbeParameters(
                            PbeEncryptionAlgorithm.Aes256Cbc,
                            HashAlgorithmName.SHA256,
                            iterationCount: configuration.Password is null ? 1 : 100_000));
                    pemKey = PemEncoding.Write("ENCRYPTED PRIVATE KEY", keyBytes);

                    if (configuration.Password is null)
                    {
                        using var tempKey = RSA.Create();
                        tempKey.ImportFromEncryptedPem(pemKey, string.Empty);
                        Array.Clear(keyBytes, 0, keyBytes.Length);
                        Array.Clear(pemKey, 0, pemKey.Length);
                        keyBytes = tempKey.ExportPkcs8PrivateKey();
                        pemKey = PemEncoding.Write("PRIVATE KEY", keyBytes);
                    }

                    Array.Clear(keyBytes, 0, keyBytes.Length);

                    if (pemKey is not null && OperatingSystem.IsMacOS() && certificate.IsAspNetCoreDevelopmentCertificate())
                    {
                        // On Mac, cache the development certificate key material if we had to load it from the keychain
                        try
                        {
                            // Create the directory for storing macOS user dev certificates if it doesn't exist
                            Directory.CreateDirectory(s_macOSUserDevCertificateLocation, UnixFileMode.UserExecute | UnixFileMode.UserWrite | UnixFileMode.UserRead);

                            await File.WriteAllTextAsync(keyFileName, new string(pemKey), cancellationToken).ConfigureAwait(false);
                        }
                        catch
                        {
                            // This is a best effort caching operation
                        }
                    }
                }
            }

            if (configuration.IsPfxPathReferenced)
            {
                var pfxFileName = Path.Join(s_macOSUserDevCertificateLocation, $"{lookup}.pfx");
                if (OperatingSystem.IsMacOS() && certificate.IsAspNetCoreDevelopmentCertificate())
                {
                    // On MacOS, we cache development certificate key material to avoid triggering repeated keychain prompts
                    // when referencing the development certificate key. We don't do this for other OSes or other certificates.
                    try
                    {
                        // Attempt to read the cached development certificate key
                        if (File.Exists(pfxFileName))
                        {
                            var pfxCandidate = File.ReadAllBytes(pfxFileName);
                            if (pfxCandidate.Length > 0)
                            {
                                using var tempCert = new X509Certificate2(pfxCandidate, configuration.Password);
                                if (tempCert.Thumbprint.Equals(certificate.Thumbprint, StringComparison.Ordinal))
                                {
                                    pfxBytes = pfxCandidate;
                                }
                            }
                        }
                    }
                    catch
                    {
                        // Ignore errors and retrieve the key from the certificate
                    }
                }

                if (pfxBytes is null)
                {
                    // On Mac, cache the development certificate pfx if we had to export it from the keychain
                    pfxBytes = certificate.Export(X509ContentType.Pfx, configuration.Password);

                    if (pfxBytes is not null && pfxBytes.Length > 0 && OperatingSystem.IsMacOS() && certificate.IsAspNetCoreDevelopmentCertificate())
                    {
                        try
                        {
                            // Create the directory for storing macOS user dev certificates if it doesn't exist
                            Directory.CreateDirectory(s_macOSUserDevCertificateLocation, UnixFileMode.UserExecute | UnixFileMode.UserWrite | UnixFileMode.UserRead);

                            File.WriteAllBytes(pfxFileName, pfxBytes);
                        }
                        catch
                        {
                            // This is a best effort caching operation
                        }
                    }
                }
            }
        }
        finally
        {
            _cacheSemaphore.Release();
        }

        return (pemKey, pfxBytes);
    }

    internal static List<PemCertificate> BuildPemCertificateList(X509Certificate2Collection certificates)
    {
        return certificates.Select(c => new PemCertificate
        {
            Thumbprint = c.Thumbprint,
            Contents = c.ExportCertificatePem(),
        }).DistinctBy(cert => cert.Thumbprint).ToList();
    }

    public void Dispose()
    {
        _cacheSemaphore.Dispose();
    }
}
