// Copyright 2026 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace Confluent.Kafka.Internal.OAuthBearer.Aws
{
    /// <summary>
    ///     Late-binds against <c>AWSSDK.SecurityToken</c> via reflection, exposing
    ///     the type / method / property handles needed to build an STS client and
    ///     invoke <c>sts:GetWebIdentityToken</c>.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Reflection cost is paid once per shim instance (resolution at
    ///         <see cref="Bind(Assembly, Assembly)"/>). Token refresh fires every
    ///         5–60 minutes, so cached <see cref="MethodInfo"/> /
    ///         <see cref="PropertyInfo"/> dispatch is not on a hot path.
    ///     </para>
    ///     <para>
    ///         <see cref="GetWebIdentityTokenAsyncMethod"/> is resolved on the
    ///         <c>IAmazonSecurityTokenService</c> interface (not the concrete
    ///         <c>AmazonSecurityTokenServiceClient</c>) so that virtual dispatch
    ///         routes correctly through both real clients and test mocks of the
    ///         interface.
    ///     </para>
    /// </remarks>
    public sealed class StsReflectionShim
    {
        /// <summary>Simple name of the AWS SDK STS assembly (<c>AWSSDK.SecurityToken</c>).</summary>
        public const string StsAssemblySimpleName = "AWSSDK.SecurityToken";

        /// <summary>Simple name of the AWS SDK core assembly (<c>AWSSDK.Core</c>).</summary>
        public const string CoreAssemblySimpleName = "AWSSDK.Core";

        /// <summary>
        ///     The first <c>AWSSDK.SecurityToken</c> NuGet release on the v3.7
        ///     line that ships <c>sts:GetWebIdentityToken</c>.
        /// </summary>
        public static readonly Version MinAssemblyVersion = new Version(3, 7, 504);

        /// <summary>The bound <c>AWSSDK.SecurityToken</c> assembly.</summary>
        public Assembly StsAssembly { get; }
        /// <summary>The bound <c>AWSSDK.Core</c> assembly.</summary>
        public Assembly CoreAssembly { get; }
        /// <summary>Version of the bound STS assembly (used for the floor check).</summary>
        public Version StsAssemblyVersion { get; }

        /// <summary>Resolved <c>Amazon.SecurityToken.IAmazonSecurityTokenService</c>.</summary>
        public Type StsClientInterface { get; }
        /// <summary>Resolved <c>Amazon.SecurityToken.AmazonSecurityTokenServiceClient</c>.</summary>
        public Type StsClientType { get; }
        /// <summary>Resolved <c>Amazon.SecurityToken.AmazonSecurityTokenServiceConfig</c>.</summary>
        public Type StsClientConfigType { get; }
        /// <summary>Resolved <c>Amazon.SecurityToken.Model.GetWebIdentityTokenRequest</c>.</summary>
        public Type RequestType { get; }
        /// <summary>Resolved <c>Amazon.SecurityToken.Model.GetWebIdentityTokenResponse</c>.</summary>
        public Type ResponseType { get; }
        /// <summary>Resolved <c>Amazon.RegionEndpoint</c>.</summary>
        public Type RegionEndpointType { get; }
        /// <summary>Resolved <c>Amazon.Runtime.FallbackCredentialsFactory</c>.</summary>
        public Type FallbackCredentialsFactoryType { get; }

        /// <summary>
        ///     Resolved <c>IAmazonSecurityTokenService.GetWebIdentityTokenAsync(GetWebIdentityTokenRequest, CancellationToken)</c>.
        ///     Resolved on the interface so virtual dispatch routes through both
        ///     real clients and test mocks.
        /// </summary>
        public MethodInfo GetWebIdentityTokenAsyncMethod { get; }
        /// <summary>Resolved <c>RegionEndpoint.GetBySystemName(string)</c>.</summary>
        public MethodInfo RegionEndpointGetBySystemName { get; }
        /// <summary>Resolved <c>FallbackCredentialsFactory.GetCredentials()</c>.</summary>
        public MethodInfo FallbackCredentialsFactoryGetCredentials { get; }

        /// <summary>Resolved <c>GetWebIdentityTokenRequest.Audience</c> (List&lt;string&gt;).</summary>
        public PropertyInfo RequestAudience { get; }
        /// <summary>Resolved <c>GetWebIdentityTokenRequest.SigningAlgorithm</c>.</summary>
        public PropertyInfo RequestSigningAlgorithm { get; }
        /// <summary>Resolved <c>GetWebIdentityTokenRequest.DurationSeconds</c>.</summary>
        public PropertyInfo RequestDurationSeconds { get; }
        /// <summary>Resolved <c>GetWebIdentityTokenResponse.WebIdentityToken</c>.</summary>
        public PropertyInfo ResponseWebIdentityToken { get; }
        /// <summary>Resolved <c>GetWebIdentityTokenResponse.Expiration</c>.</summary>
        public PropertyInfo ResponseExpiration { get; }
        /// <summary>Resolved <c>AmazonSecurityTokenServiceConfig.RegionEndpoint</c>.</summary>
        public PropertyInfo ConfigRegionEndpoint { get; }
        /// <summary>Resolved <c>AmazonSecurityTokenServiceConfig.ServiceURL</c>.</summary>
        public PropertyInfo ConfigServiceURL { get; }

        private StsReflectionShim(
            Assembly stsAssembly,
            Assembly coreAssembly,
            Version stsAssemblyVersion,
            Type stsClientInterface,
            Type stsClientType,
            Type stsClientConfigType,
            Type requestType,
            Type responseType,
            Type regionEndpointType,
            Type fallbackCredentialsFactoryType,
            MethodInfo getWebIdentityTokenAsyncMethod,
            MethodInfo regionEndpointGetBySystemName,
            MethodInfo fallbackCredentialsFactoryGetCredentials,
            PropertyInfo requestAudience,
            PropertyInfo requestSigningAlgorithm,
            PropertyInfo requestDurationSeconds,
            PropertyInfo responseWebIdentityToken,
            PropertyInfo responseExpiration,
            PropertyInfo configRegionEndpoint,
            PropertyInfo configServiceURL)
        {
            StsAssembly = stsAssembly;
            CoreAssembly = coreAssembly;
            StsAssemblyVersion = stsAssemblyVersion;
            StsClientInterface = stsClientInterface;
            StsClientType = stsClientType;
            StsClientConfigType = stsClientConfigType;
            RequestType = requestType;
            ResponseType = responseType;
            RegionEndpointType = regionEndpointType;
            FallbackCredentialsFactoryType = fallbackCredentialsFactoryType;
            GetWebIdentityTokenAsyncMethod = getWebIdentityTokenAsyncMethod;
            RegionEndpointGetBySystemName = regionEndpointGetBySystemName;
            FallbackCredentialsFactoryGetCredentials = fallbackCredentialsFactoryGetCredentials;
            RequestAudience = requestAudience;
            RequestSigningAlgorithm = requestSigningAlgorithm;
            RequestDurationSeconds = requestDurationSeconds;
            ResponseWebIdentityToken = responseWebIdentityToken;
            ResponseExpiration = responseExpiration;
            ConfigRegionEndpoint = configRegionEndpoint;
            ConfigServiceURL = configServiceURL;
        }

        /// <summary>
        ///     Loads <c>AWSSDK.SecurityToken</c> + <c>AWSSDK.Core</c> from the
        ///     current AppDomain via <see cref="Assembly.Load(string)"/> and
        ///     binds against them.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        ///     The AWS SDK is not referenced (assembly not loadable) or the
        ///     loaded version is older than <see cref="MinAssemblyVersion"/>.
        /// </exception>
        public static StsReflectionShim LoadOrThrow()
        {
            Assembly stsAsm;
            try { stsAsm = Assembly.Load(new AssemblyName(StsAssemblySimpleName)); }
            catch (FileNotFoundException ex)
            {
                throw new InvalidOperationException(
                    $"Config '{AwsAutoWire.MarkerKey}={AwsAutoWire.MarkerValue}' requires " +
                    $"{StsAssemblySimpleName} (>= {MinAssemblyVersion}) to be referenced. " +
                    $"Add: <PackageReference Include=\"{StsAssemblySimpleName}\" Version=\"{MinAssemblyVersion}\" />",
                    ex);
            }

            Assembly coreAsm;
            try { coreAsm = Assembly.Load(new AssemblyName(CoreAssemblySimpleName)); }
            catch (FileNotFoundException ex)
            {
                throw new InvalidOperationException(
                    $"{StsAssemblySimpleName} is loaded but its companion {CoreAssemblySimpleName} " +
                    "could not be loaded. The AWS SDK installation appears incomplete.",
                    ex);
            }

            return Bind(stsAsm, coreAsm);
        }

        /// <summary>
        ///     Binds against the supplied AWS SDK assemblies. Validates the
        ///     <c>AWSSDK.SecurityToken</c> version against
        ///     <see cref="MinAssemblyVersion"/> before resolving any types.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        ///     A required type / method / property is missing, or the
        ///     <paramref name="stsAssembly"/> version is below the floor.
        /// </exception>
        public static StsReflectionShim Bind(Assembly stsAssembly, Assembly coreAssembly)
        {
            if (stsAssembly == null) throw new ArgumentNullException(nameof(stsAssembly));
            if (coreAssembly == null) throw new ArgumentNullException(nameof(coreAssembly));
            return Bind(stsAssembly, coreAssembly, GetEffectiveVersion(stsAssembly));
        }

        /// <summary>
        ///     Returns the AWS-SDK-meaningful version of <paramref name="asm"/>.
        ///     The AWS SDK freezes <c>AssemblyName.Version</c> at <c>3.3.0.0</c>
        ///     across the entire v3.7 line for binary compatibility, while the
        ///     real NuGet release version lives in
        ///     <see cref="AssemblyFileVersionAttribute"/>. We read that for the
        ///     floor check and fall back to <see cref="AssemblyName.Version"/>
        ///     if the file-version attribute is missing.
        /// </summary>
        private static Version GetEffectiveVersion(Assembly asm)
        {
            var fileVer = asm.GetCustomAttribute<AssemblyFileVersionAttribute>();
            if (fileVer != null && Version.TryParse(fileVer.Version, out var parsed))
            {
                return parsed;
            }
            return asm.GetName().Version;
        }

        /// <summary>
        ///     Test seam: same as <see cref="Bind(Assembly, Assembly)"/> but the
        ///     version is supplied explicitly (so unit tests can exercise the
        ///     version-floor check independently of the loaded assembly's actual
        ///     metadata).
        /// </summary>
        public static StsReflectionShim Bind(Assembly stsAssembly, Assembly coreAssembly, Version stsAssemblyVersion)
        {
            if (stsAssembly == null) throw new ArgumentNullException(nameof(stsAssembly));
            if (coreAssembly == null) throw new ArgumentNullException(nameof(coreAssembly));
            if (stsAssemblyVersion == null) throw new ArgumentNullException(nameof(stsAssemblyVersion));

            if (stsAssemblyVersion < MinAssemblyVersion)
            {
                throw new InvalidOperationException(
                    $"{StsAssemblySimpleName} {stsAssemblyVersion} is older than the required " +
                    $"floor {MinAssemblyVersion}. Upgrade with: " +
                    $"dotnet add package {StsAssemblySimpleName} --version {MinAssemblyVersion}");
            }

            // Types
            var stsClientInterface = GetTypeOrThrow(stsAssembly, "Amazon.SecurityToken.IAmazonSecurityTokenService");
            var stsClientType = GetTypeOrThrow(stsAssembly, "Amazon.SecurityToken.AmazonSecurityTokenServiceClient");
            var stsClientConfigType = GetTypeOrThrow(stsAssembly, "Amazon.SecurityToken.AmazonSecurityTokenServiceConfig");
            var requestType = GetTypeOrThrow(stsAssembly, "Amazon.SecurityToken.Model.GetWebIdentityTokenRequest");
            var responseType = GetTypeOrThrow(stsAssembly, "Amazon.SecurityToken.Model.GetWebIdentityTokenResponse");
            var regionEndpointType = GetTypeOrThrow(coreAssembly, "Amazon.RegionEndpoint");
            var fallbackCredsType = GetTypeOrThrow(coreAssembly, "Amazon.Runtime.FallbackCredentialsFactory");

            // Methods (resolved from interface where possible, so virtual dispatch
            // routes through real clients AND mocks of the interface).
            var cancellationTokenType = typeof(CancellationToken);
            var getTokenMethod = stsClientInterface.GetMethod(
                "GetWebIdentityTokenAsync",
                BindingFlags.Public | BindingFlags.Instance,
                binder: null,
                types: new[] { requestType, cancellationTokenType },
                modifiers: null);
            if (getTokenMethod == null)
            {
                throw new InvalidOperationException(
                    "Required method 'IAmazonSecurityTokenService.GetWebIdentityTokenAsync(GetWebIdentityTokenRequest, CancellationToken)' " +
                    $"not found in {stsAssembly.GetName().Name}. " +
                    $"Reflection shim requires a compatible {StsAssemblySimpleName} (>= {MinAssemblyVersion}).");
            }

            var regionGetBySystemName = GetStaticMethodOrThrow(
                regionEndpointType, "GetBySystemName", new[] { typeof(string) });
            var fallbackGetCredentials = GetStaticMethodOrThrow(
                fallbackCredsType, "GetCredentials", Type.EmptyTypes);

            // Properties on request / response / config
            var requestAudience = GetPropertyOrThrow(requestType, "Audience");
            var requestSigningAlgorithm = GetPropertyOrThrow(requestType, "SigningAlgorithm");
            var requestDurationSeconds = GetPropertyOrThrow(requestType, "DurationSeconds");
            var responseWebIdentityToken = GetPropertyOrThrow(responseType, "WebIdentityToken");
            var responseExpiration = GetPropertyOrThrow(responseType, "Expiration");
            var configRegionEndpoint = GetPropertyOrThrow(stsClientConfigType, "RegionEndpoint");
            var configServiceURL = GetPropertyOrThrow(stsClientConfigType, "ServiceURL");

            return new StsReflectionShim(
                stsAssembly, coreAssembly, stsAssemblyVersion,
                stsClientInterface, stsClientType, stsClientConfigType,
                requestType, responseType,
                regionEndpointType, fallbackCredsType,
                getTokenMethod, regionGetBySystemName, fallbackGetCredentials,
                requestAudience, requestSigningAlgorithm, requestDurationSeconds,
                responseWebIdentityToken, responseExpiration,
                configRegionEndpoint, configServiceURL);
        }

        /// <summary>
        ///     Constructs an <c>AmazonSecurityTokenServiceClient</c> bound to the
        ///     supplied region (and optional STS endpoint override) via the AWS
        ///     SDK's default credential chain.
        /// </summary>
        public object CreateStsClient(string region, string stsEndpointOverride)
        {
            if (string.IsNullOrEmpty(region)) throw new ArgumentException("region must not be empty.", nameof(region));

            var awsConfig = Activator.CreateInstance(StsClientConfigType);
            var regionEndpoint = RegionEndpointGetBySystemName.Invoke(null, new object[] { region });
            ConfigRegionEndpoint.SetValue(awsConfig, regionEndpoint);

            if (!string.IsNullOrEmpty(stsEndpointOverride))
            {
                ConfigServiceURL.SetValue(awsConfig, stsEndpointOverride);
            }

            return Activator.CreateInstance(StsClientType, new[] { awsConfig });
        }

        /// <summary>
        ///     Invokes <c>sts:GetWebIdentityToken</c> on the supplied STS client
        ///     (real or mocked) and returns the minted JWT plus its expiration.
        /// </summary>
        /// <remarks>
        ///     <paramref name="stsClient"/> must implement
        ///     <c>IAmazonSecurityTokenService</c>; both
        ///     <see cref="CreateStsClient"/> output and Moq proxies of that
        ///     interface satisfy this.
        /// </remarks>
        public async Task<TokenResult> GetWebIdentityTokenAsync(
            object stsClient,
            string audience,
            string signingAlgorithm,
            int durationSeconds,
            CancellationToken cancellationToken)
        {
            if (stsClient == null) throw new ArgumentNullException(nameof(stsClient));
            if (string.IsNullOrEmpty(audience)) throw new ArgumentException("audience must not be empty.", nameof(audience));
            if (string.IsNullOrEmpty(signingAlgorithm)) throw new ArgumentException("signingAlgorithm must not be empty.", nameof(signingAlgorithm));

            // Build GetWebIdentityTokenRequest via reflection.
            var request = Activator.CreateInstance(RequestType);
            RequestAudience.SetValue(request, new List<string> { audience });
            RequestSigningAlgorithm.SetValue(request, signingAlgorithm);
            RequestDurationSeconds.SetValue(request, durationSeconds);

            // Invoke. The interface returns Task<GetWebIdentityTokenResponse>.
            var taskObj = GetWebIdentityTokenAsyncMethod.Invoke(
                stsClient, new object[] { request, cancellationToken });

            if (taskObj is Task task)
            {
                await task.ConfigureAwait(false);
                var resultProperty = task.GetType().GetProperty("Result");
                var response = resultProperty.GetValue(task);

                var jwt = (string)ResponseWebIdentityToken.GetValue(response);
                var expiration = (DateTime)ResponseExpiration.GetValue(response);

                return new TokenResult(jwt, expiration);
            }

            throw new InvalidOperationException(
                $"GetWebIdentityTokenAsync did not return a Task; got {taskObj?.GetType()?.FullName ?? "<null>"}.");
        }

        // ---- Resolution helpers ----

        private static Type GetTypeOrThrow(Assembly asm, string typeName)
        {
            var t = asm.GetType(typeName);
            if (t == null)
            {
                throw new InvalidOperationException(
                    $"Required type '{typeName}' not found in {asm.GetName().Name}. " +
                    $"Reflection shim requires a compatible {StsAssemblySimpleName} (>= {MinAssemblyVersion}).");
            }
            return t;
        }

        private static MethodInfo GetStaticMethodOrThrow(Type type, string name, Type[] parameterTypes)
        {
            var m = type.GetMethod(
                name,
                BindingFlags.Public | BindingFlags.Static,
                binder: null,
                types: parameterTypes,
                modifiers: null);
            if (m == null)
            {
                throw new InvalidOperationException(
                    $"Required static method '{type.FullName}.{name}' not found. " +
                    $"Reflection shim requires a compatible {StsAssemblySimpleName} (>= {MinAssemblyVersion}).");
            }
            return m;
        }

        private static PropertyInfo GetPropertyOrThrow(Type type, string name)
        {
            var p = type.GetProperty(name, BindingFlags.Public | BindingFlags.Instance);
            if (p == null)
            {
                throw new InvalidOperationException(
                    $"Required property '{type.FullName}.{name}' not found. " +
                    $"Reflection shim requires a compatible {StsAssemblySimpleName} (>= {MinAssemblyVersion}).");
            }
            return p;
        }
    }

    /// <summary>
    ///     Result of a successful <c>sts:GetWebIdentityToken</c> call.
    /// </summary>
    public readonly struct TokenResult
    {
        /// <summary>The minted JWT (compact JWS serialisation).</summary>
        public string Jwt { get; }

        /// <summary>The token's expiration time as returned by STS (UTC).</summary>
        public DateTime Expiration { get; }

        /// <summary>Initialises a new <see cref="TokenResult"/>.</summary>
        public TokenResult(string jwt, DateTime expiration)
        {
            Jwt = jwt;
            Expiration = expiration;
        }
    }
}
