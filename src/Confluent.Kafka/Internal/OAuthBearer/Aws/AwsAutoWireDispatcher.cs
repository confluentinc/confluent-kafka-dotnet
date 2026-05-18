// Copyright 2026 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Runtime.ExceptionServices;

namespace Confluent.Kafka.Internal.OAuthBearer.Aws
{
    /// <summary>
    ///     Cross-package reflection bridge into the optional
    ///     <c>Confluent.Kafka.OAuthBearer.Aws</c> package.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         When a builder sees the AWS IAM activation marker
    ///         (<see cref="AwsIamMarker"/>), it calls <see cref="LoadHandler"/> to
    ///         resolve the optional package's
    ///         <c>AwsAutoWire.CreateHandler(IReadOnlyDictionary&lt;string,string&gt;)</c>
    ///         entry-point and obtain an OAUTHBEARER refresh handler.
    ///     </para>
    ///     <para>
    ///         The <see cref="MethodInfo"/> is cached after first resolution; all
    ///         subsequent <c>Build()</c> calls in the process re-use it without
    ///         additional <see cref="Assembly.Load(string)"/> work.
    ///     </para>
    ///     <para>
    ///         The reflection target — assembly name <c>Confluent.Kafka.OAuthBearer.Aws</c>,
    ///         type name <c>Confluent.Kafka.OAuthBearer.Aws.AwsAutoWire</c>, method
    ///         name <c>CreateHandler</c>, parameter
    ///         <c>IReadOnlyDictionary&lt;string,string&gt;</c>, return
    ///         <c>Action&lt;IClient,string&gt;</c> — is a frozen cross-package contract.
    ///         The optional package's <c>AwsAutoWireTests</c> includes a
    ///         signature-freeze test that fails if any of those names or shapes drift.
    ///     </para>
    /// </remarks>
    public static class AwsAutoWireDispatcher
    {
        private const string OptionalAssemblyName = "Confluent.Kafka.OAuthBearer.Aws";
        private const string EntryPointTypeName = "Confluent.Kafka.OAuthBearer.Aws.AwsAutoWire";
        private const string EntryPointMethodName = "CreateHandler";

        private static readonly object s_lock = new object();
        private static MethodInfo s_cachedMethod;

        /// <summary>
        ///     Resolves and invokes the optional package's
        ///     <c>AwsAutoWire.CreateHandler</c>, returning the OAUTHBEARER
        ///     refresh handler it produces.
        /// </summary>
        /// <param name="kafkaConfig">
        ///     The full client config snapshot, including
        ///     <c>sasl.oauthbearer.config</c>.
        /// </param>
        /// <returns>
        ///     An <c>Action&lt;IClient, string&gt;</c> the caller binds to its
        ///     concrete client type.
        /// </returns>
        /// <exception cref="InvalidOperationException">
        ///     The optional package is not referenced by the consuming app, or
        ///     it is referenced but does not expose the expected
        ///     <c>AwsAutoWire.CreateHandler</c> signature (version mismatch).
        /// </exception>
        /// <exception cref="ArgumentException">
        ///     <c>sasl.oauthbearer.config</c> is missing or fails the optional
        ///     package's parser. Surfaced verbatim from the parser via
        ///     <see cref="ExceptionDispatchInfo"/>.
        /// </exception>
        public static Action<IClient, string> LoadHandler(
            IReadOnlyDictionary<string, string> kafkaConfig)
        {
            if (kafkaConfig == null) throw new ArgumentNullException(nameof(kafkaConfig));

            var method = ResolveCreateHandler();
            try
            {
                return (Action<IClient, string>)method.Invoke(null, new object[] { kafkaConfig });
            }
            catch (TargetInvocationException tie) when (tie.InnerException != null)
            {
                // Re-throw the inner exception with the original stack preserved,
                // so callers see ArgumentException / InvalidOperationException as
                // thrown by the optional package — not a wrapping TIE.
                ExceptionDispatchInfo.Capture(tie.InnerException).Throw();
                throw; // unreachable
            }
        }

        private static MethodInfo ResolveCreateHandler()
        {
            var cached = s_cachedMethod;
            if (cached != null) return cached;

            lock (s_lock)
            {
                if (s_cachedMethod != null) return s_cachedMethod;

                Assembly asm;
                try
                {
                    asm = Assembly.Load(OptionalAssemblyName);
                }
                catch (FileNotFoundException ex)
                {
                    throw new InvalidOperationException(
                        $"Config '{AwsIamMarker.Key}={AwsIamMarker.Value}' requires the " +
                        $"{OptionalAssemblyName} package. Add: " +
                        $"<PackageReference Include=\"{OptionalAssemblyName}\" Version=\"...\" /> " +
                        "to the project that consumes Confluent.Kafka.", ex);
                }

                var type = asm.GetType(EntryPointTypeName, throwOnError: false)
                    ?? throw new InvalidOperationException(
                        $"{OptionalAssemblyName} is loaded but does not expose " +
                        $"'{EntryPointTypeName}'. The package may be incompatible " +
                        "with this version of Confluent.Kafka — upgrade both " +
                        "packages to matching versions.");

                var method = type.GetMethod(
                    EntryPointMethodName,
                    BindingFlags.Public | BindingFlags.Static,
                    binder: null,
                    types: new[] { typeof(IReadOnlyDictionary<string, string>) },
                    modifiers: null)
                    ?? throw new InvalidOperationException(
                        $"{OptionalAssemblyName} is loaded but does not expose " +
                        $"'{EntryPointTypeName}.{EntryPointMethodName}" +
                        "(IReadOnlyDictionary<string,string>)'. " +
                        "The package may be incompatible with this version of " +
                        "Confluent.Kafka — upgrade both packages to matching versions.");

                s_cachedMethod = method;
                return method;
            }
        }

        /// <summary>
        ///     Test seam: drops the cached <see cref="MethodInfo"/>. Subsequent
        ///     <see cref="LoadHandler"/> calls re-resolve. Production code should
        ///     never call this — the cache is process-scoped by design.
        /// </summary>
        public static void ResetCacheForTests()
        {
            lock (s_lock) { s_cachedMethod = null; }
        }
    }
}
