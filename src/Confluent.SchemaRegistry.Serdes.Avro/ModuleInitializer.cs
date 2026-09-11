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

using System.Runtime.CompilerServices;

namespace Confluent.SchemaRegistry.Serdes
{
    /// <summary>
    ///     Registers the <c>variant</c> Avro logical type when this assembly is loaded.
    ///
    ///     <para>The serde constructors call
    ///     <see cref="VariantLogicalType.EnsureRegistered" /> too, but that is too late for a
    ///     caller that parses its schema first - and a generated specific record parses its
    ///     schema in a static initializer, so merely constructing one is enough. Apache.Avro
    ///     resolves a logical type at parse time and caches it on the
    ///     <see cref="Avro.LogicalSchema" />, and registering afterwards does not retrofit it:
    ///     measured, a variant schema parsed before registration keeps
    ///     <c>UnknownLogicalType</c> for the life of that schema object.</para>
    ///
    ///     <para>This is the .NET counterpart of the reference's static initializer on
    ///     <c>AvroSchemaUtils</c>, which registers on class load rather than on serde
    ///     construction. Like that one, it cannot help a caller who parses a variant schema
    ///     without ever touching this assembly - nothing has loaded to do the registering.
    ///     <see cref="VariantLogicalType.EnsureRegistered" /> stays public for exactly that
    ///     case.</para>
    /// </summary>
    internal static class ModuleInitializer
    {
        [ModuleInitializer]
        internal static void Initialize()
        {
            VariantLogicalType.EnsureRegistered();
        }
    }
}

#if !NET5_0_OR_GREATER
namespace System.Runtime.CompilerServices
{
    /// <summary>
    ///     Present in the BCL from .NET 5; declared here for netstandard2.0 and net462, where
    ///     the compiler still honours a user-defined attribute of this name.
    /// </summary>
    [AttributeUsage(AttributeTargets.Method, Inherited = false)]
    internal sealed class ModuleInitializerAttribute : Attribute
    {
    }
}
#endif
