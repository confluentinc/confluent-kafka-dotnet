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

namespace Confluent.SchemaRegistry.Rules
{
    /// <summary>
    ///     Canonical CEL type-name labels for the extended types (Decimal, Timestamp).
    ///     The labels are the cross-language spec — every client (Java, Go, Python, JS,
    ///     Rust, .NET) uses the same strings; the backing CLR type is an implementation
    ///     detail of this client. Timestamp uses CEL's built-in timestamp type, so it has
    ///     no dedicated runtime value beyond cel.net's <c>TimestampT</c>.
    /// </summary>
    internal static class CelTypeLabels
    {
        /// <summary>
        ///     CEL type label for <c>confluent.type.Decimal</c>.
        /// </summary>
        public const string DecimalName = "confluent.type.Decimal";

        /// <summary>
        ///     CEL type label for <c>confluent.type.Variant</c> (Spark/Parquet Variant).
        /// </summary>
        public const string VariantName = "confluent.type.Variant";

        /// <summary>
        ///     CEL type label for <c>google.protobuf.Timestamp</c> (= CEL built-in timestamp).
        /// </summary>
        public const string TimestampName = "google.protobuf.Timestamp";
    }
}
