// Copyright 2024 Confluent Inc.
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

using System.Runtime.CompilerServices;

// Public key of test/Confluent.SchemaRegistry.UnitTests/Confluent.SchemaRegistry.UnitTests.snk
// Strong-named IVT requires the consumer's public key because both assemblies are signed.
[assembly: InternalsVisibleTo("Confluent.SchemaRegistry.UnitTests, " +
                              "PublicKey=0024000004800000940000000602000000240000525341310004000001000100" +
                              "a9812f706439ebaee9a341c0b28d8d367873f44fa4d68a4aa89b616fa9d101e3f637c5cebbf09" +
                              "f9631c9b587a22ebe6078c384bea2caaef3bb1ac9e27de2c55f1e0e8f17d88fa2d5713fdebd4b" +
                              "799f2cfb958a09c9ea24a97c38499ffe81d69df1d477dbf50f03308ff495f71236907cbf32dba" +
                              "63b5a84aafacdff76f12ef6ca")]
