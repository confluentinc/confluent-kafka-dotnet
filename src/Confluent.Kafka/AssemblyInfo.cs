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

using System.Runtime.CompilerServices;

// Confluent.Kafka is strong-named, so each friend assembly must be declared
// with its full PublicKey. These expose internal helpers (e.g. KvStringParser
// and the AWS IAM autowire types) to the core test project and to the optional
// AWS OAUTHBEARER package and its tests.
[assembly: InternalsVisibleTo("Confluent.Kafka.UnitTests, " +
                              "PublicKey=002400000480000094000000060200000024000052534131000400000100010095ca0f" +
                              "5cfd2489bd5af4f3e35b95b7dc780e06a7b5a496ed7d64c7fd7466881fc0ad9d8d390c2d85c70f61" +
                              "ac55f1816466d5d29f986b0c3cb61d2744d772a6d70ebac8c0e84b81af2fbe7e9d5a2fa8cbbb889a" +
                              "b0ad41091fb4a4467eb03091ebb865b41b28064fd27913c9b7d678943af9950290859fc6182c20f0" +
                              "095a5587ae")]
[assembly: InternalsVisibleTo("Confluent.Kafka.OAuthBearer.Aws, " +
                              "PublicKey=002400000480000094000000060200000024000052534131000400000100010041b29d" +
                              "7acd251aad05462c2888622163e307dcc1c3c07513339921ed7c5d576dc9c993ea8c3c1730a070db" +
                              "95d10b8d4c9c62c2d25dd4e9a3e49ce47e268845472346d2c450bb8e1920fb3e9e6511f1ac8358e1" +
                              "18c60a6ec63597c96aa61cb48a0d583d252e15a25330c40d1c29000994023bb0966ddb88667e452b" +
                              "df12d1f1c1")]
[assembly: InternalsVisibleTo("Confluent.Kafka.OAuthBearer.Aws.UnitTests, " +
                              "PublicKey=0024000004800000940000000602000000240000525341310004000001000100551aa9" +
                              "127be4c0a77d033ceff8b4fa5132a0d914cb20d8f7cf3facc9f304c585c3bb68dc61cd9833669035" +
                              "747c23944cd74c63efbc76e6e59217fc2144c1dab42451278a54d29ab75d112589722c45c8e6b975" +
                              "c8d2082e30f522fe17c2633306e12bf2ba6837800817ac7f80fec1b546668a6ad839997238c3a1bf" +
                              "85eec9aae2")]
