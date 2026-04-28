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

namespace Confluent.Kafka.Internal.OAuthBearer.Aws
{
    /// <summary>
    ///     Reflection-based AWS OAUTHBEARER autowire — entry point.
    /// </summary>
    /// <remarks>
    ///     Activation marker: a config entry of the form
    ///     <c>sasl.oauthbearer.metadata.authentication.type=aws_iam</c>. The value
    ///     <c>aws_iam</c> is recognised by the .NET enum (<see cref="SaslOauthbearerMetadataAuthenticationType.AwsIam"/>)
    ///     but not by librdkafka, so it is stripped by the producer/consumer config
    ///     pipeline before native handoff. <see cref="IsMarker"/> is the predicate
    ///     used by both the strip filter and the future autowire dispatch.
    /// </remarks>
    public static class AwsAutoWire
    {
        /// <summary>Config key that activates the AWS reflection autowire path.</summary>
        public const string MarkerKey = "sasl.oauthbearer.metadata.authentication.type";

        /// <summary>On-wire marker value selecting AWS IAM authentication.</summary>
        public const string MarkerValue = "aws_iam";

        /// <summary>
        ///     Returns <c>true</c> when the supplied config entry is the AWS IAM
        ///     activation marker. Used by Producer/Consumer config processing to
        ///     strip the entry before passing to librdkafka.
        /// </summary>
        public static bool IsMarker(KeyValuePair<string, string> prop)
            => prop.Key == MarkerKey
               && string.Equals(prop.Value, MarkerValue, StringComparison.OrdinalIgnoreCase);
    }
}
