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

namespace Confluent.Kafka.Internal.OAuthBearer.Aws
{
    /// <summary>
    ///     The config-key/value pair that activates the AWS OAUTHBEARER autowire path.
    /// </summary>
    public static class AwsIamMarker
    {
        /// <summary>Config key that activates the AWS autowire path.</summary>
        public const string Key = "sasl.oauthbearer.metadata.authentication.type";

        /// <summary>On-wire marker value selecting AWS IAM authentication.</summary>
        public const string Value = "aws_iam";

        /// <summary>
        ///     Returns <c>true</c> when the supplied config entry is the AWS IAM
        ///     activation marker. Used by Producer/Consumer/AdminClient config
        ///     processing to strip the entry before passing to librdkafka.
        /// </summary>
        /// <remarks>
        ///     TODO(post-dev_oauthbearer_awsiam): Once <c>librdkafka.redist</c> is bumped
        ///     to a version that recognises the <c>aws_iam</c> enum value (see
        ///     librdkafka PR adding the no-op handler), this method and its callers
        ///     in <c>Producer.cs</c> / <c>Consumer.cs</c> become unnecessary and can
        ///     be deleted. The <see cref="Key"/> and <see cref="Value"/> constants
        ///     stay — they're still used by the dispatcher and friendly-error paths.
        /// </remarks>
        public static bool IsMarker(KeyValuePair<string, string> prop)
            => prop.Key == Key
               && string.Equals(prop.Value, Value, StringComparison.OrdinalIgnoreCase);
    }
}
