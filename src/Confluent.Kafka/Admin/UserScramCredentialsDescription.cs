// Copyright 2023 Confluent Inc.
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

using System.Collections.Generic;
using System.Linq;
using System.Text;


namespace Confluent.Kafka.Admin
{
    /// <summary>
    ///     UserScramCredentials Description: Describe SCRAM credentials
    ///     of a single user.
    /// </summary>
    public class UserScramCredentialsDescription
    {

        /// <summary>
        ///     Username of the Description
        /// </summary>
        public string User { get; set; }

        /// <summary>
        ///     ScramCredentialInfos of the User
        /// </summary>
        public List<ScramCredentialInfo> ScramCredentialInfos { get; set; }

        /// <summary>
        ///     User Level Error
        /// </summary>
        public Error Error {get;set;}

        /// <summary>
        ///     Returns a JSON representation of the UserScramCredentialsDescription object.
        /// </summary>
        /// <returns>
        ///     A JSON representation the UserScramCredentialsDescription object.
        /// </returns>
        public override string ToString()
        {
            var result = new StringBuilder();
            result.Append(
                "{\"User\": ");
            result.Append(User.Quote());
            result.Append(", \"ScramCredentialInfos\": [");
            result.Append(string.Join(", ",ScramCredentialInfos.Select(u => u.ToString())));
            result.Append("]");
            result.Append($", \"Error\": {Error.ToString().Quote()}}}");
            return  result.ToString();
        }

    }
}
