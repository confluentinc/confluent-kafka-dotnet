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

using System.Text;


namespace Confluent.Kafka.Admin
{
    /// <summary>
    ///     Deletion for the UserScramCredentialAlteration
    ///     Used to delete existing Scram Credential for a user.
    /// </summary>
    public class UserScramCredentialDeletion : UserScramCredentialAlteration
    {

        /// <summary>
        ///     The Mechanism of the Deletion
        /// </summary>
        public ScramMechanism Mechanism { get; set; }

        /// <summary>
        ///     Returns a JSON representation of the UserScramCredentialDeletion object.
        /// </summary>
        /// <returns>
        ///     A JSON representation the UserScramCredentialDeletion object.
        /// </returns>
        public override string ToString()
        {
            var result = new StringBuilder();
            result.Append(
                "{\"User\": ");
            result.Append(User.ToString().Quote());
            result.Append(
                ", \"Mechanism\": ");
            result.Append(Mechanism.ToString().Quote());
            result.Append("}");
            return  result.ToString();
        }
    }
}
