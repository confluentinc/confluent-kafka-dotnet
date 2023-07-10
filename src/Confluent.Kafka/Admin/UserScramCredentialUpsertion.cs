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
    ///     Upsertion for the UserScramCredentialAlteration
    ///     Used to upsert new Scram Credential for a user.
    /// </summary>
    public class UserScramCredentialUpsertion : UserScramCredentialAlteration
    {
        /// <summary>
        ///     The mechanism and iterations.
        /// </summary>
        public ScramCredentialInfo ScramCredentialInfo { get; set; }
        
        /// <summary>
        ///     Password to HMAC before storage
        /// </summary>
        public byte[] Password { get; set; }
        
        /// <summary>
        ///     Salt to use. Will be generated randomly if null (optional)
        /// </summary>
        public byte[] Salt { get; set; }

        /// <summary>
        ///     Returns a JSON representation of the UserScramCredentialUpsertion object.
        /// </summary>
        /// <returns>
        ///     A JSON representation the UserScramCredentialUpsertion object.
        /// </returns>
        public override string ToString()
        {
            var result = new StringBuilder();
            result.Append(
                "{\"User\": ");
            result.Append(User.ToString().Quote());
            result.Append(
                ", \"ScramCredentialInfo\": ");
            result.Append(ScramCredentialInfo.ToString());
            // Password and Salt aren't included to
            // avoid accidental leak.
            result.Append("}");
            return  result.ToString();
        }
    }
}
