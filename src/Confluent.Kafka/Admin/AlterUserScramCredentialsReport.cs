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


namespace Confluent.Kafka.Admin
{
    /// <summary>
    ///     The per-user result for an alter user scram credentials request,
    ///     including errors.
    /// </summary>
    public class AlterUserScramCredentialsReport
    {
        /// <summary>
        ///     Username for the performed Alteration.
        /// </summary>
        public string User { get; set; }

        /// <summary>
        ///     Error of the performed Alteration.
        /// </summary>
        public Error Error { get; set; }

        /// <summary>
        ///     Returns a JSON representation of the AlterUserScramCredentialsReport object.
        /// </summary>
        /// <returns>
        ///     A JSON representation the AlterUserScramCredentialsReport object.
        /// </returns>
        public override string ToString()
        {
            return $"{{\"User\": {User.Quote()}, " + 
                   $"\"Error\": {Error.ToString().Quote()}}}";
        }
    }
}
