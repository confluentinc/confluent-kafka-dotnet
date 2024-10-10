// Copyright 2024 Confluent Inc.
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


namespace Confluent.Kafka
{
    /// <summary>
    ///     Enumerates the different types of Election types.
    /// </summary>
    public enum ElectionType : int
    {
        /// <summary>
        ///     Preferred Elections
        ///  </summary>
        Preferred = 0,

        /// <summary>
        ///     Unclean Elections
        /// </summary>
        Unclean = 1,
    }
}
