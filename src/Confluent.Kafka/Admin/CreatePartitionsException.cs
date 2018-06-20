// Copyright 2018 Confluent Inc.
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
using System.Linq;
using System.Collections.Generic;


namespace Confluent.Kafka.Admin
{
    public class CreatePartitionsException : Exception
    {
        public CreatePartitionsException(List<CreatePartitionsResult> results)
            : base(
                "An error occurred creating partitions for topics: [" +
                String.Join(", ", results.Where(r => r.Error.IsError).Select(r => r.Topic)) +
                "]. Inspect the Results property of this exception for further information.")
        {
            Results = results;
        }
        
        public List<CreatePartitionsResult> Results { get; }
    }
}
