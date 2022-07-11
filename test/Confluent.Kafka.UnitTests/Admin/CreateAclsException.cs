// Copyright 2022 Confluent Inc.
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
using Confluent.Kafka.Admin;
using Xunit;


namespace Confluent.Kafka.UnitTests
{
    public class CreateAclsExceptionTests
    {
        [Fact]
        public void Equality()
        {
            var ex1 = new CreateAclsException(new List<CreateAclReport>());
            var ex2 = new CreateAclsException(new List<CreateAclReport>
            {
                new CreateAclReport()
                {
                    Error = null,
                }
            });
            var ex3 = new CreateAclsException(new List<CreateAclReport>
            {
                new CreateAclReport()
                {
                    Error = new Error(ErrorCode.NoError, "Success", false),
                }
            });
            var ex4 = new CreateAclsException(new List<CreateAclReport>
            {
                new CreateAclReport()
                {
                    Error = new Error(ErrorCode.NoError, "Other message", false),
                }
            });
            var ex5 = new CreateAclsException(new List<CreateAclReport>
            {
                new CreateAclReport()
                {
                    Error = new Error(ErrorCode.Unknown, "Other message", false),
                }
            });

            Assert.NotEqual(ex1, ex2);
            Assert.True(ex1 != ex2);
            Assert.NotEqual(ex2, ex3);
            Assert.True(ex2 != ex3);
            Assert.Equal(ex3, ex4);
            Assert.True(ex3 == ex4);
            Assert.NotEqual(ex4, ex5);
            Assert.True(ex4 != ex5);
        }
    }
}
