// Copyright 2016-2017 Confluent Inc.
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

using Xunit;


namespace Confluent.Kafka.Tests
{
    public class ErrorTests
    {
        [Fact]
        public void Constuctor()
        {
            var e = new Error(ErrorCode._BAD_COMPRESSION);
            Assert.Equal(e.Code, ErrorCode._BAD_COMPRESSION);
            Assert.NotNull(e.Message);
        }

        [Fact]
        public void Equality()
        {
            var e1 = new Error(ErrorCode._ALL_BROKERS_DOWN);
            var e2 = new Error(ErrorCode._ALL_BROKERS_DOWN);
            var e3 = new Error(ErrorCode._IN_PROGRESS);

            Assert.Equal(e1, e2);
            Assert.True(e1.Equals(e2));
            Assert.True(e1 == e2);
            Assert.False(e1 != e2);

            Assert.NotEqual(e1, e3);
            Assert.False(e1.Equals(e3));
            Assert.False(e1 == e3);
            Assert.True(e1 != e3);
        }

        [Fact]
        public void ToStringTest()
        {
            var e = new Error(ErrorCode._ASSIGN_PARTITIONS);
            Assert.True(e.ToString().Contains(((int)ErrorCode._ASSIGN_PARTITIONS).ToString()));
        }

        [Fact]
        public void HasError()
        {
            var e1 = new Error(ErrorCode.NO_ERROR);
            var e2 = new Error(ErrorCode.NOT_COORDINATOR_FOR_GROUP);

            Assert.False(e1.HasError);
            Assert.True(e2.HasError);
        }

        [Fact]
        public void BoolCast()
        {
            var e1 = new Error(ErrorCode.NO_ERROR);
            var e2 = new Error(ErrorCode.NOT_COORDINATOR_FOR_GROUP);

            Assert.False(e1);
            Assert.True(e2);
        }

        [Fact]
        public void ErrorCodeCast()
        {
            var e1 = new Error(ErrorCode.NOT_COORDINATOR_FOR_GROUP);
            var ec1 = ErrorCode.NOT_COORDINATOR_FOR_GROUP;

            Assert.Equal((ErrorCode)e1, ec1);
            Assert.Equal(ec1, (ErrorCode)e1);
        }
    }
}
