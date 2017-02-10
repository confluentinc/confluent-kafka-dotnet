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
            var e = new Error(ErrorCode.Local_BadCompression);
            Assert.Equal(e.Code, ErrorCode.Local_BadCompression);
            Assert.NotNull(e.Reason);
            Assert.Equal(e.Reason, "Local: Invalid compressed data");

            var e2 = new Error(ErrorCode.Local_BadMsg, "Dummy message");
            Assert.Equal(e2.Code, ErrorCode.Local_BadMsg);
            Assert.Equal(e2.Reason, "Dummy message");
        }

        [Fact]
        public void Equality()
        {
            var e1 = new Error(ErrorCode.Local_AllBrokersDown);
            var e2 = new Error(ErrorCode.Local_AllBrokersDown);
            var e3 = new Error(ErrorCode.Local_InProgress);
            var e4 = new Error(ErrorCode.Local_InProgress, "Dummy message");

            Assert.Equal(e1, e2);
            Assert.True(e1.Equals(e2));
            Assert.True(e1 == e2);
            Assert.False(e1 != e2);

            Assert.NotEqual(e1, e3);
            Assert.False(e1.Equals(e3));
            Assert.False(e1 == e3);
            Assert.True(e1 != e3);

            Assert.Equal(e3, e4);
            Assert.True(e3 == e4);
            Assert.NotEqual(e2, e4);
            Assert.False(e2 == e4);
        }

        [Fact]
        public void HasError()
        {
            var e1 = new Error(ErrorCode.NoError);
            var e2 = new Error(ErrorCode.NotCoordinatorForGroup);

            Assert.False(e1.HasError);
            Assert.True(e2.HasError);
            Assert.False(e1);
            Assert.True(e2);
        }

        [Fact]
        public void BoolCast()
        {
            var e1 = new Error(ErrorCode.NoError);
            var e2 = new Error(ErrorCode.NotCoordinatorForGroup);

            Assert.False(e1);
            Assert.True(e2);
        }

        [Fact]
        public void ErrorCodeCast()
        {
            var e1 = new Error(ErrorCode.NotCoordinatorForGroup);
            var ec1 = ErrorCode.NotCoordinatorForGroup;

            Assert.Equal((ErrorCode)e1, ec1);
            Assert.Equal(ec1, (ErrorCode)e1);
        }
    }
}
