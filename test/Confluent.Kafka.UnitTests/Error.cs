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

using System;
using Xunit;


namespace Confluent.Kafka.UnitTests
{
    public class ErrorTests
    {
        [Fact]
        public void Constuctor()
        {
            var e1 = new Error(ErrorCode.Local_BadCompression);
            Assert.Equal(ErrorCode.Local_BadCompression, e1.Code);
            Assert.NotNull(e1.Reason);
            Assert.Equal("Local: Invalid compressed data", e1.Reason);
            Assert.False(e1.IsFatal);

            var e2 = new Error(ErrorCode.Local_BadMsg, "Dummy message");
            Assert.Equal(ErrorCode.Local_BadMsg, e2.Code);
            Assert.Equal("Dummy message", e2.Reason);
            Assert.False(e2.IsFatal);

            var e3 = new Error(ErrorCode.Local_Fatal);
            Assert.True(e3.IsFatal);
            Assert.NotNull(e3.Reason);
            Assert.Equal(ErrorCode.Local_Fatal, e3.Code);

            var e4 = new Error(ErrorCode.Local_Conflict, "some message", true);
            Assert.True(e4.IsFatal);
            Assert.Equal("some message", e4.Reason);
            Assert.Equal(ErrorCode.Local_Conflict, e4.Code);

            var e5 = new Error(ErrorCode.Local_ExistingSubscription, "some message", false);
            Assert.False(e5.IsFatal);
            Assert.Equal("some message", e5.Reason);
            Assert.Equal(ErrorCode.Local_ExistingSubscription, e5.Code);

            Assert.Throws<ArgumentException>(() => { new Error(ErrorCode.Local_Fatal, "some message", false); });
        }

        [Fact]
        public void Equality()
        {
            var e1 = new Error(ErrorCode.Local_AllBrokersDown);
            var e2 = new Error(ErrorCode.Local_AllBrokersDown);
            var e3 = new Error(ErrorCode.Local_InProgress);
            var e4 = new Error(ErrorCode.Local_InProgress, "Dummy message");
            var e5 = new Error(ErrorCode.Local_AllBrokersDown, null, false);
            var e6 = new Error(ErrorCode.Local_AllBrokersDown, null, true);

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

            Assert.Equal(e5, e1);
            Assert.NotEqual(e6, e1);
        }

        [Fact]
        public void NullEquality()
        {
            var e1 = new Error(ErrorCode.Local_AllBrokersDown);
            var e2 = new Error(ErrorCode.Local_AllBrokersDown, null, true);
            Error e3 = null;
            Error e4 = null;

            Assert.NotEqual(e1, e3);
            Assert.NotEqual(e2, e3);
            Assert.False(e1.Equals(e3));
            Assert.False(e2.Equals(e4));
            Assert.False(e1 == e3);
            Assert.False(e2 == e3);
            Assert.True(e1 != e3);
            Assert.True(e2 != e3);

            Assert.NotEqual(e3, e1);
            Assert.False(e3 == e1);
            Assert.True(e3 != e1);
            Assert.NotEqual(e3, e2);
            Assert.False(e3 == e2);
            Assert.True(e3 != e2);

            Assert.Equal(e3, e4);
            Assert.True(e3 == e4);
            Assert.False(e3 != e4);
        }

        [Fact]
        public void HasError()
        {
            var e1 = new Error(ErrorCode.NoError);
            var e2 = new Error(ErrorCode.NotCoordinatorForGroup);
            var e3 = new Error(ErrorCode.Local_Fatal);
            var e4 = new Error(ErrorCode.Local_Intr, null, false);

            Assert.False(e1.IsError);
            Assert.True(e2.IsError);
            Assert.True(e3.IsError);
            Assert.True(e4.IsError);
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
