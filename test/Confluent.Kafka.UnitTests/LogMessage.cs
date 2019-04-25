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
using System.Diagnostics;


namespace Confluent.Kafka.UnitTests
{
    public class LogMessageTests
    {
        [Fact]
        public void Constuctor()
        {
            var lm = new LogMessage("myname", SyslogLevel.Error, "myfacility", "mymessage");
            Assert.Equal("myname", lm.Name);
            Assert.Equal(SyslogLevel.Error, lm.Level);
            Assert.Equal("myfacility", lm.Facility);
            Assert.Equal("mymessage", lm.Message);
        }

        [Fact]
        public void SystemDiagnosticsConvert()
        {
            var lm = new LogMessage("name", SyslogLevel.Emergency, "facility", "message");
            Assert.Equal((int)TraceLevel.Error, lm.LevelAs(LogLevelType.SystemDiagnostics));
            lm = new LogMessage("name", SyslogLevel.Alert, "facility", "message");
            Assert.Equal((int)TraceLevel.Error, lm.LevelAs(LogLevelType.SystemDiagnostics));
            lm = new LogMessage("name", SyslogLevel.Critical, "facility", "message");
            Assert.Equal((int)TraceLevel.Error, lm.LevelAs(LogLevelType.SystemDiagnostics));
            lm = new LogMessage("name", SyslogLevel.Error, "facility", "message");
            Assert.Equal((int)TraceLevel.Error, lm.LevelAs(LogLevelType.SystemDiagnostics));
            lm = new LogMessage("name", SyslogLevel.Warning, "facility", "message");
            Assert.Equal((int)TraceLevel.Warning, lm.LevelAs(LogLevelType.SystemDiagnostics));
            lm = new LogMessage("name", SyslogLevel.Notice, "facility", "message");
            Assert.Equal((int)TraceLevel.Info, lm.LevelAs(LogLevelType.SystemDiagnostics));
            lm = new LogMessage("name", SyslogLevel.Info, "facility", "message");
            Assert.Equal((int)TraceLevel.Info, lm.LevelAs(LogLevelType.SystemDiagnostics));
            lm = new LogMessage("name", SyslogLevel.Debug, "facility", "message");
            Assert.Equal((int)TraceLevel.Verbose, lm.LevelAs(LogLevelType.SystemDiagnostics));
        }

        [Fact]
        public void MicrosoftExtensionsLogging()
        {
            // Microsoft.Extensions.Logging.LogLevel: [0] Trace, [1] Debug, [2] Information, [3] Warning, [4] Error, [5] Critical, [6] None
            // Note: Package containing Microsoft.Extensions.Logging.LogLevel is not referenced
            //       so as to retain compatibility with net452.

            var lm = new LogMessage("name", SyslogLevel.Emergency, "facility", "message");
            Assert.Equal(5, lm.LevelAs(LogLevelType.MicrosoftExtensionsLogging));
            lm = new LogMessage("name", SyslogLevel.Alert, "facility", "message");
            Assert.Equal(5, lm.LevelAs(LogLevelType.MicrosoftExtensionsLogging));
            lm = new LogMessage("name", SyslogLevel.Critical, "facility", "message");
            Assert.Equal(5, lm.LevelAs(LogLevelType.MicrosoftExtensionsLogging));
            lm = new LogMessage("name", SyslogLevel.Error, "facility", "message");
            Assert.Equal(4, lm.LevelAs(LogLevelType.MicrosoftExtensionsLogging));
            lm = new LogMessage("name", SyslogLevel.Warning, "facility", "message");
            Assert.Equal(3, lm.LevelAs(LogLevelType.MicrosoftExtensionsLogging));
            lm = new LogMessage("name", SyslogLevel.Notice, "facility", "message");
            Assert.Equal(2, lm.LevelAs(LogLevelType.MicrosoftExtensionsLogging));
            lm = new LogMessage("name", SyslogLevel.Info, "facility", "message");
            Assert.Equal(2, lm.LevelAs(LogLevelType.MicrosoftExtensionsLogging));
            lm = new LogMessage("name", SyslogLevel.Debug, "facility", "message");
            Assert.Equal(1, lm.LevelAs(LogLevelType.MicrosoftExtensionsLogging));
        }

    }
}
