﻿using System;
using System.IO;

namespace Confluent.Kafka.Impl
{
    class PlatformApis
    {
        private static readonly Lazy<PlatformApis.DistroInfo> _distroInfo = new Lazy<PlatformApis.DistroInfo>(new Func<PlatformApis.DistroInfo>(PlatformApis.LoadDistroInfo));

        public static string GetOSName()
        {
            return PlatformApis.GetDistroId() ?? "Linux";
        }

        public static string GetOSVersion()
        {
            return PlatformApis.GetDistroVersionId() ?? string.Empty;
        }

        private static string GetDistroId() => PlatformApis._distroInfo.Value?.Id;
        private static string GetDistroVersionId() => PlatformApis._distroInfo.Value?.VersionId;

        private static DistroInfo LoadDistroInfo()
        {
            DistroInfo distroInfo = (DistroInfo)null;
            if (File.Exists("/etc/os-release"))
            {
                string[] strArray = File.ReadAllLines("/etc/os-release");
                distroInfo = new DistroInfo();
                foreach (string str in strArray)
                {
                    if (str.StartsWith("ID=", StringComparison.Ordinal))
                        distroInfo.Id = str.Substring(3).Trim('"', '\'');
                    else if (str.StartsWith("VERSION_ID=", StringComparison.Ordinal))
                        distroInfo.VersionId = str.Substring(11).Trim('"', '\'');
                }
            }
            else if (File.Exists("/etc/redhat-release"))
            {
                string[] strArray = File.ReadAllLines("/etc/redhat-release");
                if (strArray.Length >= 1)
                {
                    string str = strArray[0];
                    if (str.StartsWith("Red Hat Enterprise Linux Server release 6.") || str.StartsWith("CentOS release 6."))
                    {
                        distroInfo = new DistroInfo();
                        distroInfo.Id = "rhel";
                        distroInfo.VersionId = "6";
                    }
                }
            }
            if (distroInfo != null)
                distroInfo = NormalizeDistroInfo(distroInfo);
            return distroInfo;
        }

        private static DistroInfo NormalizeDistroInfo(
          DistroInfo distroInfo)
        {
            string versionId = distroInfo.VersionId;
            int length = versionId != null ? versionId.IndexOf('.') : -1;
            if (length != -1 && distroInfo.Id == "alpine")
                length = distroInfo.VersionId.IndexOf('.', length + 1);
            if (length != -1 && (distroInfo.Id == "rhel" || distroInfo.Id == "alpine"))
                distroInfo.VersionId = distroInfo.VersionId.Substring(0, length);
            return distroInfo;
        }

        class DistroInfo
        {
            public string Id;
            public string VersionId;
        }
    }
}
