using System.ComponentModel.DataAnnotations;
using System.Net;
using System.Net.Sockets;
using Cel.Common.Types;
using Cel.Common.Types.Ref;
using Cel.Interpreter.Functions;

namespace Confluent.SchemaRegistry.Rules
{
    public class BuiltinOverload
    {
        private const string OverloadIsEmail = "isEmail";
        private const string OverloadIsHostname = "isHostname";
        private const string OverloadIsIpv4 = "isIpv4";
        private const string OverloadIsIpv6 = "isIpv6";
        private const string OverloadIsUri = "isUri";
        private const string OverloadIsUriRef = "isUriRef";
        private const string OverloadIsUuid = "isUuid";

        public static Overload[] Create()
        {
            return new Overload[]
            {
                IsEmail(),
                IsHostname(),
                IsIpv4(),
                IsIpv6(),
                IsUri(),
                IsUriRef(),
                IsUuid(),
            };
        }

        private static Overload IsEmail()
        {
            return Overload.Unary(
                OverloadIsEmail,
                value =>
                {
                    if (value.Type().TypeEnum() != TypeEnum.String)
                    {
                        return Err.NoSuchOverload(value, OverloadIsEmail, null);
                    }

                    string input = (string)value.Value();
                    return string.IsNullOrEmpty(input)
                        ? BoolT.False
                        : Types.BoolOf(ValidateEmail(input));
                });
        }

        private static Overload IsHostname()
        {
            return Overload.Unary(
                OverloadIsHostname,
                value =>
                {
                    if (value.Type().TypeEnum() != TypeEnum.String)
                    {
                        return Err.NoSuchOverload(value, OverloadIsHostname, null);
                    }

                    string input = (string)value.Value();
                    return string.IsNullOrEmpty(input)
                        ? BoolT.False
                        : Types.BoolOf(ValidateHostname(input));
                });
        }

        private static Overload IsIpv4()
        {
            return Overload.Unary(
                OverloadIsIpv4,
                value =>
                {
                    if (value.Type().TypeEnum() != TypeEnum.String)
                    {
                        return Err.NoSuchOverload(value, OverloadIsIpv4, null);
                    }

                    string input = (string)value.Value();
                    return string.IsNullOrEmpty(input)
                        ? BoolT.False
                        : Types.BoolOf(ValidateIpv4(input));
                });
        }

        private static Overload IsIpv6()
        {
            return Overload.Unary(
                OverloadIsIpv6,
                value =>
                {
                    if (value.Type().TypeEnum() != TypeEnum.String)
                    {
                        return Err.NoSuchOverload(value, OverloadIsIpv6, null);
                    }

                    string input = (string)value.Value();
                    return string.IsNullOrEmpty(input)
                        ? BoolT.False
                        : Types.BoolOf(ValidateIpv6(input));
                });
        }

        private static Overload IsUri()
        {
            return Overload.Unary(
                OverloadIsUri,
                value =>
                {
                    if (value.Type().TypeEnum() != TypeEnum.String)
                    {
                        return Err.NoSuchOverload(value, OverloadIsUri, null);
                    }

                    string input = (string)value.Value();
                    return string.IsNullOrEmpty(input)
                        ? BoolT.False
                        : Types.BoolOf(ValidateUri(input));
                });
        }

        private static Overload IsUriRef()
        {
            return Overload.Unary(
                OverloadIsUriRef,
                value =>
                {
                    if (value.Type().TypeEnum() != TypeEnum.String)
                    {
                        return Err.NoSuchOverload(value, OverloadIsUriRef, null);
                    }

                    string input = (string)value.Value();
                    return string.IsNullOrEmpty(input)
                        ? BoolT.False
                        : Types.BoolOf(ValidateUriRef(input));
                });
        }

        private static Overload IsUuid()
        {
            return Overload.Unary(
                OverloadIsUuid,
                value =>
                {
                    if (value.Type().TypeEnum() != TypeEnum.String)
                    {
                        return Err.NoSuchOverload(value, OverloadIsUuid, null);
                    }

                    string input = (string)value.Value();
                    return string.IsNullOrEmpty(input)
                        ? BoolT.False
                        : Types.BoolOf(ValidateUuid(input));
                });
        }

        public static bool ValidateEmail(string input)
        {
            return new EmailAddressAttribute().IsValid(input);
        }

        public static bool ValidateHostname(string input)
        {
            return Uri.CheckHostName(input) != UriHostNameType.Unknown;
        }

        public static bool ValidateIpv4(string input)
        {
            if (IPAddress.TryParse(input, out IPAddress address))
            {
                return address.AddressFamily == AddressFamily.InterNetwork;
            }

            return false;
        }

        public static bool ValidateIpv6(string input)
        {
            if (IPAddress.TryParse(input, out IPAddress address))
            {
                return address.AddressFamily == AddressFamily.InterNetworkV6;
            }

            return false;
        }

        public static bool ValidateUri(string input)
        {
            return Uri.TryCreate(input, UriKind.Absolute, out _);
        }

        public static bool ValidateUriRef(string input)
        {
            return Uri.TryCreate(input, UriKind.RelativeOrAbsolute, out _);
        }

        public static bool ValidateUuid(string input)
        {
            return Guid.TryParse(input, out _);
        }
    }
}
