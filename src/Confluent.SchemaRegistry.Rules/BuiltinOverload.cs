using System;
using System.ComponentModel.DataAnnotations;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using Cel.Common.Types;
using Cel.Common.Types.Ref;
using Cel.Common.Types.Traits;
using Cel.Interpreter.Functions;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using SrVariant = Confluent.SchemaRegistry.Variant;

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
            return new[]
            {
                IsEmail(),
                IsHostname(),
                IsIpv4(),
                IsIpv6(),
                IsUri(),
                IsUriRef(),
                IsUuid(),

                // decimal(dyn) runtime-dispatches on the actual type; decimal(bytes, int)
                // builds from unscaled bytes plus scale. string(Decimal)/double(Decimal) are
                // not registered here — the stdlib string/double overloads reach DecimalT
                // through ConvertToType.
                Overload.NewOverload(
                    "decimal", Trait.None,
                    v => Guard(() => DecimalT.Of(DecimalUtils.ToBigDecimal(v.Value()))),
                    (a, b) => Guard(() => DecimalT.Of(DecimalUtils.ToBigDecimal(
                        ToBytes(a, "decimal(bytes, scale)", "first argument"),
                        RequireIntScale(ToLong(b), "decimal(bytes, scale)")))),
                    null),

                DecimalsBinaryBool("decimals.eq", (a, b) => a.CompareTo(b) == 0),
                DecimalsBinaryBool("decimals.lt", (a, b) => a.CompareTo(b) < 0),
                DecimalsBinaryBool("decimals.le", (a, b) => a.CompareTo(b) <= 0),
                DecimalsBinaryBool("decimals.gt", (a, b) => a.CompareTo(b) > 0),
                DecimalsBinaryBool("decimals.ge", (a, b) => a.CompareTo(b) >= 0),

                DecimalsBinary("decimals.add", (a, b) => a.Add(b)),
                DecimalsBinary("decimals.sub", (a, b) => a.Subtract(b)),
                DecimalsBinary("decimals.mul", (a, b) => a.Multiply(b)),
                DecimalsBinary("decimals.div", (a, b) => a.Divide(b)),
                DecimalsBinary("decimals.mod", (a, b) => a.Remainder(b)),

                DecimalsUnary("decimals.sqrt", d => d.Sqrt()),

                DecimalsBinary("decimals.greatest", (a, b) => a.Max(b)),
                DecimalsBinary("decimals.least", (a, b) => a.Min(b)),

                DecimalsUnary("decimals.neg", d => d.Negate()),
                DecimalsUnary("decimals.abs", d => d.Abs()),
                Overload.Unary("decimals.sign", v => Guard(() => IntT.IntOf(ToDecimal(v).Signum))),

                Overload.NewOverload(
                    "decimals.round", Trait.None,
                    v => Guard(() => DecimalT.Of(ToDecimal(v).SetScale(0, BigDecimal.Rounding.HalfUp))),
                    (a, b) => Guard(() =>
                        DecimalT.Of(ToDecimal(a).SetScale(
                            RequireIntScale(ToLong(b), "decimals.round"), BigDecimal.Rounding.HalfUp))),
                    null),
                Overload.NewOverload(
                    "decimals.trunc", Trait.None,
                    v => Guard(() => DecimalT.Of(TruncUnary(ToDecimal(v)))),
                    (a, b) => Guard(() => DecimalT.Of(TruncScale(
                        ToDecimal(a), RequireIntScale(ToLong(b), "decimals.trunc")))),
                    null),
                DecimalsUnary("decimals.floor", d => d.SetScale(0, BigDecimal.Rounding.Floor)),
                DecimalsUnary("decimals.ceil", d => d.SetScale(0, BigDecimal.Rounding.Ceiling)),

                // One overload on the *standard* timestamp constructor: the binary
                // timestamp(int, int) precision form. cel.net indexes overloads by function
                // name, and its dispatcher merges two definitions of one name when their arity
                // slots are disjoint — so leaving the unary slot null keeps the standard
                // timestamp(dyn) conversion (string, int-as-seconds, identity) intact.
                Overload.NewOverload(
                    "timestamp", Trait.None,
                    null,
                    (a, b) => Guard(() =>
                        TimestampT.TimestampOf(TimestampUtils.FromEpochPrecision(ToLong(a), ToLong(b)))),
                    null),

                // variant(dyn) runtime-dispatches on the actual type; variant(bytes, bytes)
                // builds directly from (value, metadata) bytes. CEL null passes through as
                // CEL null (matching the navigation accessors and the Java reference), rather
                // than erroring.
                Overload.NewOverload(
                    "variant", Trait.None,
                    v => IsCelNull(v)
                        ? NullT.NullValue
                        : Guard(() => VariantT.Of(VariantUtils.ToVariant(v.Value()))),
                    (a, b) => Guard(() => (IVal)VariantT.Of(new SrVariant(
                        ToBytes(a, "variant(value, metadata)", "value"),
                        ToBytes(b, "variant(value, metadata)", "metadata")))),
                    null),

                Overload.Unary("variants.parseJson", v => Guard(() => VariantParseJson(v))),
                Overload.Unary("variants.tryParseJson", VariantTryParseJson),
                Overload.Unary("variants.type", v => Guard(() => VariantTypeLabel(v))),
                Overload.Unary("variants.isNull", VariantIsNull),
                Overload.Binary("variants.path", (a, b) => Guard(() => VariantPathFn(a, b))),
                Overload.Binary("variants.field", (a, b) => Guard(() => VariantField(a, b))),
                Overload.Binary("variants.index", (a, b) => Guard(() => VariantIndex(a, b))),
                Overload.Binary("variants.as", (a, b) => Guard(() => VariantAs(a, b, false))),
                Overload.Binary("variants.tryAs", (a, b) => Guard(() => VariantAs(a, b, true))),
                Overload.Unary("variants.toJson", v => Guard(() => VariantToJson(v))),
            };
        }

        // ---- Variant helpers ----

        // A CEL-null argument: a missing IVal, a NullT, or an IVal wrapping a null value.
        private static bool IsCelNull(IVal v) => v == null || v is NullT || v.Value() == null;

        // A variants.* navigation receiver: CEL null passes through as null; a VariantT,
        // raw Variant, proto confluent.type.Variant message, or map is converted; anything
        // else is a hard error (VariantUtils.ToVariant throws).
        private static SrVariant ReceiverVariantOrNull(IVal v)
        {
            if (IsCelNull(v))
            {
                return null;
            }

            return v is VariantT vt ? vt.Variant : VariantUtils.ToVariant(v.Value());
        }

        private static string StringArg(IVal v) => (string)v.Value();

        private static IVal VariantParseJson(IVal v)
        {
            if (!(v.Value() is string s))
            {
                throw new ArgumentException("variants.parseJson: expected a string");
            }

            return VariantT.Of(SrVariant.ParseJson(s));
        }

        private static IVal VariantTryParseJson(IVal v)
        {
            try
            {
                return v.Value() is string s ? VariantT.Of(SrVariant.ParseJson(s)) : (IVal)NullT.NullValue;
            }
            catch (Exception)
            {
                // Soft form: any parse failure -> CEL null.
                return NullT.NullValue;
            }
        }

        private static IVal VariantTypeLabel(IVal v)
        {
            SrVariant variant = ReceiverVariantOrNull(v);
            return variant == null
                ? (IVal)NullT.NullValue
                : StringT.StringOf(VariantUtils.TypeLabel(variant.GetVariantType()));
        }

        // true iff the input is a Variant whose top type is NULL; never throws (a non-Variant
        // input is simply false), matching the Python/JS isNull.
        private static IVal VariantIsNull(IVal v)
        {
            // Coerces like every other accessor. `v.Value() as SrVariant` answered null for the
            // shapes a variant-typed field decodes to - a confluent.type.Variant message, or the
            // map an Avro variant record yields - which the dyn declaration admits, so a bare
            // variant holding an explicit JSON null reported "not null". A non-variant stays
            // false rather than throwing: this predicate never errors.
            SrVariant variant;
            if (v is VariantT vt)
            {
                variant = vt.Variant;
            }
            else
            {
                try
                {
                    variant = ReceiverVariantOrNull(v);
                }
                catch (Exception)
                {
                    return Types.BoolOf(false);
                }
            }

            return Types.BoolOf(variant != null && variant.GetVariantType() == VariantType.Null);
        }

        private static IVal VariantPathFn(IVal a, IVal b)
        {
            SrVariant v = ReceiverVariantOrNull(a);
            if (v == null)
            {
                return NullT.NullValue;
            }

            SrVariant r = VariantPath.Walk(v, StringArg(b));
            return r == null ? (IVal)NullT.NullValue : VariantT.Of(r);
        }

        private static IVal VariantField(IVal a, IVal b)
        {
            SrVariant v = ReceiverVariantOrNull(a);
            if (v == null || v.GetVariantType() != VariantType.Object)
            {
                return NullT.NullValue;
            }

            SrVariant r = v.GetFieldByKey(StringArg(b));
            return r == null ? (IVal)NullT.NullValue : VariantT.Of(r);
        }

        private static IVal VariantIndex(IVal a, IVal b)
        {
            SrVariant v = ReceiverVariantOrNull(a);
            if (v == null || v.GetVariantType() != VariantType.Array)
            {
                return NullT.NullValue;
            }

            SrVariant r = v.GetElementAtIndex((int)ToLong(b));
            return r == null ? (IVal)NullT.NullValue : VariantT.Of(r);
        }

        private static IVal VariantToJson(IVal v)
        {
            SrVariant variant = ReceiverVariantOrNull(v);
            return variant == null ? (IVal)NullT.NullValue : StringT.StringOf(variant.ToJson());
        }

        // Backing for variants.as (strict) and variants.tryAs (soft). Extracts a typed value;
        // on a type mismatch the strict form throws and the soft form returns CEL null. Types
        // with no CEL scalar extraction (object/array/null/date/time/uuid) always throw.
        private static IVal VariantAs(IVal a, IVal b, bool nullOnError)
        {
            SrVariant v = ReceiverVariantOrNull(a);
            if (v == null)
            {
                return NullT.NullValue;
            }

            string t = StringArg(b);
            VariantType vt = v.GetVariantType();
            switch (t)
            {
                case "string":
                    if (vt == VariantType.String)
                    {
                        return StringT.StringOf(v.GetString());
                    }

                    break;
                case "int":
                    if (vt == VariantType.Byte || vt == VariantType.Short ||
                        vt == VariantType.Int || vt == VariantType.Long)
                    {
                        return IntT.IntOf(v.GetLong());
                    }

                    break;
                case "double":
                    if (vt == VariantType.Float)
                    {
                        return DoubleT.DoubleOf(v.GetFloat());
                    }
                    if (vt == VariantType.Double)
                    {
                        return DoubleT.DoubleOf(v.GetDouble());
                    }

                    break;
                case "boolean":
                    if (vt == VariantType.Boolean)
                    {
                        return Types.BoolOf(v.GetBoolean());
                    }

                    break;
                case "decimal":
                    if (vt == VariantType.Decimal4 || vt == VariantType.Decimal8 ||
                        vt == VariantType.Decimal16)
                    {
                        return DecimalT.Of(v.GetDecimal());
                    }

                    break;
                case "timestamp":
                    if (vt == VariantType.TimestampTz || vt == VariantType.TimestampNtz ||
                        vt == VariantType.TimestampNanosTz || vt == VariantType.TimestampNanosNtz)
                    {
                        return VariantTimestamp(v, vt);
                    }

                    break;
                case "bytes":
                    if (vt == VariantType.Binary)
                    {
                        return BytesT.BytesOf(v.GetBinary());
                    }

                    break;
                case "object":
                case "array":
                case "null":
                case "date":
                case "time":
                case "uuid":
                    // Not extractable as a CEL scalar - always an error, even in the soft form.
                    throw new ArgumentException(
                        "variants.as: type '" + t + "' is not supported for extraction (use "
                        + "variants.type/variants.path/variants.field/variants.index instead)");
                default:
                    if (nullOnError)
                    {
                        return NullT.NullValue;
                    }

                    throw new ArgumentException(
                        "variants.as: unknown type '" + t + "' (expected one of: string, int, "
                        + "double, boolean, decimal, timestamp, bytes)");
            }

            // Recognized type string, but the variant's actual type does not match.
            if (nullOnError)
            {
                return NullT.NullValue;
            }

            throw new ArgumentException("variants.as: variant is not " + t + "-typed (type=" + vt + ")");
        }

        private static IVal VariantTimestamp(SrVariant v, VariantType vt)
        {
            long raw = v.GetLong();
            // TIMESTAMP_TZ / TIMESTAMP_NTZ store microseconds; the two nanos variants store
            // nanoseconds. The CEL surface is a proto Timestamp (seconds + int32 nanos), which
            // holds full nanosecond precision, so preserve the nanos of the NANOS variants
            // rather than collapsing to micros. FromEpoch* floor-divide, so negative epoch
            // values round toward -infinity (matching Java's variantGetTimestamp).
            Timestamp ts = vt == VariantType.TimestampTz || vt == VariantType.TimestampNtz
                ? TimestampUtils.FromEpochMicros(raw)
                : TimestampUtils.FromEpochNanos(raw);
            return TimestampT.TimestampOf(ts);
        }

        // ---- Decimal / timestamp helpers ----

        // Coerces rather than casting. A decimals.* receiver is a DecimalT when it came from
        // decimal(...) or another decimals.* result, but a bare confluent.type.Decimal message
        // when it came from protobuf field selection - which the dyn/object-typed declarations
        // now admit. A hard (BigDecimal) cast threw "Unable to cast object of type
        // 'Confluent.SchemaRegistry.Serdes.Protobuf.Decimal' to type 'BigDecimal'" on exactly
        // that shape; DecimalUtils.ToBigDecimal accepts it, and every other numeric shape too.
        private static BigDecimal ToDecimal(IVal v) =>
            v is DecimalT d ? d.Decimal : DecimalUtils.ToBigDecimal(v.Value());

        private static long ToLong(IVal v) => Convert.ToInt64(v.Value(), CultureInfo.InvariantCulture);

        // Narrow a CEL int (i64) to a Java/.NET int32 for use as a BigDecimal scale, throwing a
        // clear error on out-of-range values. CEL int is i64; BigDecimal scale is i32. A raw
        // (int) cast would silently take the lower 32 bits (e.g. 2^32 -> 0), yielding a wildly
        // wrong Decimal. Mirrors Java's DecimalUtils/BuiltinOverload.requireIntScale.
        private static int RequireIntScale(long scale, string functionName)
        {
            if (scale < int.MinValue || scale > int.MaxValue)
            {
                throw new ArgumentException(functionName + ": scale out of int range: " + scale);
            }

            return (int)scale;
        }

        // functionName/argument name the caller, because this helper serves both
        // decimal(bytes, scale) and variant(bytes, bytes) - a hard-coded "decimal(bytes, scale)"
        // reported the wrong function for a bad variant argument. Mirrors RequireIntScale, which
        // already takes the calling function's name.
        private static byte[] ToBytes(IVal v, string functionName, string argument)
        {
            switch (v.Value())
            {
                case byte[] b:
                    return b;
                case ByteString bs:
                    return bs.ToByteArray();
                default:
                    throw new ArgumentException(functionName + ": " + argument + " must be bytes");
            }
        }

        // Surface a conversion/arithmetic failure as a CEL error (like the Java client's
        // IllegalArgumentException), rather than throwing out of the interpreter.
        private static IVal Guard(Func<IVal> fn)
        {
            try
            {
                return fn();
            }
            catch (Exception e)
            {
                return Err.NewErr("{0}", e.Message);
            }
        }

        private static Overload DecimalsUnary(string name, Func<BigDecimal, BigDecimal> fn)
        {
            return Overload.Unary(name, v => Guard(() => DecimalT.Of(fn(ToDecimal(v)))));
        }

        private static Overload DecimalsBinary(string name, Func<BigDecimal, BigDecimal, BigDecimal> fn)
        {
            return Overload.Binary(name, (a, b) => Guard(() => DecimalT.Of(fn(ToDecimal(a), ToDecimal(b)))));
        }

        private static Overload DecimalsBinaryBool(string name, Func<BigDecimal, BigDecimal, bool> fn)
        {
            return Overload.Binary(name, (a, b) => Guard(() => Types.BoolOf(fn(ToDecimal(a), ToDecimal(b)))));
        }

        // Flink TRUNCATE early-returns when the target scale is at-or-finer than the current
        // scale, so it's a no-op there and the result keeps the input's representation.
        private static BigDecimal TruncUnary(BigDecimal d)
        {
            return d.Scale <= 0 ? d : d.SetScale(0, BigDecimal.Rounding.Down);
        }

        private static BigDecimal TruncScale(BigDecimal d, int scale)
        {
            return scale >= d.Scale ? d : d.SetScale(scale, BigDecimal.Rounding.Down);
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