using Cel.Checker;
using Google.Api.Expr.V1Alpha1;
using Type = Google.Api.Expr.V1Alpha1.Type;

namespace Confluent.SchemaRegistry.Rules
{
    public class BuiltinDeclarations
    {
        // Object type declaration for confluent.type.Decimal - deliberately an object type and
        // not an abstract one. DecimalT.Type() is TypeT.NewObjectTypeValue(DecimalName), and a
        // protobuf confluent.type.Decimal field is declared by CelExecutor.FindType as
        // Decls.NewObjectType of the same name; an abstract declaration is a third, incompatible
        // identity for the same logical type, so `decimals.eq(this, decimal("12.34"))` on a bare
        // decimal field failed to check with "found no matching overload ... applied to
        // '(confluent.type.Decimal, { abstractType: { name: confluent.type.Decimal } })'".
        // One kind for all three makes a bare decimal field usable with no decimal(...) call.
        private static readonly Type Decimal =
            Decls.NewObjectType(CelTypeLabels.DecimalName);

        // Abstract type declaration for confluent.type.Variant: the *result* type of
        // variant(...) and variants.parseJson(...). The variants.* accessors deliberately do
        // NOT take it as their receiver - they take dyn, as every other client declares them,
        // and coerce inside (see BuiltinOverload.ReceiverVariantOrNull, which accepts a
        // VariantT, a raw Variant, a confluent.type.Variant message, or a map). Declaring the
        // receiver as this abstract type instead would reject every one of those bare shapes:
        // VariantT.Type() is an object type and a protobuf variant field is declared as an
        // object type too, so `variants.field(this, 'name')` on a bare variant failed with
        // "found no matching overload ... applied to '(confluent.type.Variant, string)'".
        private static readonly Type Variant =
            Decls.NewAbstractType(CelTypeLabels.VariantName, new List<Type>());

        public static IList<Decl> Create()
        {
            IList<Decl> decls = new List<Decl>();

            decls.Add(
                Decls.NewFunction(
                    "isEmail",
                    Decls.NewInstanceOverload(
                        "is_email", new List<Type> { Decls.String }, Decls.Bool)));

            decls.Add(
                Decls.NewFunction(
                    "isHostname",
                    Decls.NewInstanceOverload(
                        "is_hostname", new List<Type> { Decls.String }, Decls.Bool)));

            decls.Add(
                Decls.NewFunction(
                    "isIpv4",
                    Decls.NewInstanceOverload(
                        "is_ipv4", new List<Type> { Decls.String }, Decls.Bool)));

            decls.Add(
                Decls.NewFunction(
                    "isIpv6",
                    Decls.NewInstanceOverload(
                        "is_ipv6", new List<Type> { Decls.String }, Decls.Bool)));

            decls.Add(
                Decls.NewFunction(
                    "isUriRef",
                    Decls.NewInstanceOverload(
                        "is_uri_ref", new List<Type> { Decls.String }, Decls.Bool)));

            decls.Add(
                Decls.NewFunction(
                    "isUri",
                    Decls.NewInstanceOverload(
                        "is_uri", new List<Type> { Decls.String }, Decls.Bool)));

            decls.Add(
                Decls.NewFunction(
                    "isUuid",
                    Decls.NewInstanceOverload(
                        "is_uuid", new List<Type> { Decls.String }, Decls.Bool)));

            AddDecimal(decls);
            AddTimestamp(decls);
            AddVariant(decls);

            return decls;
        }

        // ---- Variant ----

        private static void AddVariant(IList<Decl> decls)
        {
            // variant(...) constructor. (dyn) runtime-dispatches on the actual type (Avro
            // Variant, proto confluent.type.Variant, map); (bytes, bytes) builds directly
            // from (value, metadata) bytes.
            //
            // The (dyn) overload's result type is dyn for the same reason tryParseJson's is:
            // variant(null) yields CEL null (matching the Java reference), and an abstract type
            // is not comparable to null in the checker - declaring Variant there made
            // `variant(x) == null` and `type(variant(x))` fail to compile, even though the value
            // was right and composed correctly through the accessors. The (bytes, bytes) overload
            // keeps the Variant type because it can never return null.
            decls.Add(Decls.NewFunction(
                "variant",
                Decls.NewOverload("dyn_to_variant", new List<Type> { Decls.Dyn }, Decls.Dyn),
                Decls.NewOverload("bytes_bytes_to_variant",
                    new List<Type> { Decls.Bytes, Decls.Bytes }, Variant)));

            // Parsing: strict (raises on malformed) and soft (null on failure).
            decls.Add(Decls.NewFunction(
                "variants.parseJson",
                Decls.NewOverload("variants_parse_json", new List<Type> { Decls.String }, Variant)));
            // tryParseJson returns CEL null on failure, so its result type is dyn (an abstract
            // type is not comparable to null in the checker).
            decls.Add(Decls.NewFunction(
                "variants.tryParseJson",
                Decls.NewOverload("variants_try_parse_json", new List<Type> { Decls.String }, Decls.Dyn)));

            // Inspection.
            decls.Add(Decls.NewFunction(
                "variants.type",
                Decls.NewOverload("variants_type", new List<Type> { Decls.Dyn }, Decls.String)));
            decls.Add(Decls.NewFunction(
                "variants.isNull",
                Decls.NewOverload("variants_is_null", new List<Type> { Decls.Dyn }, Decls.Bool)));

            // Navigation: field/index/path return a Variant, or CEL null on a miss — so the
            // declared result is dyn (so `... == null` type-checks, and the result still feeds
            // another variants.* call).
            decls.Add(Decls.NewFunction(
                "variants.path",
                Decls.NewOverload("variants_path",
                    new List<Type> { Decls.Dyn, Decls.String }, Decls.Dyn)));
            decls.Add(Decls.NewFunction(
                "variants.field",
                Decls.NewOverload("variants_field",
                    new List<Type> { Decls.Dyn, Decls.String }, Decls.Dyn)));
            decls.Add(Decls.NewFunction(
                "variants.index",
                Decls.NewOverload("variants_index",
                    new List<Type> { Decls.Dyn, Decls.Int }, Decls.Dyn)));

            // Typed extraction: as (strict) / tryAs (null on mismatch) -> dyn.
            decls.Add(Decls.NewFunction(
                "variants.as",
                Decls.NewOverload("variants_as",
                    new List<Type> { Decls.Dyn, Decls.String }, Decls.Dyn)));
            decls.Add(Decls.NewFunction(
                "variants.tryAs",
                Decls.NewOverload("variants_try_as",
                    new List<Type> { Decls.Dyn, Decls.String }, Decls.Dyn)));

            // Serialization.
            decls.Add(Decls.NewFunction(
                "variants.toJson",
                Decls.NewOverload("variants_to_json", new List<Type> { Decls.Dyn }, Decls.String)));
        }

        // ---- Decimal ----

        private static void AddDecimal(IList<Decl> decls)
        {
            // decimal(...) constructor. (dyn) runtime-dispatches on the actual type (proto
            // confluent.type.Decimal, Avro AvroDecimal, numbers, string); (bytes, int) builds
            // from unscaled bytes plus scale. String is handled by the (dyn) arm.
            decls.Add(Decls.NewFunction(
                "decimal",
                Decls.NewOverload("dyn_to_decimal", new List<Type> { Decls.Dyn }, Decimal),
                Decls.NewOverload("bytes_int_to_decimal",
                    new List<Type> { Decls.Bytes, Decls.Int }, Decimal)));

            // Comparison: decimals.eq/lt/le/gt/ge.
            decls.Add(BinaryDecimal("decimals.eq", "decimals_eq_decimal_decimal", Decls.Bool));
            decls.Add(BinaryDecimal("decimals.lt", "decimals_lt_decimal_decimal", Decls.Bool));
            decls.Add(BinaryDecimal("decimals.le", "decimals_le_decimal_decimal", Decls.Bool));
            decls.Add(BinaryDecimal("decimals.gt", "decimals_gt_decimal_decimal", Decls.Bool));
            decls.Add(BinaryDecimal("decimals.ge", "decimals_ge_decimal_decimal", Decls.Bool));

            // Arithmetic: decimals.add/sub/mul/div/mod.
            decls.Add(BinaryDecimal("decimals.add", "decimals_add_decimal_decimal", Decimal));
            decls.Add(BinaryDecimal("decimals.sub", "decimals_sub_decimal_decimal", Decimal));
            decls.Add(BinaryDecimal("decimals.mul", "decimals_mul_decimal_decimal", Decimal));
            decls.Add(BinaryDecimal("decimals.div", "decimals_div_decimal_decimal", Decimal));
            // Modulo: remainder with the sign of the dividend.
            decls.Add(BinaryDecimal("decimals.mod", "decimals_mod_decimal_decimal", Decimal));

            // Square root — MathContext(38, HALF_UP), like div. Throws on a negative value.
            decls.Add(UnaryDecimal("decimals.sqrt", "decimals_sqrt_decimal", Decimal));

            // Selection: decimals.greatest/least return the larger/smaller operand.
            decls.Add(BinaryDecimal("decimals.greatest", "decimals_greatest_decimal_decimal", Decimal));
            decls.Add(BinaryDecimal("decimals.least", "decimals_least_decimal_decimal", Decimal));

            // Unary numeric: decimals.neg/abs/sign.
            decls.Add(UnaryDecimal("decimals.neg", "decimals_neg_decimal", Decimal));
            decls.Add(UnaryDecimal("decimals.abs", "decimals_abs_decimal", Decimal));
            decls.Add(UnaryDecimal("decimals.sign", "decimals_sign_decimal", Decls.Int));

            // (Decimal) -> string. Extends stdlib string(...); merges cleanly because the
            // opaque Decimal type isn't assignable to any existing string(...) overload.
            decls.Add(Decls.NewFunction(
                "string",
                Decls.NewOverload("decimal_to_string", new List<Type> { Decimal }, Decls.String)));

            // (Decimal) -> double. Extends stdlib double(...) for the same reason.
            decls.Add(Decls.NewFunction(
                "double",
                Decls.NewOverload("decimal_to_double", new List<Type> { Decimal }, Decls.Double)));

            // Rounding family: round/trunc accept 1 or 2 args; floor/ceil unary.
            decls.Add(Decls.NewFunction(
                "decimals.round",
                Decls.NewOverload("decimals_round_unary", new List<Type> { Decimal }, Decimal),
                Decls.NewOverload("decimals_round_scale",
                    new List<Type> { Decimal, Decls.Int }, Decimal)));
            decls.Add(Decls.NewFunction(
                "decimals.trunc",
                Decls.NewOverload("decimals_trunc_unary", new List<Type> { Decimal }, Decimal),
                Decls.NewOverload("decimals_trunc_scale",
                    new List<Type> { Decimal, Decls.Int }, Decimal)));
            decls.Add(UnaryDecimal("decimals.floor", "decimals_floor_decimal", Decimal));
            decls.Add(UnaryDecimal("decimals.ceil", "decimals_ceil_decimal", Decimal));
        }

        private static Decl BinaryDecimal(string name, string overloadId, Type result)
        {
            return Decls.NewFunction(name,
                Decls.NewOverload(overloadId, new List<Type> { Decimal, Decimal }, result));
        }

        private static Decl UnaryDecimal(string name, string overloadId, Type result)
        {
            return Decls.NewFunction(name,
                Decls.NewOverload(overloadId, new List<Type> { Decimal }, result));
        }

        // ---- Timestamp ----

        private static void AddTimestamp(IList<Decl> decls)
        {
            // One overload on the *standard* timestamp constructor, rather than a timestamp.of
            // namespace of our own: timestamp(int, int), an epoch value at a Flink-style decimal
            // precision (0 seconds, 3 millis, 6 micros, 9 nanos).
            //
            // Nothing is needed for the one-argument cases: cel.net's TypeAdapterSupport already
            // adapts Timestamp, Instant, ZonedDateTime and DateTime to a CEL timestamp, and the
            // standard unary `timestamp` overload converts a string or an int (epoch seconds), so
            // an Avro or Protobuf timestamp field needs no wrapper at all.
            decls.Add(Decls.NewFunction(
                "timestamp",
                Decls.NewOverload("timestamp_int_int",
                    new List<Type> { Decls.Int, Decls.Int }, Decls.Timestamp)));
        }
    }
}
