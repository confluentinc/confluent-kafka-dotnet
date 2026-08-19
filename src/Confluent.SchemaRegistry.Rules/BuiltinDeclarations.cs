using Cel.Checker;
using Google.Api.Expr.V1Alpha1;
using Type = Google.Api.Expr.V1Alpha1.Type;

namespace Confluent.SchemaRegistry.Rules
{
    public class BuiltinDeclarations
    {
        // Abstract (opaque) type declaration for confluent.type.Decimal. The label is the
        // cross-language spec; cel.net has no opaque type, so the runtime value is DecimalT,
        // whose type name matches this. Timestamp uses CEL's built-in timestamp type.
        private static readonly Type Decimal =
            Decls.NewAbstractType(CelTypeLabels.DecimalName, new List<Type>());

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

            return decls;
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
            // timestamp.of — (dyn) runtime-dispatch + explicit (int, string) epoch + unit.
            decls.Add(Decls.NewFunction(
                "timestamp.of",
                Decls.NewOverload("timestamp_of_dyn", new List<Type> { Decls.Dyn }, Decls.Timestamp),
                Decls.NewOverload("timestamp_of_int_string",
                    new List<Type> { Decls.Int, Decls.String }, Decls.Timestamp)));
        }
    }
}
