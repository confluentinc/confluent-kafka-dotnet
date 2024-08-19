using Cel.Checker;
using Google.Api.Expr.V1Alpha1;
using Type = Google.Api.Expr.V1Alpha1.Type;

namespace Confluent.SchemaRegistry.Rules
{
    public class BuiltinDeclarations
    {
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

            return decls;
        }
    }
}
