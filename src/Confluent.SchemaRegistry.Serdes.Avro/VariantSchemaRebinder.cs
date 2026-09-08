// Copyright 2026 Confluent Inc.
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

using Newtonsoft.Json.Linq;

namespace Confluent.SchemaRegistry.Serdes
{
    /// <summary>
    ///     Re-applies the <c>variant</c> logical type at every **by-name reference** to
    ///     <c>confluent.type.Variant</c>, before the schema text is parsed.
    ///
    ///     <para>
    ///         Apache.Avro applies a logical type only where the schema *defines* the record.
    ///         <c>SchemaNames.Add</c> is typed <c>(SchemaName, NamedSchema)</c> and
    ///         <c>LogicalSchema : UnnamedSchema</c>, so the wrapper cannot be stored in the names
    ///         map and every later reference resolves to the bare record. The result was that a
    ///         schema could carry a variant at exactly **one** site: a second field referencing
    ///         it by name got a plain <c>RecordSchema</c>, and the writer then refused the
    ///         <see cref="Variant" /> with "GenericRecord required to write against record
    ///         schema".
    ///     </para>
    ///     <para>
    ///         Not fixable on the parsed tree - <c>LogicalSchema</c>'s constructor is private and
    ///         <c>Field.Schema</c> has a private setter - but it is fixable *before* the parse.
    ///         A reference written in object form, <c>{"type": "confluent.type.Variant",
    ///         "logicalType": "variant"}</c>, does get the logical type, because
    ///         <c>LogicalSchema.NewInstance</c> passes any type string that is not one of
    ///         record/enum/array/map/fixed straight to <c>Schema.ParseJson</c>, which resolves it
    ///         through the names map and wraps the result. So the reference is rewritten into
    ///         that form and the existing logical-type machinery does the rest - read, write and
    ///         rules alike, with no per-message data walk.
    ///     </para>
    ///     <para>
    ///         <b>This changes nothing on the wire and nothing that is registered.</b> It rewrites
    ///         the schema text the client parses *locally*; a <c>LogicalSchema</c> encodes as its
    ///         base schema, so the bytes are identical, and the schema registered for the subject
    ///         is whatever the caller wrote. Other clients recognise the variant by record name
    ///         and never look at the annotation.
    ///     </para>
    /// </summary>
    internal static class VariantSchemaRebinder
    {
        /// <summary>The record name that is the cross-client contract for a variant.</summary>
        internal const string VariantFullName = "confluent.type.Variant";

        /// <summary>The last segment of <see cref="VariantFullName" />, for the cheap pre-filter.</summary>
        private static readonly string VariantSimpleName =
            VariantFullName.Substring(VariantFullName.LastIndexOf('.') + 1);

        /// <summary>
        ///     Returns <paramref name="schemaString" /> with every by-name variant reference in
        ///     object form, or the original string when there is nothing to rewrite - which is
        ///     the overwhelmingly common case, and why the parse is left untouched then.
        /// </summary>
        public static string Rebind(string schemaString)
        {
            if (schemaString == null
                || schemaString.IndexOf(VariantSimpleName, System.StringComparison.Ordinal) < 0)
            {
                // The contract name cannot appear, so neither can a reference to it. Filtering on
                // the *name* rather than on "logicalType":"variant" deliberately: the definition
                // may live in a referenced schema, in which case this text carries only the
                // reference.
                return schemaString;
            }

            JToken root;
            try
            {
                root = JToken.Parse(schemaString);
            }
            catch (Newtonsoft.Json.JsonException)
            {
                // Not our business to report a malformed schema; the parser below will.
                return schemaString;
            }

            bool rewrote = false;
            Visit(root, null, ref rewrote);
            return rewrote ? root.ToString(Newtonsoft.Json.Formatting.None) : schemaString;
        }

        /// <summary>
        ///     Walks every schema node, rewriting the type positions and carrying the enclosing
        ///     namespace down, because a reference may be written as a short name.
        /// </summary>
        private static void Visit(JToken node, string enclosingNamespace, ref bool rewrote)
        {
            if (node is JArray union)
            {
                // A union: every branch is a type position.
                for (int i = 0; i < union.Count; i++)
                {
                    JToken replacement = RewriteIfVariantReference(union[i], enclosingNamespace);
                    if (replacement != null)
                    {
                        union[i] = replacement;
                        rewrote = true;
                    }
                    else
                    {
                        Visit(union[i], enclosingNamespace, ref rewrote);
                    }
                }

                return;
            }

            if (!(node is JObject obj))
            {
                return;
            }

            // A named type establishes the namespace its children resolve against.
            string scope = NamespaceOf(obj, enclosingNamespace);

            // An object that already carries the variant logical type is the exact form this
            // rebinder produces. Rewriting its `type` again would nest one inside the other -
            // {"type":{"type":...},"logicalType":"variant"} - corrupting a schema a user wrote
            // correctly by hand, and making Rebind non-idempotent.
            bool alreadyVariant =
                obj.Property("logicalType")?.Value?.Type == JTokenType.String
                && (string)obj.Property("logicalType").Value
                    == VariantLogicalType.LogicalTypeName;

            foreach (string position in new[] { "type", "items", "values" })
            {
                JProperty property = obj.Property(position);
                if (property == null)
                {
                    continue;
                }

                JToken replacement = position == "type" && alreadyVariant
                    ? null
                    : RewriteIfVariantReference(property.Value, scope);
                if (replacement != null)
                {
                    property.Value = replacement;
                    rewrote = true;
                }
                else
                {
                    Visit(property.Value, scope, ref rewrote);
                }
            }

            // A record's fields are schema nodes in their own right; nothing else nested in a
            // schema object is one, so the walk deliberately does not descend anywhere else.
            if (obj.Property("fields")?.Value is JArray fields)
            {
                foreach (JToken field in fields)
                {
                    Visit(field, scope, ref rewrote);
                }
            }
        }

        /// <summary>
        ///     Returns the object-form replacement for a bare by-name variant reference, or null
        ///     when <paramref name="token" /> is not one.
        /// </summary>
        private static JToken RewriteIfVariantReference(JToken token, string enclosingNamespace)
        {
            if (token.Type != JTokenType.String)
            {
                return null;
            }

            string name = (string)token;
            if (Resolve(name, enclosingNamespace) != VariantFullName)
            {
                return null;
            }

            return new JObject
            {
                ["type"] = name,
                ["logicalType"] = VariantLogicalType.LogicalTypeName,
            };
        }

        /// <summary>
        ///     Avro's name resolution: a name containing a dot is already a fullname, and a bare
        ///     name resolves against the enclosing namespace. Getting this wrong in the lenient
        ///     direction would rewrite an unrelated type that merely happens to be called
        ///     <c>Variant</c>.
        /// </summary>
        private static string Resolve(string name, string enclosingNamespace)
        {
            if (name.IndexOf('.') >= 0 || string.IsNullOrEmpty(enclosingNamespace))
            {
                return name;
            }

            return enclosingNamespace + "." + name;
        }

        /// <summary>
        ///     The namespace a named type's children resolve against: the one it declares, or the
        ///     one implied by a dotted name, or the namespace inherited from its parent.
        /// </summary>
        private static string NamespaceOf(JObject obj, string enclosingNamespace)
        {
            if (obj.Property("name")?.Value is JValue nameValue
                && nameValue.Type == JTokenType.String)
            {
                string name = (string)nameValue;
                int lastDot = name.LastIndexOf('.');
                if (lastDot > 0)
                {
                    return name.Substring(0, lastDot);
                }
            }

            if (obj.Property("namespace")?.Value is JValue ns && ns.Type == JTokenType.String)
            {
                return (string)ns;
            }

            return enclosingNamespace;
        }
    }
}
