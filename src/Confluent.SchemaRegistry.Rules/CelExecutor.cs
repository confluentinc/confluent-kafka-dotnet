using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using Avro;
using Avro.Generic;
using Avro.Specific;
using Confluent.Shared.CollectionUtils;
using Cel.Checker;
using Cel.Common.Types.Avro;
using Cel.Common.Types.Json;
using Cel.Common.Types.Pb;
using Cel.Extension;
using Cel.Tools;
using Duration = Google.Protobuf.WellKnownTypes.Duration;
using Google.Api.Expr.V1Alpha1;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Newtonsoft.Json.Linq;
using NodaTime;

namespace Confluent.SchemaRegistry.Rules
{
    public class CelExecutor : IRuleExecutor
    {
        public static void Register()
        {
            RuleRegistry.RegisterRuleExecutor(new CelExecutor());
        }

        public static readonly string RuleType = "CEL";

        public static readonly Avro.Schema NullAvroSchema = AvroTypeDescription.NullAvroSchema;

        private readonly IDictionary<RuleWithArgs, Script> cache = new Dictionary<RuleWithArgs, Script>();
        private readonly SemaphoreSlim cacheMutex = new SemaphoreSlim(1);

        public CelExecutor()
        {
        }

        public void Configure(IEnumerable<KeyValuePair<string, string>> config,
            ISchemaRegistryClient client = null)
        {
        }

        public string Type() => RuleType;


        public async Task<object> Transform(RuleContext ctx, object message)
        {
            return await Execute(ctx, message, new Dictionary<string, object>() { { "message", message } })
                .ConfigureAwait(false);
        }

        public async Task<object> Execute(RuleContext ctx, object obj, IDictionary<string, object> args)
        {
            string expr = ctx.Rule.Expr;
            int index = expr.IndexOf(';');
            if (index >= 0)
            {
                string guard = expr.Substring(0, index);
                if (!string.IsNullOrEmpty(guard.Trim()))
                {
                    object guardResult = false;
                    try
                    {
                        guardResult = await Execute(ctx, guard, obj, args).ConfigureAwait(false);
                    }
                    catch (RuleException e)
                    {
                        // ignore
                    }

                    if (false.Equals(guardResult))
                    {
                        return ctx.Rule.Kind == RuleKind.Condition ? true : obj;
                    }
                }

                expr = expr.Substring(index + 1);
            }

            return await Execute(ctx, expr, obj, args).ConfigureAwait(false);
        }

        private async Task<object> Execute(RuleContext ctx, string rule, object obj, IDictionary<string, object> args)
        {
            try
            {
                if (!args.TryGetValue("message", out object msg))
                {
                    msg = obj;
                }

                ScriptType type = ScriptType.Json;
                if (msg is ISpecificRecord || msg is GenericRecord)
                {
                    type = ScriptType.Avro;
                }
                else if (msg is IMessage)
                {
                    type = ScriptType.Protobuf;
                }
                else if (!(msg is JObject)
                         && (typeof(IList).IsAssignableFrom(msg.GetType())
                             || (msg.GetType().IsGenericType
                                 && (msg.GetType().GetGenericTypeDefinition() == typeof(List<>)
                                     || msg.GetType().GetGenericTypeDefinition() == typeof(IList<>)))))
                {
                    // list not supported
                    return obj;
                }

                IDictionary<string, Google.Api.Expr.V1Alpha1.Type> declTypes = ToDeclTypes(args);
                RuleWithArgs ruleWithArgs = new RuleWithArgs(rule, type, declTypes, ctx.Target.SchemaString);
                Script script;
                await cacheMutex.WaitAsync().ConfigureAwait(continueOnCapturedContext: false);
                try
                {
                    if (!cache.TryGetValue(ruleWithArgs, out script))
                    {
                        script = BuildScript(ruleWithArgs, msg);
                        cache[ruleWithArgs] = script;
                    }
                }
                finally
                {
                    cacheMutex.Release();
                }

                return script.Execute<object>(args);
            }
            catch (ScriptException e)
            {
                throw new RuleException("Could not execute CEL script", e);
            }
        }

        internal Script BuildScript(RuleWithArgs ruleWithArgs, object msg)
        {
            // Build the script factory
            ScriptHost.Builder scriptHostBuilder = ScriptHost.NewBuilder();
            object type;
            switch (ruleWithArgs.ScriptType)
            {
                case ScriptType.Avro:
                    scriptHostBuilder = scriptHostBuilder.Registry(AvroRegistry.NewRegistry());
                    if (msg is ISpecificRecord)
                    {
                        type = ((ISpecificRecord)msg).Schema;
                        
                    }
                    else
                    {
                        type = ((GenericRecord)msg).Schema;
                        
                    }
                    break;
                case ScriptType.Json:
                    scriptHostBuilder = scriptHostBuilder.Registry(JsonRegistry.NewRegistry());
                    type = msg.GetType();
                    break;
                case ScriptType.Protobuf:
                    type = msg;
                    break;
                default:
                    throw new ArgumentException("Unsupported type " + ruleWithArgs.ScriptType);
            }

            ScriptHost scriptHost = scriptHostBuilder.Build();

            ScriptHost.ScriptBuilder scriptBuilder = scriptHost
                .BuildScript(ruleWithArgs.Rule)
                .WithDeclarations(ToDecls(ruleWithArgs.DeclTypes))
                .WithTypes(type);

            scriptBuilder = scriptBuilder.WithLibraries(new StringsLib(), new BuiltinLibrary());
            return scriptBuilder.Build();
        }

        private static IDictionary<string, Google.Api.Expr.V1Alpha1.Type> ToDeclTypes(IDictionary<string, object> args)
        {
            return args.ToDictionary(e => e.Key, e => FindType(e.Value));
        }

        private static List<Decl> ToDecls(IDictionary<string, Google.Api.Expr.V1Alpha1.Type> declTypes)
        {
            return declTypes
                .Select(e => Decls.NewVar(e.Key, e.Value))
                .ToList();
        }

        internal static Google.Api.Expr.V1Alpha1.Type FindType(Object arg)
        {
            if (arg == null)
            {
                return Checked.CheckedNull;
            }

            if (arg is ISpecificRecord)
            {
                return FindTypeForAvroType(((ISpecificRecord)arg).Schema);
            }

            if (arg is GenericRecord)
            {
                return FindTypeForAvroType(((GenericRecord)arg).Schema);
            }

            if (arg is IMessage)
            {
                return Decls.NewObjectType(((IMessage)arg).Descriptor.FullName);
            }

            return FindTypeForClass(arg.GetType());
        }

        private static Google.Api.Expr.V1Alpha1.Type FindTypeForAvroType(Avro.Schema schema)
        {
            Avro.Schema.Type type = schema.Tag;
            switch (type)
            {
                case Avro.Schema.Type.Boolean:
                    return Checked.CheckedBool;
                case Avro.Schema.Type.Int:
                case Avro.Schema.Type.Long:
                    return Checked.CheckedInt;
                case Avro.Schema.Type.Bytes:
                case Avro.Schema.Type.Fixed:
                    return Checked.CheckedBytes;
                case Avro.Schema.Type.Float:
                case Avro.Schema.Type.Double:
                    return Checked.CheckedDouble;
                case Avro.Schema.Type.String:
                    return Checked.CheckedString;
                // TODO duration, timestamp
                case Avro.Schema.Type.Array:
                    return Checked.CheckedListDyn;
                case Avro.Schema.Type.Map:
                    return Checked.CheckedMapStringDyn;
                case Avro.Schema.Type.Enumeration:
                    return Decls.NewObjectType(schema.Fullname);
                case Avro.Schema.Type.Null:
                    return Checked.CheckedNull;
                case Avro.Schema.Type.Record:
                    return Decls.NewObjectType(schema.Fullname);
                case Avro.Schema.Type.Union:
                    UnionSchema unionSchema = (UnionSchema)schema;
                    if (unionSchema.Schemas.Count == 2 && unionSchema.Schemas.Contains(NullAvroSchema))
                    {
                        foreach (Avro.Schema memberSchema in unionSchema.Schemas)
                        {
                            if (!memberSchema.Equals(NullAvroSchema))
                            {
                                return FindTypeForAvroType(memberSchema);
                            }
                        }
                    }

                    throw new ArgumentException("Unsupported union type");
                case Avro.Schema.Type.Logical:
                    return FindTypeForAvroType((schema as LogicalSchema).BaseSchema);
                default:
                    throw new ArgumentException("Unsupported type " + type);
            }
        }

        /// <summary>
        ///     Presents a value the way its declared type implies. A protobuf enum arrives as
        ///     the generated CLR enum, which CEL has no type for; its number is what
        ///     <see cref="FindTypeForClass" /> declares and so what has to be bound. A
        ///     repeated or map field of enums needs the same for its elements, which is why
        ///     this descends.
        ///     <para>
        ///         A collection is rebuilt only if something inside it actually changed, so a
        ///         byte[] - which is an IList of bytes - and a list of messages are handed
        ///         back exactly as they came in.
        ///     </para>
        /// </summary>
        internal static object ToCelValue(object value)
        {
            if (value is System.Enum)
            {
                return Convert.ToInt64(value, CultureInfo.InvariantCulture);
            }

            // A protobuf repeated or map field is homogeneous, so a collection of enums is
            // all enums. Converting it to a typed collection keeps the declared element type
            // an int; rebuilding it as object would type it dyn and the comparison would not
            // resolve. Anything else - a byte[], a list of messages - is left alone.
            if (value is IDictionary dictionary)
            {
                var converted = new Dictionary<object, long>(dictionary.Count);
                foreach (DictionaryEntry entry in dictionary)
                {
                    if (!(entry.Value is System.Enum))
                    {
                        return value;
                    }

                    converted[entry.Key] = Convert.ToInt64(entry.Value, CultureInfo.InvariantCulture);
                }

                return converted.Count > 0 ? converted : value;
            }

            if (value is IList list && !(value is byte[]))
            {
                var converted = new List<long>(list.Count);
                foreach (object element in list)
                {
                    if (!(element is System.Enum))
                    {
                        return value;
                    }

                    converted.Add(Convert.ToInt64(element, CultureInfo.InvariantCulture));
                }

                return converted.Count > 0 ? (object)converted : value;
            }

            return value;
        }

        private static Google.Api.Expr.V1Alpha1.Type FindTypeForClass(System.Type type)
        {
            var underlyingType = Nullable.GetUnderlyingType(type);
            if (underlyingType != null) type = underlyingType;

            if (type == typeof(bool)) return Checked.CheckedBool;

            // A protobuf enum is compared by its number, as in the Java, Go and C++
            // clients: a rule reads `this == 1`, not the generated symbol. Without this the
            // generated enum type matches nothing below and the rule fails to compile, so a
            // rule on an enum field rejected every message.
            if (type.IsEnum) return Checked.CheckedInt;

            if (type == typeof(long) || type == typeof(int) ||
                type == typeof(short) || type == typeof(sbyte) ||
                type == typeof(byte))
                return Checked.CheckedInt;

            if (type == typeof(uint) || type == typeof(ulong)) return Checked.CheckedUint;

            if (type == typeof(byte[]) || type == typeof(ByteString)) return Checked.CheckedBytes;

            if (type == typeof(double) || type == typeof(float)) return Checked.CheckedDouble;

            if (type == typeof(string)) return Checked.CheckedString;

            if (type == typeof(Duration) || type == typeof(Period)) return Checked.CheckedDuration;

            if (type == typeof(Timestamp) || type == typeof(Instant) ||
                type == typeof(ZonedDateTime))
                return Checked.CheckedTimestamp;

            if (type.IsGenericType &&
                (type.GetGenericTypeDefinition() == typeof(Dictionary<,>) ||
                 type.GetGenericTypeDefinition() == typeof(IDictionary<,>)))
            {
                var arguments = type.GetGenericArguments();
                var keyType = FindTypeForClass(arguments[0]);
                var valueType = FindTypeForClass(arguments[1]);
                return Decls.NewMapType(keyType, valueType);
            }

            if (typeof(IDictionary).IsAssignableFrom(type))
            {
                // Protobuf's MapField<K,V> implements the non-generic IDictionary without
                // being a Dictionary<,>, so take the key and value types from whichever
                // generic dictionary interface it closes over. Falling back to object
                // would leave the map unusable: an object-keyed map cannot be indexed.
                var mapArguments = ClosedGenericArguments(type, typeof(IDictionary<,>));
                if (mapArguments != null)
                {
                    return Decls.NewMapType(FindElementTypeForClass(mapArguments[0]),
                        FindElementTypeForClass(mapArguments[1]));
                }

                return Decls.NewMapType(Checked.CheckedDyn, Checked.CheckedDyn);
            }

            if (type.IsGenericType &&
                (type.GetGenericTypeDefinition() == typeof(List<>) || type.GetGenericTypeDefinition() == typeof(IList<>)))
            {
                var arguments = type.GetGenericArguments();
                var valueType = FindTypeForClass(arguments[0]);
                return Decls.NewListType(valueType);
            }

            if (typeof(IList).IsAssignableFrom(type))
            {
                // As above for protobuf's RepeatedField<T>.
                var listArguments = ClosedGenericArguments(type, typeof(IList<>));
                if (listArguments != null)
                {
                    return Decls.NewListType(FindElementTypeForClass(listArguments[0]));
                }

                return Checked.CheckedListDyn;
            }

            return Decls.NewObjectType(type.FullName);
        }

        /// <summary>
        ///     The type of an element inside a list or a map. A protobuf message or Avro
        ///     record element stays dynamic: its CLR type name is not the schema type name
        ///     the checker would need, and the registry resolves its fields at evaluation
        ///     time anyway.
        /// </summary>
        private static Google.Api.Expr.V1Alpha1.Type FindElementTypeForClass(System.Type type)
        {
            if (typeof(IMessage).IsAssignableFrom(type) ||
                typeof(ISpecificRecord).IsAssignableFrom(type) ||
                typeof(GenericRecord).IsAssignableFrom(type) ||
                type == typeof(object))
            {
                return Checked.CheckedDyn;
            }

            return FindTypeForClass(type);
        }

        /// <summary>
        ///     The type arguments with which <paramref name="type" /> closes over
        ///     <paramref name="openGeneric" />, or null if it does not implement it.
        /// </summary>
        private static System.Type[] ClosedGenericArguments(System.Type type, System.Type openGeneric)
        {
            foreach (System.Type candidate in type.GetInterfaces())
            {
                if (candidate.IsGenericType && candidate.GetGenericTypeDefinition() == openGeneric)
                {
                    return candidate.GetGenericArguments();
                }
            }

            return null;
        }

        public void Dispose()
        {
            cacheMutex.Dispose();
            cache.Clear();
        }

        internal enum ScriptType
        {
            Avro,
            Json,
            Protobuf
        }

        internal class RuleWithArgs : IEquatable<RuleWithArgs>
        {
            public string Rule { get; }
            public ScriptType ScriptType { get; }
            public IDictionary<string, Google.Api.Expr.V1Alpha1.Type> DeclTypes { get; }
            public string Schema { get; }

            public RuleWithArgs(string rule, ScriptType scriptType,
                IDictionary<string, Google.Api.Expr.V1Alpha1.Type> declTypes, string schema)
            {
                Rule = rule;
                ScriptType = scriptType;
                DeclTypes = declTypes;
                Schema = schema;
            }

            /// <inheritdoc />
            public bool Equals(RuleWithArgs other)
            {
                return RuleWithArgsEqualityComparer.Instance.Equals(this, other);
            }

            /// <inheritdoc />
            public override bool Equals(object obj)
            {
                return Equals(obj as RuleWithArgs);
            }

            /// <inheritdoc />
            public override int GetHashCode()
            {
                return RuleWithArgsEqualityComparer.Instance.GetHashCode(this);
            }

            private class RuleWithArgsEqualityComparer : IEqualityComparer<RuleWithArgs>
            {
                private readonly DictionaryEqualityComparer<string, Google.Api.Expr.V1Alpha1.Type> declTypesEqualityComparer = new();

                private RuleWithArgsEqualityComparer()
                {
                }

                public static RuleWithArgsEqualityComparer Instance { get; } = new();

                public bool Equals(RuleWithArgs x, RuleWithArgs y)
                {
                    if (ReferenceEquals(x, y)) return true;
                    if (x is null) return false;
                    if (y is null) return false;
                    if (x.GetType() != y.GetType()) return false;
                    if (x.Rule != y.Rule) return false;
                    if (x.ScriptType != y.ScriptType) return false;
                    if (!declTypesEqualityComparer.Equals(x.DeclTypes, y.DeclTypes)) return false;
                    if (x.Schema != y.Schema) return false;
                    return true;
                }

                public int GetHashCode(RuleWithArgs obj)
                {
                    var hashCode = new HashCode();
                    hashCode.Add(obj.Rule);
                    hashCode.Add((int) obj.ScriptType);
                    hashCode.Add(obj.DeclTypes, declTypesEqualityComparer);
                    hashCode.Add(obj.Schema);
                    return hashCode.ToHashCode();
                }
            }
        }
    }
}