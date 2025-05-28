using System.Collections;
using Avro;
using Avro.Generic;
using Avro.Specific;
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
                else if (typeof(IList).IsAssignableFrom(msg.GetType()) 
                         || (msg.GetType().IsGenericType 
                             && (msg.GetType().GetGenericTypeDefinition() == typeof(List<>) 
                                 || msg.GetType().GetGenericTypeDefinition() == typeof(IList<>))))
                {
                    // list not supported
                    return obj;
                }

                IDictionary<string, Decl> decls = ToDecls(args);
                RuleWithArgs ruleWithArgs = new RuleWithArgs(rule, type, decls, ctx.Target.SchemaString);
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

        private Script BuildScript(RuleWithArgs ruleWithArgs, object msg)
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
                .WithDeclarations(new List<Decl>(ruleWithArgs.Decls.Values))
                .WithTypes(type);

            scriptBuilder = scriptBuilder.WithLibraries(new StringsLib(), new BuiltinLibrary());
            return scriptBuilder.Build();
        }

        private static IDictionary<String, Decl> ToDecls(IDictionary<String, Object> args)
        {
            return args
                .Select(e => Decls.NewVar(e.Key, FindType(e.Value)))
                .ToDictionary(e => e.Name, e => e);
        }

        private static Google.Api.Expr.V1Alpha1.Type FindType(Object arg)
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

        private static Google.Api.Expr.V1Alpha1.Type FindTypeForClass(System.Type type)
        {
            var underlyingType = Nullable.GetUnderlyingType(type);
            if (underlyingType != null) type = underlyingType;

            if (type == typeof(bool)) return Checked.CheckedBool;

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
                var objType = FindTypeForClass(typeof(object));
                return Decls.NewMapType(objType, objType);
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
                var objType = FindTypeForClass(typeof(object));
                return Decls.NewListType(objType);
            }
            
            return Decls.NewObjectType(type.FullName);
        }

        public void Dispose()
        {
        }

        public enum ScriptType
        {
            Avro,
            Json,
            Protobuf
        }

        public record RuleWithArgs(string Rule, ScriptType ScriptType, IDictionary<string, Decl> Decls, string Schema);
    }
}