using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using Avro;
using Avro.Generic;
using Avro.Specific;
using Confluent.Shared.CollectionUtils;
using Cel.Checker;
using Cel.Common.Types;
using Cel.Common.Types.Avro;
using Cel.Common.Types.Json;
using Cel.Common.Types.Pb;
using Cel.Extension;
using Cel.Tools;
using IVal = Cel.Common.Types.Ref.IVal;
using Duration = Google.Protobuf.WellKnownTypes.Duration;
using Google.Api.Expr.V1Alpha1;
using Google.Protobuf;
using Google.Protobuf.Reflection;
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
            object result = await Execute(ctx, message, new Dictionary<string, object>() { { "message", message } })
                .ConfigureAwait(false);
            return ShapeResult(ctx, message, result);
        }

        /// <summary>
        ///     Converts a message-level transform's result back to the form the serializer writes.
        ///     A rule that rebuilds a message returns a plain map, which neither the protobuf nor
        ///     the Avro writer can take; see <see cref="ProtobufResultWriter" /> and
        ///     <see cref="AvroResultWriter" /> for the replace semantics that implies.
        ///
        ///     Dispatches on the value rather than on <c>ctx.Target.SchemaType</c>, matching how
        ///     <see cref="Execute(RuleContext, string, object, IDictionary{string, object})" />
        ///     already picks its <c>ScriptType</c> a few lines below.
        ///
        ///     Only <see cref="Transform" /> calls this, and <c>CelFieldExecutor</c> goes through
        ///     <see cref="Execute(RuleContext, object, IDictionary{string, object})" /> instead - so
        ///     a <c>CEL_FIELD</c> result never reaches here and cannot be encoded twice. Several
        ///     clients had to add a guard for exactly that; here the two entry points already
        ///     separate them.
        /// </summary>
        private static object ShapeResult(RuleContext ctx, object original, object result)
        {
            if (ctx.Rule.Kind == RuleKind.Condition)
            {
                // A bool is a pass/fail signal to the framework, not data to coerce.
                return result;
            }

            switch (original)
            {
                case IMessage _:
                    return ProtobufResultWriter.Convert(original, result);
                case GenericRecord _:
                    return AvroResultWriter.Convert(original, result);
                default:
                    // JSON Schema, and Avro's ISpecificRecord: unchanged, as before this moved.
                    return result;
            }
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

        /// <param name="schemaHint">
        ///     The walker's schema, when there is one. Only the Avro arm uses it, to find the
        ///     record whose fields the checker must resolve — a field rule binds a bare value
        ///     that carries no schema of its own.
        /// </param>
        internal Script BuildScript(RuleWithArgs ruleWithArgs, object msg, object schemaHint = null)
        {
            // Build the script factory
            ScriptHost.Builder scriptHostBuilder = ScriptHost.NewBuilder();
            object[] types;
            switch (ruleWithArgs.ScriptType)
            {
                case ScriptType.Avro:
                    var avroRegistry = AvroRegistry.NewRegistry(AvroValueToCel);
                    // The decimal type by name, so a rule can say it. The name is this client's,
                    // not any Avro schema's, so the registry has to be told; ProtoTypeRegistry
                    // resolves it from the descriptor pool for free. Needs Cel.NET >= 2.3.1,
                    // where AvroRegistry.RegisterType stopped throwing.
                    avroRegistry.RegisterType(TypeT.NewObjectTypeValue(CelTypeLabels.DecimalName));
                    scriptHostBuilder = scriptHostBuilder.Registry(avroRegistry);
                    // Only a record has fields to resolve: the walker's hint when it names one,
                    // else the value's own schema, else nothing. A bare field value used to be
                    // cast to GenericRecord here and throw.
                    RecordSchema avroRecord = schemaHint as RecordSchema
                        ?? (msg as ISpecificRecord)?.Schema as RecordSchema
                        ?? (msg as GenericRecord)?.Schema;
                    types = avroRecord != null
                        ? new object[] { avroRecord }
                        : new object[0];
                    break;
                case ScriptType.Json:
                    scriptHostBuilder = scriptHostBuilder.Registry(JsonRegistry.NewRegistry());
                    // JsonRegistry keys its type descriptions by CLR type, so the value is the
                    // right source here, unlike the Avro arm above.
                    types = new object[] { msg.GetType() };
                    break;
                case ScriptType.Protobuf:
                    // A registry carrying ProtoValueToCel, so a confluent.type.Decimal is a
                    // DecimalT wherever it appears - including a field reached by selection, which
                    // no boundary conversion can see. Without it cel.net answers `==` with
                    // lhs.Equal(rhs) on the raw message, comparing unscaled bytes and scale field
                    // by field, and `this.subtotal == this.total` was false for 1.50 against 1.5.
                    //
                    // Deliberately the registry hook rather than ScriptHost.Adapter /
                    // EnvOptions.CustomTypeAdapter: an environment-level adapter is consulted for
                    // the bound value only. cel.net's attribute layer adapts the *container* first
                    // and then reads fields off the resulting PbObjectT using the registry that
                    // object was built with, so an environment adapter never sees a nested field.
                    // See ProtoTypeRegistry.NewRegistry(customAdapter) for the full comparison
                    // with cel-go, which adapts field values natively and so does not have this
                    // limitation.
                    scriptHostBuilder = scriptHostBuilder.Registry(
                        ProtoTypeRegistry.NewRegistry(ProtoValueToCel));
                    // As on the Avro arm, only a message has fields to resolve. A rule on a
                    // scalar field binds a primitive, which the registry cannot register and does
                    // not need to. The reference registers the field's containing type instead;
                    // nothing is equivalent here, and avoids a descriptor the registry may not
                    // unify with the runtime one.
                    types = msg is IMessage
                        ? new object[] { msg }
                        : new object[0];
                    break;
                default:
                    throw new ArgumentException("Unsupported type " + ruleWithArgs.ScriptType);
            }

            ScriptHost scriptHost = scriptHostBuilder.Build();

            ScriptHost.ScriptBuilder scriptBuilder = scriptHost
                .BuildScript(ruleWithArgs.Rule)
                .WithDeclarations(ToDecls(ruleWithArgs.DeclTypes))
                .WithTypes(types);

            scriptBuilder = scriptBuilder.WithLibraries(new StringsLib(), new MathLib(), new BuiltinLibrary());
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

            if (arg is NullValue)
            {
                // An absent value is bound as NullValue.NullValue rather than a CLR null,
                // because Cel.NET needs a non-null binding. NullValue is a protobuf *enum*,
                // so without this arm FindTypeForClass declared it as `int` and a guard like
                // `value == null` failed at check time with "no matching overload for '_==_'
                // applied to '(int, null)'" - before the rule ever ran.
                return Checked.CheckedNull;
            }

            if (arg is DecimalT || arg is AvroDecimal)
            {
                // Matches DecimalT.Type() and the declaration in BuiltinDeclarations, so a
                // converted decimal and decimal(...) are one type. An AvroDecimal is declared
                // the same way rather than converted: only the declaration was ever missing,
                // since AvroValueToCel adapts the value and DecimalUtils.ToBigDecimal coerces
                // it. The reference splits it the same way - declare by name, coerce in
                // DecimalUtils.
                return Decls.NewObjectType(CelTypeLabels.DecimalName);
            }

            if (arg is IMessage)
            {
                return Decls.NewObjectType(((IMessage)arg).Descriptor.FullName);
            }

            return FindTypeForClass(arg.GetType());
        }

        /// <summary>
        ///     Presents an Avro value the way this client's CEL surface expects, for the shapes
        ///     whose logical representation differs from cel.net's default mapping. A
        ///     <c>decimal</c> logical type decodes to an <see cref="AvroDecimal" />, which cel.net
        ///     would otherwise carry as its own <c>avro.decimal</c> value — a different CEL type
        ///     from the <see cref="DecimalT" /> that <c>decimal(...)</c> produces, so
        ///     <c>decimals.*</c> would not accept it and <c>==</c> against a decimal literal
        ///     would answer false. Carrying it as a DecimalT makes a decimal field usable with no
        ///     <c>decimal(...)</c> call, and keeps equality numeric. Returning null leaves the
        ///     value to cel.net's standard mapping.
        /// </summary>
        private static IVal AvroValueToCel(object value)
        {
            if (value is AvroDecimal dec)
            {
                return DecimalT.Of(DecimalUtils.ToBigDecimal(dec.UnscaledValue, dec.Scale));
            }

            if (value is Variant variant)
            {
                // An Avro confluent.type.Variant record surfaces as a Variant (see
                // VariantLogicalType), which CEL knows nothing about. Carried opaquely it
                // reaches the variants.* functions unchanged - VariantUtils.ToVariant takes a
                // Variant directly. Without this arm it fell through to the registry's object
                // fallback, which needs an Avro schema for the CLR type and threw
                // "Cannot get schema for Confluent.SchemaRegistry.Variant", so every rule that
                // read a variant field failed.
                return OpaqueT.Of(variant, CelTypeLabels.VariantName);
            }

            return null;
        }

        /// <summary>
        ///     A <c>confluent.type.Decimal</c> message as a <see cref="DecimalT" />, or null for
        ///     anything else. The protobuf counterpart of <c>AvroValueToCel</c>'s AvroDecimal arm,
        ///     and needed for the same reason: cel.net intercepts <c>==</c> in the planner and
        ///     answers it with <c>lhs.Equal(rhs)</c>, so a decimal left as a protobuf message
        ///     compares structurally - field by field over unscaled bytes and scale - and calls
        ///     12.34 and 12.340 unequal even though they are the same number. Carried as a
        ///     DecimalT it compares numerically, and <c>string()</c> / <c>double()</c> resolve.
        ///     <para>
        ///         Only reaches a value bound directly. A decimal reached by selection instead
        ///         (<c>this.amount</c>) is resolved inside cel.net, past any boundary.
        ///     </para>
        /// </summary>
        internal static object ToCelDecimalOrNull(object value)
        {
            if (value is DecimalT)
            {
                return value;
            }

            if (value is IMessage msg
                && msg.Descriptor?.FullName == CelTypeLabels.DecimalName)
            {
                return DecimalT.Of(DecimalUtils.ToBigDecimal(msg));
            }

            // The Avro counterpart. A message-level rule reaches a decimal field through the Avro
            // registry adapter, which converts it (see AvroValueToCel); a *field*-level rule binds
            // the value straight to `this` and misses that adapter, so without this arm the checker
            // was told the type was Avro.AvroDecimal and no decimals.* overload matched - the rule
            // failed before it ran.
            if (value is AvroDecimal avroDecimal)
            {
                return DecimalT.Of(
                    DecimalUtils.ToBigDecimal(avroDecimal.UnscaledValue, avroDecimal.Scale));
            }

            return null;
        }

        /// <summary>
        ///     Presents a protobuf value the way this client's CEL surface expects. The protobuf
        ///     counterpart of <see cref="AvroValueToCel" />: a <c>confluent.type.Decimal</c>
        ///     message is carried as a <see cref="DecimalT" /> so it compares numerically rather
        ///     than by its encoding. Returning null leaves the value to cel.net's standard
        ///     mapping. Reaches message fields as well as top-level values, because the registry
        ///     adapts its fields through this same hook.
        /// </summary>
        private static IVal ProtoValueToCel(object value)
        {
            return ToCelDecimalOrNull(value) as IVal;
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
                    // A logical-typed value is the logical representation, not the underlying
                    // primitive: timestamp-* decodes to DateTime, decimal to AvroDecimal, uuid
                    // to Guid. Declaring the base type (Int for a timestamp-millis long, say)
                    // would be a check/runtime mismatch, so defer to the runtime value —
                    // matching the JVM client's findCelTypeForAvroSchema. Field types inside a
                    // record come from cel.net's AvroTypeDescription, which does the same;
                    // this path covers a schema handed in for a value directly.
                    return Checked.CheckedDyn;
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

            // An Avro timestamp logical type decodes to a DateTime, which CEL knows nothing about:
            // FindTypeForClass maps Timestamp, Instant and ZonedDateTime to a CEL timestamp and a
            // DateTime to none of them, so `this > timestamp(...)` was rejected at *check* time
            // with "found no matching overload for '_>_' applied to '(System.DateTime, timestamp)'".
            // The runtime value was always right; only the declaration was wrong.
            //
            // Converted here rather than in AvroValueToCel because the declared type is derived
            // from the value as it is bound, and the registry adapter runs after that - too late to
            // affect the check. This is the one conversion both the inline and the CEL_FIELD path
            // need, and both go through here.
            if (value is DateTime dateTime)
            {
                // Avro decodes a timestamp logical type as UTC; an unspecified kind is treated as
                // UTC rather than local, which is Avro's own reading and avoids a silent shift.
                DateTime utc =
                    dateTime.Kind == DateTimeKind.Utc ? dateTime
                    : dateTime.Kind == DateTimeKind.Local ? dateTime.ToUniversalTime()
                    : DateTime.SpecifyKind(dateTime, DateTimeKind.Utc);
                return Instant.FromDateTimeUtc(utc);
            }

            // No decimal arm here, deliberately. `decimals.add(value, ...)` on an Avro decimal
            // failed the check because the declared type was the CLR AvroDecimal - a declaration
            // problem, fixed in FindType above rather than by converting the value. Converting
            // it here also erased what the registry choice reads (see CelValidator).

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

        /// <summary>
        ///     The inverse of <see cref="ToCelValue" />'s DateTime arm: whatever CEL hands back for
        ///     a timestamp, as the <c>DateTime</c> Avro's writer encodes from. Returns null when the
        ///     value is not a timestamp at all, so the caller can leave it alone.
        ///
        ///     Needed because the read side has to convert: an Avro timestamp field arrives as a
        ///     DateTime and must be declared to the checker as a CEL timestamp, so a rule that
        ///     returns one - <c>value + duration('60s')</c> - returns CEL's shape, not Avro's, and
        ///     the writer fails with "Unable to cast Timestamp to System.DateTime".
        /// </summary>
        internal static object ToAvroDateTimeOrNull(object value)
        {
            switch (value)
            {
                case DateTime dateTime:
                    return dateTime;
                case Instant instant:
                    return instant.ToDateTimeUtc();
                case ZonedDateTime zoned:
                    return zoned.ToInstant().ToDateTimeUtc();
                case DateTimeOffset offset:
                    return offset.UtcDateTime;
                case Google.Protobuf.WellKnownTypes.Timestamp timestamp:
                    return timestamp.ToDateTime();
                default:
                    return null;
            }
        }

        /// <summary>
        ///     The inverse of the AvroDecimal arm of <see cref="ToCelDecimalOrNull" />: an Avro
        ///     decimal field is presented to CEL as a decimal, so whatever the rule hands back has
        ///     to become an <c>AvroDecimal</c> again before Avro's writer sees it. Returns null
        ///     when the value is not a decimal in any recognised form, so the caller can leave it
        ///     alone.
        ///
        ///     Symmetric with <see cref="ToAvroDateTimeOrNull" />, and needed for the same reason:
        ///     without it a rule computing a decimal for an Avro decimal field failed with
        ///     "Unable to cast object of type 'BigDecimal' to type 'Avro.AvroDecimal'" - on a
        ///     repeated field, where the rule is applied per element, and on a scalar one alike.
        /// </summary>
        internal static object ToAvroDecimalOrNull(object value)
        {
            switch (value)
            {
                case AvroDecimal avroDecimal:
                    return avroDecimal;
                case BigDecimal bigDecimal:
                    return new AvroDecimal(bigDecimal.Unscaled, bigDecimal.Scale);
                default:
                    return null;
            }
        }

        /// <summary>
        ///     Presents a field's value the way its declared type implies, so that the value
        ///     and the type <see cref="FindTypeForField" /> declares always agree. Without
        ///     this the two could disagree - a uint64 field declared uint while its value is
        ///     bound as an int - and the rule would fail at evaluation instead of answering.
        /// </summary>
        internal static object ToCelValueForField(FieldDescriptor field, object value)
        {
            if (value == null || field.IsMap)
            {
                return ToCelValue(value);
            }

            if (field.IsRepeated)
            {
                if (!(value is IList list))
                {
                    return ToCelValue(value);
                }

                var converted = new List<object>(list.Count);
                foreach (object element in list)
                {
                    converted.Add(ToCelScalar(field.FieldType, element));
                }

                return converted;
            }

            return ToCelScalar(field.FieldType, value);
        }

        private static object ToCelScalar(FieldType fieldType, object value)
        {
            switch (fieldType)
            {
                case FieldType.Float:
                case FieldType.Double:
                    return value is double ? value : Convert.ToDouble(value, CultureInfo.InvariantCulture);
                case FieldType.Int32:
                case FieldType.Int64:
                case FieldType.SInt32:
                case FieldType.SInt64:
                case FieldType.SFixed32:
                case FieldType.SFixed64:
                case FieldType.Enum:
                    return value is long ? value : Convert.ToInt64(value, CultureInfo.InvariantCulture);
                case FieldType.UInt32:
                case FieldType.UInt64:
                case FieldType.Fixed32:
                case FieldType.Fixed64:
                    return ToUnsigned(value);
                case FieldType.Bool:
                    return value is bool ? value : Convert.ToBoolean(value, CultureInfo.InvariantCulture);
                default:
                    // string, bytes, message, group: already what CEL expects.
                    return value;
            }
        }

        /// <summary>
        ///     An unsigned field's value as a ulong. A signed input is reinterpreted bit for
        ///     bit rather than rejected: that is the same value on the wire, and it is what
        ///     the Java client does with Long bits for a uint64 field.
        /// </summary>
        private static object ToUnsigned(object value)
        {
            switch (value)
            {
                case ulong u:
                    return u;
                case uint u:
                    return (ulong)u;
                case long l:
                    return unchecked((ulong)l);
                case int i:
                    return unchecked((ulong)(long)i);
                default:
                    return value;
            }
        }

        /// <summary>
        ///     The CEL type of a protobuf field, taken from the field's own declared type.
        ///     Returns null when the descriptor does not settle it - a message, a map, or an
        ///     unrecognised type - and the caller should fall back to inferring from the value.
        ///     <para>
        ///         Keyed on the descriptor rather than the CLR type of the value, which is what
        ///         every other client and protovalidate do. C#'s generated types happen to
        ///         imply the right CEL type for each protobuf scalar, so inferring from the
        ///         value lands in the same place - but only by coincidence of the type system,
        ///         and it did not hold for enums, which have no CEL counterpart at all.
        ///     </para>
        /// </summary>
        internal static Google.Api.Expr.V1Alpha1.Type FindTypeForField(FieldDescriptor field)
        {
            if (field.IsMap)
            {
                // The key and value types live on the entry message; the bound value is a
                // dictionary and infers correctly from itself.
                return null;
            }

            Google.Api.Expr.V1Alpha1.Type singular = FindTypeForFieldType(field.FieldType);
            if (singular == null)
            {
                return null;
            }

            // A repeated field binds the whole collection.
            return field.IsRepeated ? Decls.NewListType(singular) : singular;
        }

        private static Google.Api.Expr.V1Alpha1.Type FindTypeForFieldType(FieldType fieldType)
        {
            switch (fieldType)
            {
                case FieldType.Float:
                case FieldType.Double:
                    return Checked.CheckedDouble;
                case FieldType.Int32:
                case FieldType.Int64:
                case FieldType.SInt32:
                case FieldType.SInt64:
                case FieldType.SFixed32:
                case FieldType.SFixed64:
                case FieldType.Enum:
                    return Checked.CheckedInt;
                case FieldType.UInt32:
                case FieldType.UInt64:
                case FieldType.Fixed32:
                case FieldType.Fixed64:
                    return Checked.CheckedUint;
                case FieldType.Bool:
                    return Checked.CheckedBool;
                case FieldType.String:
                    return Checked.CheckedString;
                case FieldType.Bytes:
                    return Checked.CheckedBytes;
                default:
                    // Message and group bind the message itself, whose type comes from its
                    // descriptor rather than from here.
                    return null;
            }
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