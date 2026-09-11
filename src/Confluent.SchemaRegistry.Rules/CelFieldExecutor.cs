using Avro;
using Avro.Generic;
using Avro.Specific;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

namespace Confluent.SchemaRegistry.Rules
{
    public class CelFieldExecutor : FieldRuleExecutor
    {
        public static void Register()
        {
            RuleRegistry.RegisterRuleExecutor(new CelFieldExecutor());
        }

        public static readonly string RuleType = "CEL_FIELD";

        private CelExecutor celExecutor;

        public CelFieldExecutor()
        {
            this.celExecutor = new CelExecutor();
        }

        public override string Type() => RuleType;


        public override void Configure(IEnumerable<KeyValuePair<string, string>> config,
            ISchemaRegistryClient client = null)
        {
        }
        
        public override IFieldTransform NewTransform(RuleContext ctx)
        {
            CelFieldExecutorTransform transform = new CelFieldExecutorTransform(celExecutor);
            transform.Init(ctx);
            return transform;
        }

        public override void Dispose()
        {
            celExecutor.Dispose();
        }
        
        public class CelFieldExecutorTransform : IFieldTransform
        {
            private CelExecutor celExecutor;
            
            public CelFieldExecutorTransform(CelExecutor celExecutor)
            {
                this.celExecutor = celExecutor;
            }

            public void Init(RuleContext ctx)
            {
            }

            public async Task<object> Transform(RuleContext ctx, RuleContext.FieldContext fieldCtx, object fieldValue)
            {
                if (!fieldCtx.IsPrimitive())
                {
                    // CEL field transforms only apply to primitive types
                    return fieldValue;
                }
                object message = fieldCtx.ContainingMessage;
                // Convert before the executor reads it: the value determines the declared
                // type as well as the binding, and Cel.NET rejects a CLR enum in either.
                object celValue = CelExecutor.ToCelValue(fieldValue);
                object result = await celExecutor.Execute(ctx, celValue, new Dictionary<string, object>
                    {
                        { "value", celValue ?? NullValue.NullValue},
                        { "fullName", fieldCtx.FullName },
                        { "name", fieldCtx.Name },
                        { "typeName", fieldCtx.Type.ToString().ToUpper() },
                        { "tags", fieldCtx.Tags.ToList() },
                        { "message", message }
                    }
                ).ConfigureAwait(false);
                if (result is ByteString)
                {
                    result = ((ByteString)result).ToByteArray();
                }

                // The null branch carries no type, so neither arm below can key off the value
                // alone: a rule materialising a decimal from it handed Avro a BigDecimal it
                // cannot write. The containing message is what is still known, and each
                // conversion is a no-op for any other result, so both are attempted.
                bool nullAvroField = fieldValue == null
                    && (message is GenericRecord || message is ISpecificRecord);

                // Symmetric with the DateTime arm of ToCelValue above: the field went in as a
                // DateTime and was presented to CEL as a timestamp, so what comes back has to
                // become a DateTime again before Avro's writer sees it. A protobuf Timestamp
                // field is a message rebuilt by ProtobufUtils, and is left alone.
                if (fieldValue is DateTime || nullAvroField)
                {
                    result = CelExecutor.ToAvroDateTimeOrNull(result) ?? result;
                }

                // The decimal counterpart: an Avro decimal field goes in as an AvroDecimal and
                // is presented to CEL as a decimal, so the result has to be turned back. On a
                // repeated field the walk applies the rule per element, so `fieldValue` is the
                // element and this covers arrays as well as scalars.
                if (fieldValue is AvroDecimal || nullAvroField)
                {
                    result = CelExecutor.ToAvroDecimalOrNull(result) ?? result;
                }

                return result;
            }
            
            public void Dispose()
            {
            }
        }
    }
}