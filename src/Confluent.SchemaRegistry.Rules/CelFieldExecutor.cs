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


        public override void Configure(IEnumerable<KeyValuePair<string, string>> config)
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
                object result = await celExecutor.Execute(ctx, fieldValue, new Dictionary<string, object>
                    {
                        { "value", fieldValue ?? NullValue.NullValue},
                        { "fullName", fieldCtx.FullName },
                        { "name", fieldCtx.Name },
                        { "typeName", fieldCtx.Type.ToString().ToUpper() },
                        { "tags", fieldCtx.Tags.ToList() },
                        { "message", message }
                    }
                );
                if (result is ByteString)
                {
                    result = ((ByteString)result).ToByteArray();
                }
                return result;
            }
            
            public void Dispose()
            {
            }
        }
    }
}