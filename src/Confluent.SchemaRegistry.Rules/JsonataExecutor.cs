using Jsonata.Net.Native;
using Jsonata.Net.Native.Json;
using Jsonata.Net.Native.JsonNet;

namespace Confluent.SchemaRegistry.Rules
{
    public class JsonataExecutor : IRuleExecutor
    {
        public static void Register()
        {
            RuleRegistry.RegisterRuleExecutor(new JsonataExecutor());
        }

        public static readonly string RuleType = "JSONATA";
        
        private readonly IDictionary<string, JsonataQuery> cache = new Dictionary<string, JsonataQuery>();
        private readonly SemaphoreSlim cacheMutex = new SemaphoreSlim(1);
        
        public JsonataExecutor()
        {
        }
        
        public void Configure(IEnumerable<KeyValuePair<string, string>> config)
        {
        }

        public string Type() => RuleType;


        public async Task<object> Transform(RuleContext ctx, object message)
        {
            JsonataQuery query;
            await cacheMutex.WaitAsync().ConfigureAwait(continueOnCapturedContext: false);
            try
            {
                if (!cache.TryGetValue(ctx.Rule.Expr, out query))
                {
                    query = new JsonataQuery(ctx.Rule.Expr);
                    cache[ctx.Rule.Expr] = query;
                }
            }
            finally
            {
                cacheMutex.Release();
            }
            JToken jsonObj = JsonataExtensions.FromNewtonsoft((Newtonsoft.Json.Linq.JToken)message);
            JToken jtoken = query.Eval(jsonObj);
            object result = JsonataExtensions.ToNewtonsoft(jtoken);
            return result;
        }

        public void Dispose()
        {
        }
    }
}
