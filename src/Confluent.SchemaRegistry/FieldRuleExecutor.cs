// Copyright 2022 Confluent Inc.
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

namespace Confluent.SchemaRegistry
{
    public delegate object FieldTransformer(RuleContext ctx, FieldTransform fieldTransform, object message);
    
    public delegate object FieldTransform(RuleContext ctx, RuleContext.FieldContext fieldCtx, object fieldValue);
    
    public abstract class FieldRuleExecutor : IRuleExecutor
    {
        internal FieldTransformer FieldTransformer { get; set; }
        
        public abstract string Type();

        protected abstract FieldTransform newTransform(RuleContext ctx);

        public object Transform(RuleContext ctx, object message)
        {
            return FieldTransformer.Invoke(ctx, newTransform(ctx), message);
        }
    }
}
