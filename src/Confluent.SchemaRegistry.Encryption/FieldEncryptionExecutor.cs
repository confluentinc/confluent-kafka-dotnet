// Copyright 2025 Confluent Inc.
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

using System.Collections.Generic;
using System.Threading.Tasks;

namespace Confluent.SchemaRegistry.Encryption
{
    public class FieldEncryptionExecutor : FieldRuleExecutor
    {
        public static void Register()
        {
            RuleRegistry.RegisterRuleExecutor(new FieldEncryptionExecutor());
        }

        public static readonly string RuleType = "ENCRYPT";

        public static readonly string EncryptKekName = EncryptionExecutor.EncryptKekName;
        public static readonly string EncryptKmsKeyid = EncryptionExecutor.EncryptKmsKeyid;
        public static readonly string EncryptKmsType = EncryptionExecutor.EncryptKmsType;
        public static readonly string EncryptDekAlgorithm = EncryptionExecutor.EncryptDekAlgorithm;
        public static readonly string EncryptDekExpiryDays = EncryptionExecutor.EncryptDekExpiryDays;

        public static readonly string KmsTypeSuffix = EncryptionExecutor.KmsTypeSuffix;

        internal EncryptionExecutor EncryptionExecutor;

        public FieldEncryptionExecutor()
        {
            EncryptionExecutor = new EncryptionExecutor();
        }

        public FieldEncryptionExecutor(IDekRegistryClient client, IClock clock)
        {
            EncryptionExecutor = new EncryptionExecutor(client, clock);
        }

        public override void Configure(IEnumerable<KeyValuePair<string, string>> config,
            ISchemaRegistryClient client = null)
        {
            EncryptionExecutor.Configure(config, client);
        }

        public override string Type() => RuleType;

        public override IFieldTransform NewTransform(RuleContext ctx)
        {
            FieldEncryptionExecutorTransform transform = new FieldEncryptionExecutorTransform(EncryptionExecutor);
            transform.Init(ctx);
            return transform;
        }

        public override void Dispose()
        {
            EncryptionExecutor.Dispose();
        }
    }

    public class FieldEncryptionExecutorTransform : IFieldTransform
    {

        private EncryptionExecutor encryptionExecutor;
        private EncryptionExecutorTransform transform;

        public FieldEncryptionExecutorTransform(EncryptionExecutor encryptionExecutor)
        {
            this.encryptionExecutor = encryptionExecutor;
        }
        
        public void Init(RuleContext ctx)
        {
            transform = encryptionExecutor.NewTransform(ctx);

        }

        public async Task<object> Transform(RuleContext ctx, RuleContext.FieldContext fieldCtx, object fieldValue)
        {
            return await transform.Transform(ctx, fieldCtx.Type, fieldValue).ConfigureAwait(false);
        }

        public void Dispose()
        {
            transform?.Dispose();
        }
    }
}