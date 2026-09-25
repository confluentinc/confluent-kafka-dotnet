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

using Cel.Common.Types;
using Cel.Common.Types.Ref;
using SrVariant = Confluent.SchemaRegistry.Variant;
using Type = System.Type;

namespace Confluent.SchemaRegistry.Rules
{
    /// <summary>
    ///     The CEL runtime value for a Variant. cel.net has no opaque/abstract value type, so a
    ///     Variant is carried as this <see cref="BaseVal" /> subclass wrapping a
    ///     <see cref="SrVariant" />, named by <see cref="CelTypeLabels.VariantName" /> so it
    ///     agrees with the abstract type declared to the checker — mirroring
    ///     <see cref="DecimalT" />. The <c>variant(...)</c> constructor and the
    ///     <c>variants.*</c> accessors produce and consume it; <c>string(...)</c> reaches it
    ///     through the stdlib conversion, which calls <see cref="ConvertToType" />.
    /// </summary>
    internal sealed class VariantT : BaseVal
    {
        /// <summary>Runtime type value; its name matches the checker's abstract Variant type.</summary>
        public static readonly IType VariantCelType = TypeT.NewObjectTypeValue(CelTypeLabels.VariantName);

        private readonly SrVariant value;

        private VariantT(SrVariant value)
        {
            this.value = value;
        }

        public static VariantT Of(SrVariant value) => new VariantT(value);

        public SrVariant Variant => value;

        public override object Value() => value;

        public override IType Type() => VariantCelType;

        public override IVal Equal(IVal other)
        {
            return other is VariantT o ? Types.BoolOf(value.Equals(o.value)) : BoolT.False;
        }

        public override IVal ConvertToType(IType typeValue)
        {
            switch (typeValue.TypeName())
            {
                case "string":
                    return StringT.StringOf(value.ToJson());
                case "type":
                    return VariantCelType;
                default:
                    if (typeValue.TypeName().Equals(CelTypeLabels.VariantName))
                    {
                        return this;
                    }

                    return Err.NewErr("type conversion error from '{0}' to '{1}'",
                        CelTypeLabels.VariantName, typeValue.TypeName());
            }
        }

        public override object ConvertToNative(Type typeDesc)
        {
            if (typeDesc == typeof(SrVariant) || typeDesc == typeof(object))
            {
                return value;
            }

            if (typeDesc == typeof(string))
            {
                return value.ToJson();
            }

            return Err.NewTypeConversionError(VariantCelType, typeDesc.Name);
        }

        public override bool Equals(object o) => o is VariantT other && value.Equals(other.value);

        public override int GetHashCode() => value.GetHashCode();

        public override string ToString() => value.ToJson();
    }
}
