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
using Type = System.Type;

namespace Confluent.SchemaRegistry.Rules
{
    /// <summary>
    ///     The CEL runtime value for a Decimal. cel.net has no opaque/abstract value type,
    ///     so Decimal is carried as this <see cref="BaseVal" /> subclass wrapping a
    ///     <see cref="BigDecimal" />, named by <see cref="CelTypeLabels.DecimalName" /> so it
    ///     agrees with the abstract type declared to the checker. <c>string(...)</c> and
    ///     <c>double(...)</c> reach it through the stdlib conversions, which call
    ///     <see cref="ConvertToType" />.
    /// </summary>
    internal sealed class DecimalT : BaseVal
    {
        /// <summary>Runtime type value; its name matches the checker's abstract Decimal type.</summary>
        public static readonly IType DecimalType = TypeT.NewObjectTypeValue(CelTypeLabels.DecimalName);

        private readonly BigDecimal value;

        private DecimalT(BigDecimal value)
        {
            this.value = value;
        }

        public static DecimalT Of(BigDecimal value) => new DecimalT(value);

        public BigDecimal Decimal => value;

        public override object Value() => value;

        public override IType Type() => DecimalType;

        public override IVal Equal(IVal other)
        {
            return other is DecimalT o ? Types.BoolOf(value.Equals(o.value)) : BoolT.False;
        }

        public override IVal ConvertToType(IType typeValue)
        {
            switch (typeValue.TypeName())
            {
                case "string":
                    return StringT.StringOf(value.ToPlainString());
                case "double":
                    return DoubleT.DoubleOf(value.ToDouble());
                case "type":
                    return DecimalType;
                default:
                    if (typeValue.TypeName().Equals(CelTypeLabels.DecimalName))
                    {
                        return this;
                    }

                    return Err.NewErr("type conversion error from '{0}' to '{1}'",
                        CelTypeLabels.DecimalName, typeValue.TypeName());
            }
        }

        public override object ConvertToNative(Type typeDesc)
        {
            if (typeDesc == typeof(BigDecimal) || typeDesc == typeof(object))
            {
                return value;
            }

            if (typeDesc == typeof(string))
            {
                return value.ToPlainString();
            }

            if (typeDesc == typeof(double))
            {
                return value.ToDouble();
            }

            return Err.NewTypeConversionError(DecimalType, typeDesc.Name);
        }

        public override bool Equals(object o) => o is DecimalT other && value.Equals(other.value);

        public override int GetHashCode() => value.GetHashCode();

        public override string ToString() => value.ToPlainString();
    }
}
