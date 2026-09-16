using Cel;

namespace Confluent.SchemaRegistry.Rules
{
    public class BuiltinLibrary : ILibrary
    {

        public virtual IList<EnvOption> CompileOptions
        {
            get => new List<EnvOption>
            {
                EnvOptions.Declarations(BuiltinDeclarations.Create()),
                // Let a comparison mix int, uint and double operands, so that a rule on an
                // unsigned field can be written `value > 0` and not only `value > 0u`. The
                // Java, Go and Python clients all enable the same thing; without it, .NET
                // would reject expressions they accept. Ordering only - equality stays
                // homogeneous in every client.
                EnvOptions.Features(EnvFeature.FeatureCrossTypeNumericComparisons)
            };
        }

        public virtual IList<ProgramOption> ProgramOptions
        {
            get => new List<ProgramOption>
            {
                Cel.ProgramOptions.EvalOptions(EvalOption.OptOptimize),
                Cel.ProgramOptions.Functions(BuiltinOverload.Create())
            };
        }
    }
}