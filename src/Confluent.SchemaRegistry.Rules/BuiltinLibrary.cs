using Cel;

namespace Confluent.SchemaRegistry.Rules
{
    public class BuiltinLibrary : ILibrary
    {

        public virtual IList<EnvOption> CompileOptions
        {
            get => new List<EnvOption> { EnvOptions.Declarations(BuiltinDeclarations.Create()) };
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