using Cel;

namespace Confluent.SchemaRegistry.Rules
{
    public class BuiltinLibrary : ILibrary
    {

        public virtual IList<EnvOption> CompileOptions
        {
            get => new List<EnvOption> { IEnvOption.Declarations(BuiltinDeclarations.Create()) };
        }

        public virtual IList<ProgramOption> ProgramOptions
        {
            get => new List<ProgramOption>
            {
                IProgramOption.EvalOptions(EvalOption.OptOptimize),
                IProgramOption.Functions(BuiltinOverload.Create())
            };
        }
    }
}
