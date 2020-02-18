using System.Collections.Generic;

namespace Confluent.SchemaRegistry.Serdes.IntegrationTests
{
    public class Util
    {
        public static List<string> GetRandomJsonSchemas(int num)
        {
            var schemas = new List<string>();
            for (int i=0; i<num; ++i)
            {
                // var schema = ;
            }
            return schemas;
        }
    }
}





//   public static List<String> getRandomJsonSchemas(int num) {
//     List<String> schemas = new ArrayList<>();
//     for (int i = 0; i < num; i++) {
//       String schema = "{\"type\":\"object\",\"properties\":{\"f"
//           + random.nextInt(Integer.MAX_VALUE)
//                     + "\":"
//           + "{\"type\":\"string\"}},\"additionalProperties\":false}";
//       schemas.add(schema);
//     }
//     return schemas;
//   }

//   public static Map<String, String> getJsonSchemaWithReferences() {
//     Map<String, String> schemas = new HashMap<>();
//     String reference = "{\"type\":\"object\",\"additionalProperties\":false,\"definitions\":"
//         + "{\"ExternalType\":{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"}},"
//         + "\"additionalProperties\":false}}}";
//                /*
//         String reference = "{\n  \"type\": \"object\",\n"
//             + "  \"additionalProperties\": false,\n"
//             + "  \"definitions\": {\n"
//             + "    \"ExternalType\": {\n"
//             + "      \"type\": \"object\",\n"
//             + "      \"additionalProperties\": false,\n"
//             + "      \"properties\": {\n"
//             + "        \"name\": {\n"
//             + "          \"type\": \"string\"\n"
//             + "        }\n"
//             + "      }\n"
//             + "    }\n"
//             + "  }\n"
//             + "}\n";
//          */
//     schemas.put("ref.json", new JsonSchema(reference).canonicalString());
//     String schemaString = "{\"type\":\"object\",\"properties\":{\"Ref\":"
//         + "{\"$ref\":\"ref.json#/definitions/ExternalType\"}},\"additionalProperties\":false}";
//         /*
//         String schemaString = "{\n  \"type\": \"object\",\n"
//             + "  \"additionalProperties\": false,\n"
//             + "  \"properties\": {\n"
//             + "    \"Ref\": {\n"
//             + "      \"$ref\": \"ref.json#/definitions/ExternalType\"\n"
//             + "    }\n"
//             + "  }\n"
//             + "}\n";
//          */
//     schemas.put("main.json", schemaString);
//     return schemas;
//   }
// }