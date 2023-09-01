using System;
using System.Threading.Tasks;
using Xunit;

namespace Confluent.SchemaRegistry.IntegrationTests;

public static partial class Tests
{
	[Theory, MemberData(nameof(SchemaRegistryParameters))]
	public static async Task UpdateCompatibility(Config config)
	{
		var sr = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = config.Server });

		// Case 1: Subject is not specified

		var globalCompatibility = await sr.UpdateCompatibilityAsync(Compatibility.BackwardTransitive);
		Assert.Equal(Compatibility.BackwardTransitive, globalCompatibility);

		Assert.Equal(Compatibility.BackwardTransitive, await sr.GetCompatibilityAsync());

		// Case 2: Subject is specified

		var testSchema =
			"{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"Confluent.Kafka.Examples.AvroSpecific" +
			"\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favorite_number\",\"type\":[\"i" +
			"nt\",\"null\"]},{\"name\":\"favorite_color\",\"type\":[\"string\",\"null\"]}]}";


		var topicName = Guid.NewGuid().ToString();
		var subject =
			SubjectNameStrategy.Topic.ConstructKeySubjectName(topicName, "Confluent.Kafka.Examples.AvroSpecific.User");

		await sr.RegisterSchemaAsync(subject, testSchema);

		var compatibility = await sr.UpdateCompatibilityAsync(Compatibility.FullTransitive, subject);
		Assert.Equal(Compatibility.FullTransitive, compatibility);

		Assert.Equal(Compatibility.FullTransitive, await sr.GetCompatibilityAsync(subject));
		Assert.Equal(Compatibility.BackwardTransitive, await sr.GetCompatibilityAsync());
	}
}