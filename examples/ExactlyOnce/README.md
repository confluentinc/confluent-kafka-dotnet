# Basic EOS producer example

This example shows a basic Exactly-once semantics producer moving messages from
an input topic to an output topic.

One or more producers or consumers can be run. Producers create random words while consumers reverse those words before sending
them to an output topic.

For each one of these a different transactional id is needed.

Configure bootstrap servers and other properties in appsettings.json.

Steps:
1. run a producer with: create_words <transactional-id1>
2. run a consumer with: reverse_words <transactional-id2>
3. optionally run other producers or consumers with different transactional ids.