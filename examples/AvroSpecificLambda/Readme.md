# AvroSpecificLambda

This project contains a basic Lambda implementation, and shows how to produce a message from a Lambda function. Publish this lambda to your AWS host with the instructions below, and then configure a test in the AWS console with an input in the following format:

{
  "BootstrapServers": "kafka1.myserver.com:9092,kafka2.myserver.com:9092,",
  "SchemaRegistryUrl": "http://sr.myserver.com:8081",
  "TopicName": "mytopic"
}


## If you are going to host a WebApi under a Lambda - Use Async Methods Only!

You must use async Lambda methods to work with the Confluent.Kafka library. Attempting to use .Result, or GetAwaiter().GetResult() will freeze the MVC WebApi thread and prevent your calls from returning any results. This is true for all async methods under WebApi, not just the Confluent libraries. This sample uses async as a model.

## Here are some steps to follow from Visual Studio:

To deploy your function to AWS Lambda, right click the project in Solution Explorer and select *Publish to AWS Lambda*.

To view your deployed function open its Function View window by double-clicking the function name shown beneath the AWS Lambda node in the AWS Explorer tree.

To perform testing against your deployed function use the Test Invoke tab in the opened Function View window.

To configure event sources for your deployed function, for example to have your function invoked when an object is created in an Amazon S3 bucket, use the Event Sources tab in the opened Function View window.

To update the runtime configuration of your deployed function use the Configuration tab in the opened Function View window.

To view execution logs of invocations of your function use the Logs tab in the opened Function View window.

## Here are some steps to follow to get started from the command line:

Once you have edited your template and code you can deploy your application using the [Amazon.Lambda.Tools Global Tool](https://github.com/aws/aws-extensions-for-dotnet-cli#aws-lambda-amazonlambdatools) from the command line.

Install Amazon.Lambda.Tools Global Tools if not already installed.
```
    dotnet tool install -g Amazon.Lambda.Tools
```

If already installed check if new version is available.
```
    dotnet tool update -g Amazon.Lambda.Tools
```

Execute unit tests
```
    cd "AvroSpecificLambda/test/AvroSpecificLambda.Tests"
    dotnet test
```

Deploy function to AWS Lambda
```
    cd "AvroSpecificLambda/src/AvroSpecificLambda"
    dotnet lambda deploy-function
```
