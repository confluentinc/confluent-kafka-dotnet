# TLS Authentication Example

This is a walkthough of how to configure Kafka for TLS authentication with example code in C#.

For simplicity we're just going to be setting up a single broker and client, but we'll also make some notes along the way on what you'll need to do for more complex configurations.

As you work through these instructions, you'll need to specify the names of your broker `{server_hostname}` and client `{client_hostname}` machines a number of times. Ideally, you will use the Fully Qualified Domain Name (FQDN) of the machines but if you haven't assigned hostnames to your machines, you can make do with IP addresses. If you are just experimenting and you're going to run the broker and client on the same machine, it's also ok to use `localhost` or `127.0.0.1` for both.


For further information, some good resources are:

- [https://github.com/edenhill/librdkafka/wiki/Using-SSL-with-librdkafka](https://github.com/edenhill/librdkafka/wiki/Using-SSL-with-librdkafka)
- [https://kafka.apache.org/documentation/#security](https://kafka.apache.org/documentation/#security)
- [https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)


## One-Way TLS

- Exactly the same setup as an HTTPS connection to a website.
- Client verifies the identity of the broker - i.e. the client can be sure it is communicating with the intended broker, not an impersonator. i.e. safe from man-in-the-middle attacks.
- Communication is encrypted.
- Server **does not** verify the identity of the client (does not authenticate the client) - any client can connect to the server.


**Procedure:**

1. Create a *private key* / *public key certificate* pair for the broker:

    **Note:** You will need to repeat this step for every broker in your cluster.

    **Note:** We use the `keytool` utility that comes with Java for this task to generated key / certificate pair in a JKS container. Since v2.7.0, Kafka also supports PKCS12 format.

    ```
    keytool -keystore server.keystore.jks -alias {server_hostname} -validity 365 -genkey -keyalg RSA -dname "cn=127.0.0.1"
    ```

    For the `localhost` case:

    ```
    keytool -keystore server.keystore.jks -alias 127.0.0.1 -validity 365 -genkey -keyalg RSA -dname "cn=127.0.0.1"
    ```

    **Note:** We are using the IP address `127.0.0.1`, not `localhost` to help you avoid potential problems in the case where `localhost` resolves to an IPv6 address.


    This will:

    1. Create a new keystore `server.keystore.jks` in the current directory.
   
    1. Prompt you for a password to protect the keystore.
        - Alternatively, you can specify this using the `-storepass` command line argument.

    1. Generate a new public/private key pair and set the Distinguishing Name (DN) as required for our purposes.
        - Alternatively, you can omit the `-dname` parameter, and you will be prompted to enter this information. You need to set the CN (the answer to the question 'What is your first and last name?') to `{server_hostname}`. None of the other information is important for our purposes.

    1. Store the private key and self-signed public key certificate under the alias `{server_hostname}` in the keystore file. You will be prompted for a password to protect this specific key / certificate pair in the keystore.
        - Alternatively, you can specify the password using the `-keypass` command line argument.


1. Create a Certificate Authority (CA) private key / root certificate:

    **Important Note:** Don't be tempted to simplify the configuration procedure by using self-signed certificates (avoiding the use of a CA) - this is not secure (is susceptible to man-in-the-middle attacks).

    ```
    openssl req -nodes -new -x509 -keyout ca-root.key -out ca-root.crt -days 365 -subj "/C=US/ST=CA/L=MV/O=CFLT/CN=CFLT"
    ```

    This will:

    1. Create a private key / self-signed public key cetificate pair where the private key isn't password protected.
       - Note that the `-subj` command line argument is used to specify the DN information in the certificate. If not specified, you will be prompted for this. None of this is important for our purposes.

    1. If you would like to password protect the private key, omit the `-nodes` flag and you will be prompted for a password.


1. Sign the broker public key certificate:

    1. Generate a Certificate Signing Request (CSR) from the self-signed certificate you created in step 1 housed inside the server keystore file:

    ```
    keytool -keystore server.keystore.jks -alias {server_hostname} -certreq -file {server_hostname}_server.csr
    ```

    For the `localhost` case:

    ```
    keytool -keystore server.keystore.jks -alias 127.0.0.1 -certreq -file 127.0.0.1_server.csr
    ```


    2. Use the CA key pair you generated in step 2 to create a CA signed certificate from the CSR you just created:

    ```
    openssl x509 -req -CA ca-root.crt -CAkey ca-root.key -in {server_hostname}_server.csr -out {server_hostname}_server.crt -days 365 -CAcreateserial 
    ```

    For the `localhost` case:

    ```
    openssl x509 -req -CA ca-root.crt -CAkey ca-root.key -in 127.0.0.1_server.csr -out 127.0.0.1_server.crt -days 365 -CAcreateserial 
    ```


    3. Import this signed certificate into your server keystore (over-writing the existing self-signed one). Before you can do this, you'll need to add the CA public key certificate as well:

    ```
    keytool -keystore server.keystore.jks -alias CARoot -import -noprompt -file ca-root.crt
    keytool -keystore server.keystore.jks -alias {server_hostname} -import -file {server_hostname}_server.crt
    ```

    For the `localhost` case:

    ```
    keytool -keystore server.keystore.jks -alias CARoot -import -noprompt -file ca-root.crt
    keytool -keystore server.keystore.jks -alias 127.0.0.1 -import -file 127.0.0.1_server.crt
    ```


1. Configure the broker and client

    You now have everything you need to configure the broker and client.

    Broker config (in the `localhost` case):

    ```
    listeners=PLAINTEXT://127.0.0.1:9092,SSL://127.0.0.1:9093
    ssl.keystore.location=/path/to/keystore/file/server.keystore.jks
    ssl.keystore.type=JKS
    ssl.keystore.password={password}
    ssl.key.password={password}
    ```

    `Confluent.Kafka` config (in the `localhost` case):

    ```
    ProducerConfig config = new ProducerConfig
    {
        BootstrapServers = "127.0.0.1:9093",
        SecurityProtocol = SecurityProtocol.Ssl,
        SslCaLocation = @"C:\path\to\caroot\file\ca-root.crt"
    };
    ```

1. Run the example program:

    In the `localhost` case:

    ```
    dotnet run 127.0.0.1:9093 my-topic-name noauth ca-root.crt
    ```


## Two-Way TLS (Example 1 + TLS Client Authentication)

- Functionality provided by Example 1, plus:

- Server verifies the identity of the client (i.e. that the public key certificate provided by the client has been signed by a CA trusted by the server).


**Procedure:**

1. Create a private key / public key certificate pair for the client: 

    The .NET client is not Java based and consequently doesn't use Java's JKS container format for storing private keys and certificates. We will use `openssl` to create the key / certificate pair for the client, not `keytool` as we did for the broker.

    The first step is to create a Certificate Signing Request (CSR). Note: there is no need to explicitly create a self signed certificate first as we did for the broker.

    ```
    openssl req -newkey rsa:2048 -nodes -keyout {client_hostname}_client.key -out {client_hostname}_client.csr -subj "/C=US/ST=CA/L=MV/O=CFLT/CN=CFLT"
    ```

    In the `localhost` case:

    ```
    openssl req -newkey rsa:2048 -nodes -keyout 127.0.0.1_client.key -out 127.0.0.1_client.csr -subj "/C=US/ST=CA/L=MV/O=CFLT/CN=CFLT"
    ```

    The public key certificate field names (specified via the `-subj` command line option) can be used for Authorization purposes. For more information refer to the [Authorization using ACLs](https://docs.confluent.io/platform/current/kafka/authorization.html) page in the Confluent documentation. Authorization is out of scope in this example, and none of this information is important for our purposes.

    You will be prompted for a password. You can enter a blank password here, or set a password.

    Now you have the CSR, you can generate a CA signed certificate as follows:

    ```
    openssl x509 -req -CA ca-root.crt -CAkey ca-root.key -in {client_hostname}_client.csr -out {client_hostname}_client.crt -days 365 -CAcreateserial 
    ```

    In the `localhost` case:

    ```
    openssl x509 -req -CA ca-root.crt -CAkey ca-root.key -in 127.0.0.1_client.csr -out 127.0.0.1_client.crt -days 365 -CAcreateserial 
    ```

    Now package the client key and certificate into a single PKCS12 file (you will be prompted for a password):

    ```
    openssl pkcs12 -export -in {client_hostname}_client.crt -inkey {client_hostname}_client.key -name ‘{client_hostname}’ -out client.keystore.p12
    ```

    In the `localhost` case:

    ```
    openssl pkcs12 -export -in 127.0.0.1_client.crt -inkey 127.0.0.1_client.key -name ‘127.0.0.1’ -out client.keystore.p12
    ```

    **Note:** Creating the PKCS12 keystore is optional. If you skip this step, use the `SslCertificateLocation` and `SslKeyLocation` client configuration properties instead of `SslKeystoreLocation` and `SslKeystorePassword`


1. Create a truststore containing the ca-root.crt:

    The broker now requires access to the CA root certificate in order to check the validity of the certificate supplied by the client. Create a truststore (another JKS container file) that contains the CA root certificate as follows:

    ```
    keytool -keystore server.truststore.jks -alias CARoot -import -file ca-root.crt
    ```

1. Configure the broker and client.

    Broker config (in the `localhost` case):

    ```
    listeners=PLAINTEXT://127.0.0.1:9092,SSL://127.0.0.1:9093
    ssl.keystore.location=/path/to/keystore/file/server.keystore.jks
    ssl.keystore.type=JKS
    ssl.keystore.password={password}
    ssl.key.password={password}
    ssl.truststore.location=/path/to/truststore/file/server.truststore.jks
    ssl.truststore.type=JKS
    ssl.truststore.password={password}
    ssl.client.auth=required
    ```

    Confluent.Kafka config (in the `localhost` case):

    ```
    ProducerConfig config = new ProducerConfig
    {
        BootstrapServers = "127.0.0.1:9093",
        SecurityProtocol = SecurityProtocol.Ssl,
        SslCaLocation = @"C:\path\to\caroot\file\ca-root.crt",
        SslKeystoreLocation = @"C:\path\to\client\keystore\file\client.keystore.p12",
        SslKeystorePassword = "{password}"
    };
    ```

1. Run the example program:

    In the `localhost` case:

    ```
    dotnet run 127.0.0.1:9093 my-topic-name auth ca-root.crt client.keystore.p12 {password}
    ```
