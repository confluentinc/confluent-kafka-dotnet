#!/bin/bash

keytool -keystore server.keystore.jks -alias ${SERVER_HOSTNAME} -validity 365 -genkey -keyalg RSA -dname "cn=${SERVER_HOSTNAME}" -storepass test1234 -keypass test1234
openssl req -nodes -new -x509 -keyout ca-root.key -out ca-root.crt -days 365 -subj "/C=US/ST=CA/L=Palo Alto/O=Confluent/CN=Confluent"
keytool -keystore server.keystore.jks -alias ${SERVER_HOSTNAME} -certreq -file ${SERVER_HOSTNAME}_server.csr -storepass test1234 -keypass test1234
openssl x509 -req -CA ca-root.crt -CAkey ca-root.key -in ${SERVER_HOSTNAME}_server.csr -out ${SERVER_HOSTNAME}_server.crt -days 365 -CAcreateserial
keytool -keystore server.keystore.jks -alias CARoot -import -noprompt -file ca-root.crt -keypass test1234 -storepass test1234
keytool -keystore server.keystore.jks -alias ${SERVER_HOSTNAME} -import -file ${SERVER_HOSTNAME}_server.crt -keypass test1234 -storepass test1234

# for client auth
openssl req -newkey rsa:2048 -nodes -keyout ${CLIENT_HOSTNAME}_client.key -out ${CLIENT_HOSTNAME}_client.csr -subj "/C=US/ST=CA/L=Palo Alto/O=Confluent/CN=Confluent"
openssl x509 -req -CA ca-root.crt -CAkey ca-root.key -in ${CLIENT_HOSTNAME}_client.csr -out ${CLIENT_HOSTNAME}_client.crt -days 365 -CAcreateserial
keytool -keystore server.truststore.jks -alias CARoot -import -file ca-root.crt -keypass test1234 -storepass test1234 -noprompt
