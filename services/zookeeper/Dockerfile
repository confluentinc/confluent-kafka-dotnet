FROM ubuntu:18.04

COPY make_settings.sh /opt/
COPY bootstrap.sh /opt/
COPY jmx_zookeeper.yml /opt/

RUN apt-get update \
  && apt-get -y install openjdk-8-jdk curl \
  && cd /opt \
  && curl "http://us.mirrors.quenda.co/apache/kafka/2.1.1/kafka_2.11-2.1.1.tgz" -o /opt/kafka.tgz \
  && tar xvf kafka.tgz \
  && curl "https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.9/jmx_prometheus_javaagent-0.9.jar" -o /opt/jmx_prometheus_javaagent-0.9.jar

CMD /opt/bootstrap.sh
