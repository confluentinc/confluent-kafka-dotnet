FROM ubuntu:16.04

COPY bootstrap.sh /opt/
COPY make_settings.sh /opt/

RUN apt-get update \
  && apt-get -y install curl \
  && cd /opt \
  && chmod a+rwx /opt/bootstrap.sh \
  && curl -L "https://github.com/prometheus/prometheus/releases/download/v2.6.0/prometheus-2.6.0.linux-amd64.tar.gz" -o /opt/prometheus-2.6.0.linux-amd64.tar.gz \
  && tar xvf prometheus-2.6.0.linux-amd64.tar.gz

CMD /opt/bootstrap.sh
