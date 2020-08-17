FROM python:3.8-alpine

MAINTAINER "https://github.com/CoolDil/solaredge-exporter_dual-database"

ENV INFLUX_SERVER=192.168.1.1
ENV INFLUX_DATABASE=solaredge
ENV INFLUX_PORT=8086
ENV PROMETHEUS_EXPORTER_PORT=2112
ENV INVERTER_IP=192.168.1.2
ENV INVERTER_PORT=1502
ENV UNITID=1
ENV METERS=0
ENV INTERVAL=5
ENV LEGACY_SUPPORT=False

EXPOSE 2112/tcp

ADD requirements.txt /
RUN apk add --no-cache --update alpine-sdk && \
    pip3 install -r /requirements.txt && \
    apk del alpine-sdk

ADD solaredge.py /

CMD python3 /solaredge.py --influx_server $INFLUX_SERVER --influx_port $INFLUX_PORT --influx_database $INFLUX_DATABASE --prometheus_exporter_port $PROMETHEUS_EXPORTER_PORT --inverter_port $INVERTER_PORT --unitid $UNITID --meters $METERS --interval $INTERVAL --legacy_support $LEGACY_SUPPORT $INVERTER_IP 
