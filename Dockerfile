FROM python:3.6-alpine

LABEL maintainer="https://github.com/AlexCPU/Solaredge-influxdb"

ENV INFLUXDB=localhost
ENV INFLUXPORT=8086
ENV INVERTER=192.168.1.2
ENV INVERTERPORT=502
ENV UNITID=1
ENV METERS=0

ADD requirements.txt /

RUN apk add --no-cache --update alpine-sdk && \
    pip3 install -r /requirements.txt && \
    apk del alpine-sdk

ADD solaredge.py /

CMD python3 /solaredge.py --influxdb $INFLUXDB --influxport $INFLUXPORT --port $INVERTERPORT --unitid $UNITID --meters $METERS $INVERTER
