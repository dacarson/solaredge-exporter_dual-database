SolarEdge InfluxDB and Prometheus monitor
==========================

This is a simple python tool to export SolarEdge inverter data to multiple monitoring platforms; <a href="https://www.influxdata.com/time-series-platform/influxdb/">InfluxDB</a> as well as <a href="https://prometheus.io/">Promethus</a>.  It works by reading the inverter
data from a SolarEdge inverter that has its ModBusTCP interface activated.
It then connects via TCP and reads the SunSpec modbus registers via ModBusTCP.

See <https://www.solaredge.com/sites/default/files/sunspec-implementation-technical-note.pdf> for details of the SunSpec implementation from solarEdge.

Docker Usage
---
```
docker run -d \ 
    -e INFLUX_SERVER=<hostname/IP of InfluxDB server - default=192.168.1.1> \ 
    -e INFLUX_PORT=<port InfluxDB is running on - default=8086> \ 
    -e INFLUX_DATABASE=<name of the InfluxDB database - default=solaredge> \
    -e INVERTER_IP=<hostname/IP of SolarEdge Inverter - default=192.168.1.2> \
    -e INVERTER_PORT=<ModbusTCP port on Inverter - default=502> \
    -e METERS=<number of Modbus meters attached to Inverter - default=0 range=0-3> \
    -e UNITID=<Modbus ID of Inverter - default=1> \
    -e PROMETHEUS_EXPORTER_PORT<Port to have the prometheus exporter listen on - default=2112> \

```
Please replace user variables in the above command defined by <> with the correct values.  Environment variables can be excluded if the defaults are suitable.

Docker Example
---
```
docker run -d \ 
    -e INFLUX_SERVER=192.168.1.50 \ 
    -e INVERTER_IP=192.168.1.200 \
    -e METERS=1 \

```

Command Line Usage:
------
`./solaredge.py [inverter IP]`

In addition, you can specify additional flags to customize the tool:
* `--influx_server` specifies the IP or hostname of the InfluxDb (default localhost)
* `--influx_port` specifies the port InfluxDb is running on (default 8086)
* `--influx_database` specifies the InfluxDb database to use (default solaredge)
* `--unitid` specifies the ModBus ID used by the inverter (default 1)
* `--inverter_port` specifies the ModBus TCP port to connect to (default 1502)
* `--meters` specifies the number of ModBus meters connected to the inverter (default 0) (range 0-3)
* `--prometheus_exporter_port` specifies the port that the inverter data will be availabe for Prometheus scraping (default 2112)
* `-d` or `--debug` activates debug logging
