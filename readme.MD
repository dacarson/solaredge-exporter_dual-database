SolarEdge InfluxDB and Prometheus monitor
==========================

This is a simple python tool to export SolarEdge inverter data to multiple monitoring platforms; <a href="https://www.influxdata.com/time-series-platform/influxdb/">InfluxDB</a> as well as <a href="https://prometheus.io/">Promethus</a>.  It works by reading the inverter
data from a SolarEdge inverter that has its ModBusTCP interface activated.
It then connects via TCP, reads the SunSpec modbus registers via ModBusTCP, pushes the data to InfluxDB and exposes the values via an exporter for Prometheus consumption.

Features
------
* **Inverter Monitoring**: Real-time monitoring of SolarEdge inverter performance metrics
* **Meter Support**: Support for up to 3 ModBus meters with individual phase monitoring
* **Battery Monitoring**: Support for up to 2 ModBus batteries with comprehensive battery metrics
* **Dual Database Support**: Simultaneous export to InfluxDB and Prometheus
* **Authentication**: Optional username/password authentication for InfluxDB
* **Configurable Polling**: Adjustable data collection interval
* **Legacy Compatibility**: Optional legacy meter naming convention support
* **Comprehensive Metrics**: AC/DC power, voltage, current, frequency, temperature, and energy data

See <https://www.solaredge.com/sites/default/files/sunspec-implementation-technical-note.pdf> for details of the SunSpec implementation from solarEdge.

Docker Usage
---
```
docker run -d \ 
    -e INFLUX_SERVER=<hostname/IP of InfluxDB server - default=192.168.1.1> \ 
    -e INFLUX_PORT=<port InfluxDB is running on - default=8086> \ 
    -e INFLUX_DATABASE=<name of the InfluxDB database - default=solaredge> \
    -e INFLUX_USER=<InfluxDB username - optional> \
    -e INFLUX_PASS=<InfluxDB password - optional> \
    -e INVERTER_IP=<hostname/IP of SolarEdge Inverter - default=192.168.1.2> \
    -e INVERTER_PORT=<ModbusTCP port on Inverter - default=502> \
    -e METERS=<number of Modbus meters attached to Inverter - default=0 range=0-3> \
    -e BATTERIES=<number of Modbus batteries attached to Inverter - default=0 range=0-2> \
    -e UNITID=<Modbus ID of Inverter - default=1> \
    -e PROMETHEUS_EXPORTER_PORT=<Port to have the prometheus exporter listen on - default=2112> \
    -e INTERVAL=<polling interval in seconds - default=5> \
    -e LEGACY_SUPPORT=<set to True to have Meter 1 prometheus metrics start with M_ vs M1_  default=False> \

```
Please replace user variables in the above command defined by <> with the correct values.  Environment variables can be excluded if the defaults are suitable.

Docker Example
---
```
docker run -d \ 
    -e INFLUX_SERVER=192.168.1.50 \ 
    -e INVERTER_IP=192.168.1.200 \
    -e METERS=1 \
    -e BATTERIES=1 \
    -e INFLUX_USER=admin \
    -e INFLUX_PASS=password123 \
    -e INTERVAL=10 \

```

Command Line Usage:
------
`./solaredge.py [inverter IP]`

In addition, you can specify additional flags to customize the tool:
* `--influx_server` specifies the IP or hostname of the InfluxDb (default localhost)
* `--influx_port` specifies the port InfluxDb is running on (default 8086)
* `--influx_database` specifies the InfluxDb database to use (default solaredge)
* `--influx_user` specifies the InfluxDB username for authentication (optional)
* `--influx_pass` specifies the InfluxDB password for authentication (optional)
* `--unitid` specifies the ModBus ID used by the inverter (default 1)
* `--inverter_port` specifies the ModBus TCP port to connect to (default 502)
* `--meters` specifies the number of ModBus meters connected to the inverter (default 0) (range 0-3)
* `--batteries` specifies the number of ModBus batteries connected to the inverter (default 0) (range 0-2)
* `--prometheus_exporter_port` specifies the port for Prometheus scraping (default 2112)
* `--interval` specifies the polling interval in seconds (default 5)
* `--legacy_support` enables legacy meter naming for Meter 1 (M_ vs M1_ prefix) (default False)
* `-d` or `--debug` activates debug logging
