SolarEdge InfluxDB monitor
==========================

This is a simple python tool to export SolarEdge inverter data to
an InfluxDb <https://www.influxdata.com/time-series-platform/influxdb/> for monitoring purposes. It works by reading the inverter
data from a SolarEdge inverter that has its ModBusTCP interface activated.
It then connects via TCP and reads the SunSpec modbus registers via ModBusTCP.

See <https://www.solaredge.com/sites/default/files/sunspec-implementation-technical-note.pdf> for details of the SunSpec implementation from solarEdge.

Docker Usage
---
```
docker run -d \ 
    -e INFLUXDB=<hostname/IP of InfluxDB server - default=localhost> \ 
    -e INFLUXPORT=<port InfluxDB is running on - default=8086> \ 
    -e INVERTER=<hostname/IP of SolarEdge Inverter - default=192.168.1.2> \
    -e INVERTERPORT=<ModbusTCP port on Inverter - default=502> \
    -e METERS=<number of Modbus meters attached to Inverter - default=0 range=0-3> \
    -e UNITID=<Modbus ID of Inverter - default=1> \
    alexcpu/solaredge-influxdb
```
Please replace user variables in the above command defined by <> with the correct values.  Environment variables can be excluded if the defaults are suitable.

Docker Example
---
```
docker run -d \ 
    -e INFLUXDB=192.168.1.50 \ 
    -e INVERTER=192.168.1.200 \
    -e METERS=1 \
    alexcpu/solaredge-influxdb
```

Command Line Usage:
------
`./solaredge.py [inverter IP]`

In addition, you can specify additional flags to customize the tool:
* `--influxdb` specifies the IP or hostname of the InfluxDb (default localhost)
* `--influxport` specifies the port InfluxDb is running on (default 8086)
* `--unitid` specifies the ModBus ID used by the inverter (default 1)
* `--port` specifies the ModBus TCP port to connect to (default 502)
* `--meters` specifies the number of ModBus meters connected to the inverter (default 0) (range 0-3)
* `-d` or `--debug` activates debug logging
