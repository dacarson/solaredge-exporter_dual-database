#!/usr/bin/env python3
import argparse
import datetime
import logging

from pyModbusTCP.client.mixin import ModbusClient
from pymodbus.client import ModbusClientMixin
import asyncio
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from prometheus_client import Gauge
from prometheus_client import start_http_server
from prometheus_client import Counter, Histogram

datapoint = {
    'measurement': 'SolarEdge',
    'tags': {},
    'fields': {}
}
reg_block = {}
promInv = {}
promMeter = {}
logger = logging.getLogger('solaredge')

# Prometheus counters/histogram for Modbus I/O health
modbus_send_errors = Counter('modbus_send_errors_total', 'Modbus send() failures', ['section'])
modbus_recv_errors = Counter('modbus_recv_errors_total', 'Modbus recv() failures', ['section'])
modbus_timeouts    = Counter('modbus_timeouts_total', 'Modbus timeouts', ['section'])
modbus_req_latency_sec = Histogram('modbus_request_latency_seconds', 'ReadHoldingRegisters latency', ['section'])

############################################################

def publish_metrics(dictobj, objtype, metriclabel, meternum=0, legacysupport=False):

    global datapoint

    if objtype == 'inverter' or objtype == 'battery':
        global promInv
        for key, value in dictobj.items():
            # InfluxDB metrics
            datapoint['fields'][key] = value
            # Prometheus Metrics
            if key in promInv:
                promInv[key].set(value)
            else:
                promInv[key] = Gauge(key, key)
                promInv[key].set(value)

    if objtype == 'meter':
        global promMeter
        for key, value in dictobj.items():
            # InfluxDB Metrics
            datapoint['fields'][key] = value
            # Prometheus Metrics
            if meternum==1 and legacysupport==True:
                if key in promMeter:
                    promMeter[key].set(value)
                else:
                    promMeter[key] = Gauge(key, key + ' - ' + metriclabel)
                    promMeter[key].set(value)
            else:
                metricname = key.replace('M_', 'M' + str(meternum) + '_')
                if metricname in promMeter:
                    promMeter[metricname].set(value)
                else:
                    promMeter[metricname] = Gauge(metricname, metricname + ' - ' + metriclabel)
                    promMeter[metricname].set(value)
                  
############################################################


async def write_to_influx(dbhost, dbport, mbmeters, mbbatteries, period, dbname, legacysupport, uname, passw):
    global client
    global datapoint
    global reg_block
    global promInv
    global promMeter

    def trunc_float(floatval):
        return float('%.2f' % floatval)

    try:
        url = f"http://{dbhost}:{dbport}"
        solar_client = InfluxDBClient(url=url, org="-", token=f"{uname}:{passw}")
        write_api = solar_client.write_api(write_options=SYNCHRONOUS)
        bucket = dbname

        def write_point(datapoint):
            p = Point(datapoint["measurement"])
            for k, v in datapoint.get("fields", {}).items():
                p = p.field(k, v)
            for k, v in datapoint.get("tags", {}).items():
                p = p.tag(k, v)
            if "time" in datapoint:
                p = p.time(datapoint["time"])
            write_api.write(bucket=bucket, record=p)
    except ClientConnectionError as e:
        logger.error(f'Error during connection to InfluxDb {dbhost}: {e}')
        return
    logger.info('Database opened and initialized')
    
    # Connect to the solaredge inverter
    client = ModbusClient(args.inverter_ip, port=args.inverter_port, unit_id=args.unitid, auto_open=True, timeout=10.0)

    async def read_regs(addr: int, count: int, section: str):
        """Read holding registers with labeled metrics and differentiated error handling."""
        # Measure request latency for this section
        with modbus_req_latency_sec.labels(section=section).time():
            rb = client.read_holding_registers(addr, count)
        if rb:
            return rb
        le = client.last_error
        if le == 3:  # send error
            modbus_send_errors.labels(section=section).inc()
            logger.error(f'Send error (write failed); section={section}. Reconnecting socket.')
            try:
                client.close()
            except Exception:
                pass
            await asyncio.sleep(period)
            return None
        if le == 4:  # receive error
            modbus_recv_errors.labels(section=section).inc()
            logger.warning(f'Receive error; section={section}. Retrying once on same socket.')
            with modbus_req_latency_sec.labels(section=section).time():
                rb = client.read_holding_registers(addr, count)
            if rb:
                return rb
            logger.error(f'Receive error persisted; section={section}. Reconnecting socket.')
            try:
                client.close()
            except Exception:
                pass
            await asyncio.sleep(period)
            return None
        if le == 5:  # timeout
            modbus_timeouts.labels(section=section).inc()
            logger.error(f'Timeout; section={section}. Reconnecting socket.')
            try:
                client.close()
            except Exception:
                pass
            await asyncio.sleep(period)
            return None
        # Unknown or unexpected error code
        logger.error(f'Unknown Modbus error last_error={le}; section={section}. Reconnecting socket.')
        try:
            client.close()
        except Exception:
            pass
        await asyncio.sleep(period)
        return None

    # Read the common blocks on the Inverter
    while True:
        reg_block = {}
        reg_block = await read_regs(40004, 65, "inv_info")
        if reg_block:
            InvManufacturer = client.convert_from_registers(reg_block[0:16], data_type="string", byteorder=">", wordorder=">")
            InvModel = client.convert_from_registers(reg_block[16:32], data_type="string", byteorder=">", wordorder=">")
            Invfoo = client.convert_from_registers(reg_block[32:40], data_type="string", byteorder=">", wordorder=">")
            InvVersion = client.convert_from_registers(reg_block[40:48], data_type="string", byteorder=">", wordorder=">")
            InvSerialNumber = client.convert_from_registers(reg_block[48:64], data_type="string", byteorder=">", wordorder=">")
            InvDeviceAddress = client.convert_from_registers([reg_block[64]], data_type="uint16", byteorder=">", wordorder=">")

            print('*' * 60)
            print('* Inverter Info')
            print('*' * 60)
            print(' Manufacturer: ' + InvManufacturer)
            print(' Model: ' + InvModel)
            print(' Version: ' + InvVersion)
            print(' Serial Number: ' + InvSerialNumber)
            print(' ModBus ID: ' + str(InvDeviceAddress))
            break
        else:
            continue

    # Read the common blocks on the meter/s (if present)
    connflag = False
    if mbmeters >= 1:
        while True:
            dictMeterLabel = []
            for x in range(1, mbmeters+1):
                reg_block = {}
                if x==1:
                    reg_block = await read_regs(40123, 65, f"meter_info_{x}")
                if x==2:
                    reg_block = await read_regs(40297, 65, f"meter_info_{x}")
                if x==3:
                    reg_block = await read_regs(40471, 65, f"meter_info_{x}")
                if reg_block:
                    MManufacturer = client.convert_from_registers(reg_block[0:16], data_type="string", byteorder=">", wordorder=">")
                    MModel = client.convert_from_registers(reg_block[16:32], data_type="string", byteorder=">", wordorder=">")
                    MOption = client.convert_from_registers(reg_block[32:40], data_type="string", byteorder=">", wordorder=">")
                    MVersion = client.convert_from_registers(reg_block[40:48], data_type="string", byteorder=">", wordorder=">")
                    MSerialNumber = client.convert_from_registers(reg_block[48:64], data_type="string", byteorder=">", wordorder=">")
                    MDeviceAddress = client.convert_from_registers([reg_block[64]], data_type="uint16", byteorder=">", wordorder=">")
                    fooLabel = MManufacturer.split('\x00')[0] + '(' + MSerialNumber.split('\x00')[0] + ')'
                    dictMeterLabel.append(fooLabel)
                    print('*' * 60)
                    print('* Meter ' + str(x) + ' Info')
                    print('*' * 60)
                    print(' Manufacturer: ' + MManufacturer)
                    print(' Model: ' + MModel)
                    print(' Mode: ' + MOption)
                    print(' Version: ' + MVersion)
                    print(' Serial Number: ' + MSerialNumber)
                    print(' ModBus ID: ' + str(MDeviceAddress))
                    if x==mbmeters:
                        print('*' * 60)
                    connflag = True
                else:
                    continue
            if connflag:
                break

    # Read the common blocks on the battery/s (if present)
    battflag = False
    if mbbatteries >= 1:
        while True:
            dictBattery = []
            for x in range(1, mbbatteries+1):
                reg_block = {}
                if x==1:
                    reg_block = await read_regs(57600, 76, f"battery_info_{x}")
                if x==2:
                    reg_block = await read_regs(57856, 76, f"battery_info_{x}")
                if reg_block:
                    BManufacturer = client.convert_from_registers(reg_block[0:16], data_type="string", byteorder=">", wordorder=">")
                    BModel = client.convert_from_registers(reg_block[16:32], data_type="string", byteorder=">", wordorder=">")
                    BVersion = client.convert_from_registers(reg_block[32:48], data_type="string", byteorder=">", wordorder=">")
                    BSerialNumber = client.convert_from_registers(reg_block[48:64], data_type="string", byteorder=">", wordorder=">")
                    BDeviceAddress = client.convert_from_registers([reg_block[64]], data_type="uint16", byteorder=">", wordorder=">")
                    # skip reg_block[65] (2 bytes)
                    BRatedEnergy = client.convert_from_registers(reg_block[66:68], data_type="float32", byteorder=">", wordorder=">")
                    BMaxChargePower = client.convert_from_registers(reg_block[68:70], data_type="float32", byteorder=">", wordorder=">")
                    BMaxDischargePower = client.convert_from_registers(reg_block[70:72], data_type="float32", byteorder=">", wordorder=">")
                    BMaxChargePeakPower = client.convert_from_registers(reg_block[72:74], data_type="float32", byteorder=">", wordorder=">")
                    BMaxDischargePeakPower = client.convert_from_registers(reg_block[74:76], data_type="float32", byteorder=">", wordorder=">")
                    fooLabel = BManufacturer.split('\x00')[0] + '(' + BSerialNumber.split('\x00')[0] + ')'
                    dictBattery.append(fooLabel)
                    print('*' * 60)
                    print('* Battery ' + str(x) + ' Info')
                    print('*' * 60)
                    print(' Manufacturer: ' + BManufacturer)
                    print(' Model: ' + BModel)
                    print(' Version: ' + BVersion)
                    print(' Serial Number: ' + BSerialNumber)
                    print(' ModBus ID: ' + str(BDeviceAddress))
                    print(' Rated Energy: ' + str(BRatedEnergy))
                    print(' Max Charge Power: ' + str(BMaxChargePower))
                    print(' Max Discharge Power: ' + str(BMaxDischargePower))
                    print(' Max Charge Peak Power: ' + str(BMaxChargePeakPower))
                    print(' Max Discharge Peak Power: ' + str(BMaxDischargePeakPower))
                    if x==mbbatteries:
                        print('*' * 60)
                    connflag = True
                else:
                    continue
            if connflag:
                break

    # Start the loop for collecting the metrics...
    while True:
        try:
            reg_block = {}
            dictInv = {}
            reg_block = await read_regs(40069, 50, "inverter")
            if reg_block:
                # print(reg_block)
                # reg_block[0] = Sun Spec DID
                # reg_block[1] = Length of Model Block
                # reg_block[2] = AC Total current value
                # reg_block[3] = AC Phase A current value
                # reg_block[4] = AC Phase B current value
                # reg_block[5] = AC Phase C current value
                # reg_block[6] = AC current scale factor
                # reg_block[7] = AC Phase A to B voltage value
                # reg_block[8] = AC Phase B to C voltage value
                # reg_block[9] = AC Phase C to A voltage value
                # reg_block[10] = AC Phase A to N voltage value
                # reg_block[11] = AC Phase B to N voltage value
                # reg_block[12] = AC Phase C to N voltage value
                # reg_block[13] = AC voltage scale factor
                # reg_block[14] = AC Power value
                # reg_block[15] = AC Power scale factor
                # reg_block[16] = AC Frequency value
                # reg_block[17] = AC Frequency scale factor
                # reg_block[18] = AC Apparent Power
                # reg_block[19] = AC Apparent Power scale factor
                # reg_block[20] = AC Reactive Power
                # reg_block[21] = AC Reactive Power scale factor
                # reg_block[22] = AC Power Factor
                # reg_block[23] = AC Power Factor scale factor
                # reg_block[24] = AC Lifetime Energy (HI bits)
                # reg_block[25] = AC Lifetime Energy (LO bits)
                # reg_block[26] = AC Lifetime Energy scale factor
                # reg_block[27] = DC Current value
                # reg_block[28] = DC Current scale factor
                # reg_block[29] = DC Voltage value
                # reg_block[30] = DC Voltage scale factor
                # reg_block[31] = DC Power value
                # reg_block[32] = DC Power scale factor
                # reg_block[34] = Inverter temp
                # reg_block[37] = Inverter temp scale factor
                # reg_block[38] = Inverter Operating State
                # reg_block[39] = Inverter Status Code
                datapoint = {
                    'measurement': 'Inverter',
                    'fields': {}
                }
                logger.debug(f'inverter reg_block: {str(reg_block)}')

                # SunSpec DID
                fooVal = trunc_float(client.convert_from_registers([reg_block[0]], data_type="uint16", byteorder=">", wordorder=">"))
                fooName = 'SunSpec_DID'
                dictInv[fooName] = fooVal if fooVal < 65535 else 0.0
                # SunSpec Length
                fooVal = trunc_float(client.convert_from_registers([reg_block[1]], data_type="uint16", byteorder=">", wordorder=">"))
                fooName = 'SunSpec_Length'
                dictInv[fooName] = fooVal if fooVal < 65535 else 0.0
                # AC Current scale factor
                ac_current_sf = client.convert_from_registers([reg_block[6]], data_type="int16", byteorder=">", wordorder=">")
                ac_current_sf = 10 ** ac_current_sf
                # AC Current
                fooVal = trunc_float(client.convert_from_registers([reg_block[2]], data_type="uint16", byteorder=">", wordorder=">"))
                fooName = 'AC_Current'
                dictInv[fooName] = fooVal * ac_current_sf if fooVal < 65535 else 0.0
                fooVal = trunc_float(client.convert_from_registers([reg_block[3]], data_type="uint16", byteorder=">", wordorder=">"))
                fooName = 'AC_CurrentA'
                dictInv[fooName] = fooVal * ac_current_sf if fooVal < 65535 else 0.0
                fooVal = trunc_float(client.convert_from_registers([reg_block[4]], data_type="uint16", byteorder=">", wordorder=">"))
                fooName = 'AC_CurrentB'
                dictInv[fooName] = fooVal * ac_current_sf if fooVal < 65535 else 0.0
                fooVal = trunc_float(client.convert_from_registers([reg_block[5]], data_type="uint16", byteorder=">", wordorder=">"))
                fooName = 'AC_CurrentC'
                dictInv[fooName] = fooVal * ac_current_sf if fooVal < 65535 else 0.0
                # AC Voltage scale factor
                ac_voltage_sf = client.convert_from_registers([reg_block[13]], data_type="int16", byteorder=">", wordorder=">")
                ac_voltage_sf = 10 ** ac_voltage_sf
                # AC Voltage
                fooVal = trunc_float(client.convert_from_registers([reg_block[7]], data_type="uint16", byteorder=">", wordorder=">"))
                fooName = 'AC_VoltageAB'
                dictInv[fooName] = fooVal * ac_voltage_sf if fooVal < 65535 else 0.0
                fooVal = trunc_float(client.convert_from_registers([reg_block[8]], data_type="uint16", byteorder=">", wordorder=">"))
                fooName = 'AC_VoltageBC'
                dictInv[fooName] = fooVal * ac_voltage_sf if fooVal < 65535 else 0.0
                fooVal = trunc_float(client.convert_from_registers([reg_block[9]], data_type="uint16", byteorder=">", wordorder=">"))
                fooName = 'AC_VoltageCA'
                dictInv[fooName] = fooVal * ac_voltage_sf if fooVal < 65535 else 0.0
                fooVal = trunc_float(client.convert_from_registers([reg_block[10]], data_type="uint16", byteorder=">", wordorder=">"))
                fooName = 'AC_VoltageAN'
                dictInv[fooName] = fooVal * ac_voltage_sf if fooVal < 65535 else 0.0
                fooVal = trunc_float(client.convert_from_registers([reg_block[11]], data_type="uint16", byteorder=">", wordorder=">"))
                fooName = 'AC_VoltageBN'
                dictInv[fooName] = fooVal * ac_voltage_sf if fooVal < 65535 else 0.0
                fooVal = trunc_float(client.convert_from_registers([reg_block[12]], data_type="uint16", byteorder=">", wordorder=">"))
                fooName = 'AC_VoltageCN'
                dictInv[fooName] = fooVal * ac_voltage_sf if fooVal < 65535 else 0.0
                # AC Power scale factor
                ac_power_sf = client.convert_from_registers([reg_block[15]], data_type="int16", byteorder=">", wordorder=">")
                ac_power_sf = 10 ** ac_power_sf
                # AC Power
                fooVal = trunc_float(client.convert_from_registers([reg_block[14]], data_type="int16", byteorder=">", wordorder=">"))
                fooName = 'AC_Power'
                dictInv[fooName] = fooVal * ac_power_sf if fooVal < 65535 else 0.0
                # AC Frequency scale factor
                ac_freq_sf = client.convert_from_registers([reg_block[17]], data_type="int16", byteorder=">", wordorder=">")
                ac_freq_sf = 10 ** ac_freq_sf
                # AC Frequency
                fooVal = trunc_float(client.convert_from_registers([reg_block[16]], data_type="uint16", byteorder=">", wordorder=">"))
                fooName = 'AC_Frequency'
                dictInv[fooName] = fooVal * ac_freq_sf if fooVal < 65535 else 0.0
                # AC Apparent Power scale factor
                ac_va_sf = client.convert_from_registers([reg_block[19]], data_type="int16", byteorder=">", wordorder=">")
                ac_va_sf = 10 ** ac_va_sf
                # AC Apparent Power
                fooVal = trunc_float(client.convert_from_registers([reg_block[18]], data_type="int16", byteorder=">", wordorder=">"))
                fooName = 'AC_VA'
                dictInv[fooName] = fooVal * ac_va_sf if fooVal < 65535 else 0.0
                # AC Reactive Power scale factor
                ac_var_sf = client.convert_from_registers([reg_block[21]], data_type="int16", byteorder=">", wordorder=">")
                ac_var_sf = 10 ** ac_var_sf
                # AC Reactive Power
                fooVal = trunc_float(client.convert_from_registers([reg_block[20]], data_type="int16", byteorder=">", wordorder=">"))
                fooName = 'AC_VAR'
                dictInv[fooName] = fooVal * ac_var_sf if fooVal < 65535 else 0.0
                # AC Power Factor scale factor
                ac_pf_sf = client.convert_from_registers([reg_block[23]], data_type="int16", byteorder=">", wordorder=">")
                ac_pf_sf = 10 ** ac_pf_sf
                # AC Power Factor
                fooVal = trunc_float(client.convert_from_registers([reg_block[22]], data_type="int16", byteorder=">", wordorder=">"))
                fooName = 'AC_PF'
                dictInv[fooName] = fooVal * ac_pf_sf if fooVal < 65535 else 0.0
                # AC Lifetime Energy scale factor
                ac_wh_sf = client.convert_from_registers([reg_block[26]], data_type="uint16", byteorder=">", wordorder=">")
                ac_wh_sf = 10 ** ac_wh_sf
                # AC Lifetime Energy Production
                fooVal = trunc_float(client.convert_from_registers(reg_block[24:26], data_type="uint32", byteorder=">", wordorder=">"))
                fooName = 'AC_Energy_WH'
                dictInv[fooName] = fooVal * ac_wh_sf if fooVal < 4294967295 else 0.0
                # DC Current scale factor
                dc_current_sf = client.convert_from_registers([reg_block[28]], data_type="int16", byteorder=">", wordorder=">")
                dc_current_sf = 10 ** dc_current_sf
                # DC Current
                fooVal = trunc_float(client.convert_from_registers([reg_block[27]], data_type="uint16", byteorder=">", wordorder=">"))
                fooName = 'DC_Current'
                dictInv[fooName] = fooVal * dc_current_sf if fooVal < 65535 else 0.0
                # DC Voltage scale factor
                dc_voltage_sf = client.convert_from_registers([reg_block[30]], data_type="int16", byteorder=">", wordorder=">")
                dc_voltage_sf = 10 ** dc_voltage_sf
                # DC Voltage
                fooVal = trunc_float(client.convert_from_registers([reg_block[29]], data_type="uint16", byteorder=">", wordorder=">"))
                fooName = 'DC_Voltage'
                dictInv[fooName] = fooVal * dc_voltage_sf if fooVal < 65535 else 0.0
                # DC Power scale factor
                dc_power_sf = client.convert_from_registers([reg_block[32]], data_type="int16", byteorder=">", wordorder=">")
                dc_power_sf = 10 ** dc_power_sf
                # DC Power
                fooVal = trunc_float(client.convert_from_registers([reg_block[31]], data_type="int16", byteorder=">", wordorder=">"))
                fooName = 'DC_Power'
                dictInv[fooName] = fooVal * dc_power_sf if fooVal < 65535 else 0.0
                # Inverter temp scale factor
                temp_sf = client.convert_from_registers([reg_block[37]], data_type="int16", byteorder=">", wordorder=">")
                temp_sf = 10 ** temp_sf
                # Inverter temp
                fooVal = trunc_float(client.convert_from_registers([reg_block[34]], data_type="int16", byteorder=">", wordorder=">"))
                fooName = 'Temp_Sink'
                dictInv[fooName] = fooVal * temp_sf if fooVal < 65535 else 0.0
                # Inverter Operating State
                fooVal = trunc_float(client.convert_from_registers([reg_block[38]], data_type="uint16", byteorder=">", wordorder=">"))
                fooName = 'Status'
                dictInv[fooName] = fooVal if fooVal < 65535 else 0.0
                # Inverter Status Code
                fooVal = trunc_float(client.convert_from_registers([reg_block[39]], data_type="uint16", byteorder=">", wordorder=">"))
                fooName = 'Status_Vendor'
                dictInv[fooName] = fooVal if fooVal < 65535 else 0.0
                # Adding the ScaleFactor elements
                dictInv['AC_Current_SF'] = 0.0
                dictInv['AC_Voltage_SF'] = 0.0
                dictInv['AC_Power_SF'] = 0.0
                dictInv['AC_Frequency_SF'] = 0.0
                dictInv['AC_VA_SF'] = 0.0
                dictInv['AC_VAR_SF'] = 0.0
                dictInv['AC_PF_SF'] = 0.0
                dictInv['AC_Energy_WH_SF'] = 0.0
                dictInv['DC_Current_SF'] = 0.0
                dictInv['DC_Voltage_SF'] = 0.0
                dictInv['DC_Power_SF'] = 0.0
                dictInv['Temp_SF'] = 0.0
                logger.debug(f'Inverter')
                for j, k in dictInv.items():
                    logger.debug(f'  {j}: {k}')
                publish_metrics(dictInv, 'inverter', '')
                logger.debug('Done publishing inverter metrics...')
                datapoint['time'] = str(datetime.datetime.now(datetime.UTC).isoformat())
                logger.debug(f'Writing to Influx: {str(datapoint)}')
                try:
                    write_point(datapoint)
                    logger.info('Wrote Inverter datapoint to Influx.')
                except Exception as e:
                    logger.error(f'Failed to write inverter data to InfluxDB: {e}')

            else:
                continue
                    
            for x in range(1, mbmeters+1):
                # Now loop through this for each meter that is attached.
                logger.debug(f'Meter={str(x)}')
                reg_block = {}
                dictM = {}

                # Start point is different for each meter
                if x==1:
                    reg_block = await read_regs(40188, 105, f"meter{x}")
                if x==2:
                    reg_block = await read_regs(40362, 105, f"meter{x}")
                if x==3:
                    reg_block = await read_regs(40537, 105, f"meter{x}")
                # Guard for meter labels
                if not dictMeterLabel or len(dictMeterLabel) < x:
                    logger.error(f'Meter labels not initialized for meter {x}; skipping this read.')
                    continue
                if reg_block:
                    # print(reg_block)
                    # reg_block[0] = AC Total current value
                    # reg_block[1] = AC Phase A current value
                    # reg_block[2] = AC Phase B current value
                    # reg_block[3] = AC Phase C current value
                    # reg_block[4] = AC current scale factor
                    # reg_block[5] = AC Phase Line (average) to N voltage value
                    # reg_block[6] = AC Phase A to N voltage value
                    # reg_block[7] = AC Phase B to N voltage value
                    # reg_block[8] = AC Phase C to N voltage value
                    # reg_block[9] = AC Phase Line to Line voltage value
                    # reg_block[10] = AC Phase A to B voltage value
                    # reg_block[11] = AC Phase B to C voltage value
                    # reg_block[12] = AC Phase C to A voltage value
                    # reg_block[13] = AC voltage scale factor
                    # reg_block[14] = AC Frequency value
                    # reg_block[15] = AC Frequency scale factor
                    # reg_block[16] = Total Real Power
                    # reg_block[17] = Phase A Real Power
                    # reg_block[18] = Phase B Real Power
                    # reg_block[19] = Phase C Real Power
                    # reg_block[20] = Real Power scale factor
                    # reg_block[21] = Total Apparent Power
                    # reg_block[22] = Phase A Apparent Power
                    # reg_block[23] = Phase B Apparent Power
                    # reg_block[24] = Phase C Apparent Power
                    # reg_block[25] = Apparent Power scale factor
                    # reg_block[26] = Total Reactive Power
                    # reg_block[27] = Phase A Reactive Power
                    # reg_block[28] = Phase B Reactive Power
                    # reg_block[29] = Phase C Reactive Power
                    # reg_block[30] = Reactive Power scale factor
                    # reg_block[31] = Average Power Factor
                    # reg_block[32] = Phase A Power Factor
                    # reg_block[33] = Phase B Power Factor
                    # reg_block[34] = Phase C Power Factor
                    # reg_block[35] = Power Factor scale factor
                    # reg_block[36] = Total Exported Real Energy
                    # reg_block[38] = Phase A Exported Real Energy
                    # reg_block[40] = Phase B Exported Real Energy
                    # reg_block[42] = Phase C Exported Real Energy
                    # reg_block[44] = Total Imported Real Energy
                    # reg_block[46] = Phase A Imported Real Energy
                    # reg_block[48] = Phase B Imported Real Energy
                    # reg_block[50] = Phase C Imported Real Energy
                    # reg_block[52] = Real Energy scale factor
                    # reg_block[53] = Total Exported Real Energy
                    # reg_block[55] = Phase A Exported Real Energy
                    # reg_block[57] = Phase B Exported Real Energy
                    # reg_block[59] = Phase C Exported Real Energy
                    # reg_block[61] = Total Imported Real Energy
                    # reg_block[63] = Phase A Imported Real Energy
                    # reg_block[65] = Phase B Imported Real Energy
                    # reg_block[67] = Phase C Imported Real Energy
                    # reg_block[69] = Real Energy scale factor
                    logger.debug(f'meter reg_block: {str(reg_block)}')
                
                    # Set the Label to use for the Meter Metrics for Prometheus
                    metriclabel = dictMeterLabel[x-1]
                    # Clear data from inverter, otherwise we publish that again!
                    datapoint = {
                        'measurement': dictMeterLabel[x-1],
                        'fields': {}
                    }
                    
                    # SunSpec DID
                    fooVal = trunc_float(client.convert_from_registers([reg_block[0]], data_type="uint16", byteorder=">", wordorder=">"))
                    fooName = 'M_SunSpec_DID'
                    dictM[fooName] = fooVal if fooVal < 65535 else 0.0
                    # SunSpec Length
                    fooVal = trunc_float(client.convert_from_registers([reg_block[1]], data_type="uint16", byteorder=">", wordorder=">"))
                    fooName = 'M_SunSpec_Length'
                    dictM[fooName] = fooVal if fooVal < 65535 else 0.0
                    # AC Current scale factor
                    m_ac_current_sf = client.convert_from_registers([reg_block[4]], data_type="int16", byteorder=">", wordorder=">")
                    m_ac_current_sf = 10 ** m_ac_current_sf
                    # AC Current
                    fooVal = trunc_float(client.convert_from_registers([reg_block[2]], data_type="int16", byteorder=">", wordorder=">"))
                    fooName = 'M_AC_Current'
                    dictM[fooName] = fooVal * m_ac_current_sf if fooVal < 32768 else 0.0
                    fooVal = trunc_float(client.convert_from_registers([reg_block[3]], data_type="int16", byteorder=">", wordorder=">"))
                    fooName = 'M_AC_CurrentA'
                    dictM[fooName] = fooVal * m_ac_current_sf if fooVal < 32768 else 0.0
                    fooVal = trunc_float(client.convert_from_registers([reg_block[4]], data_type="int16", byteorder=">", wordorder=">"))
                    fooName = 'M_AC_CurrentB'
                    dictM[fooName] = fooVal * m_ac_current_sf if fooVal < 32768 else 0.0
                    fooVal = trunc_float(client.convert_from_registers([reg_block[5]], data_type="int16", byteorder=">", wordorder=">"))
                    fooName = 'M_AC_CurrentC'
                    dictM[fooName] = fooVal * m_ac_current_sf if fooVal < 32768 else 0.0
                    # AC Voltage scale factor
                    m_ac_voltage_sf = client.convert_from_registers([reg_block[13]], data_type="int16", byteorder=">", wordorder=">")
                    m_ac_voltage_sf = 10 ** m_ac_voltage_sf
                    # AC Voltage
                    fooVal = trunc_float(client.convert_from_registers([reg_block[5]], data_type="int16", byteorder=">", wordorder=">"))
                    fooName = 'M_AC_VoltageLN'
                    dictM[fooName] = fooVal * m_ac_voltage_sf if fooVal < 32768 else 0.0
                    fooVal = trunc_float(client.convert_from_registers([reg_block[6]], data_type="int16", byteorder=">", wordorder=">"))
                    fooName = 'M_AC_VoltageAN'
                    dictM[fooName] = fooVal * m_ac_voltage_sf if fooVal < 32768 else 0.0
                    fooVal = trunc_float(client.convert_from_registers([reg_block[7]], data_type="int16", byteorder=">", wordorder=">"))
                    fooName = 'M_AC_VoltageBN'
                    dictM[fooName] = fooVal * m_ac_voltage_sf if fooVal < 32768 else 0.0
                    fooVal = trunc_float(client.convert_from_registers([reg_block[8]], data_type="int16", byteorder=">", wordorder=">"))
                    fooName = 'M_AC_VoltageCN'
                    dictM[fooName] = fooVal * m_ac_voltage_sf if fooVal < 32768 else 0.0
                    fooVal = trunc_float(client.convert_from_registers([reg_block[9]], data_type="int16", byteorder=">", wordorder=">"))
                    fooName = 'M_AC_VoltageLL'
                    dictM[fooName] = fooVal * m_ac_voltage_sf if fooVal < 32768 else 0.0
                    fooVal = trunc_float(client.convert_from_registers([reg_block[10]], data_type="int16", byteorder=">", wordorder=">"))
                    fooName = 'M_AC_VoltageAB'
                    dictM[fooName] = fooVal * m_ac_voltage_sf if fooVal < 32768 else 0.0
                    fooVal = trunc_float(client.convert_from_registers([reg_block[11]], data_type="int16", byteorder=">", wordorder=">"))
                    fooName = 'M_AC_VoltageBC'
                    dictM[fooName] = fooVal * m_ac_voltage_sf if fooVal < 32768 else 0.0
                    fooVal = trunc_float(client.convert_from_registers([reg_block[12]], data_type="int16", byteorder=">", wordorder=">"))
                    fooName = 'M_AC_VoltageCA'
                    dictM[fooName] = fooVal * m_ac_voltage_sf if fooVal < 32768 else 0.0
                    # AC Frequency scale factor
                    m_ac_freq_sf = client.convert_from_registers([reg_block[15]], data_type="int16", byteorder=">", wordorder=">")
                    m_ac_freq_sf = 10 ** m_ac_freq_sf
                    # AC Frequency
                    fooVal = trunc_float(client.convert_from_registers([reg_block[14]], data_type="int16", byteorder=">", wordorder=">"))
                    fooName = 'M_AC_Frequency'
                    dictM[fooName] = fooVal * m_ac_freq_sf if fooVal < 32768 else 0.0
                    # AC Real Power scale factor
                    m_ac_power_sf = client.convert_from_registers([reg_block[20]], data_type="int16", byteorder=">", wordorder=">")
                    m_ac_power_sf = 10 ** m_ac_power_sf
                    # AC Real Power
                    fooVal = trunc_float(client.convert_from_registers([reg_block[16]], data_type="int16", byteorder=">", wordorder=">"))
                    fooName = 'M_AC_Power'
                    dictM[fooName] = fooVal * m_ac_power_sf if fooVal < 32768 else 0.0
                    fooVal = trunc_float(client.convert_from_registers([reg_block[17]], data_type="int16", byteorder=">", wordorder=">"))
                    fooName = 'M_AC_Power_A'
                    dictM[fooName] = fooVal * m_ac_power_sf if fooVal < 32768 else 0.0
                    fooVal = trunc_float(client.convert_from_registers([reg_block[18]], data_type="int16", byteorder=">", wordorder=">"))
                    fooName = 'M_AC_Power_B'
                    dictM[fooName] = fooVal * m_ac_power_sf if fooVal < 32768 else 0.0
                    fooVal = trunc_float(client.convert_from_registers([reg_block[19]], data_type="int16", byteorder=">", wordorder=">"))
                    fooName = 'M_AC_Power_C'
                    dictM[fooName] = fooVal * m_ac_power_sf if fooVal < 32768 else 0.0
                    # AC Apparent Power scale factor
                    m_ac_va_sf = client.convert_from_registers([reg_block[25]], data_type="int16", byteorder=">", wordorder=">")
                    m_ac_va_sf = 10 ** m_ac_va_sf
                    # AC Apparent Power
                    fooVal = trunc_float(client.convert_from_registers([reg_block[21]], data_type="int16", byteorder=">", wordorder=">"))
                    fooName = 'M_AC_VA'
                    dictM[fooName] = fooVal * m_ac_va_sf if fooVal < 32768 else 0.0
                    fooVal = trunc_float(client.convert_from_registers([reg_block[22]], data_type="int16", byteorder=">", wordorder=">"))
                    fooName = 'M_AC_VA_A'
                    dictM[fooName] = fooVal * m_ac_va_sf if fooVal < 32768 else 0.0
                    fooVal = trunc_float(client.convert_from_registers([reg_block[23]], data_type="int16", byteorder=">", wordorder=">"))
                    fooName = 'M_AC_VA_B'
                    dictM[fooName] = fooVal * m_ac_va_sf if fooVal < 32768 else 0.0
                    fooVal = trunc_float(client.convert_from_registers([reg_block[24]], data_type="int16", byteorder=">", wordorder=">"))
                    fooName = 'M_AC_VA_C'
                    dictM[fooName] = fooVal * m_ac_va_sf if fooVal < 32768 else 0.0
                    # AC Reactive Power scale factor
                    m_ac_var_sf = client.convert_from_registers([reg_block[30]], data_type="int16", byteorder=">", wordorder=">")
                    m_ac_var_sf = 10 ** m_ac_var_sf
                    # AC Reactive Power
                    fooVal = trunc_float(client.convert_from_registers([reg_block[26]], data_type="int16", byteorder=">", wordorder=">"))
                    fooName = 'M_AC_VAR'
                    dictM[fooName] = fooVal * m_ac_var_sf if fooVal < 32768 else 0.0
                    fooVal = trunc_float(client.convert_from_registers([reg_block[27]], data_type="int16", byteorder=">", wordorder=">"))
                    fooName = 'M_AC_VAR_A'
                    dictM[fooName] = fooVal * m_ac_var_sf if fooVal < 32768 else 0.0
                    fooVal = trunc_float(client.convert_from_registers([reg_block[28]], data_type="int16", byteorder=">", wordorder=">"))
                    fooName = 'M_AC_VAR_B'
                    dictM[fooName] = fooVal * m_ac_var_sf if fooVal < 32768 else 0.0
                    fooVal = trunc_float(client.convert_from_registers([reg_block[29]], data_type="int16", byteorder=">", wordorder=">"))
                    fooName = 'M_AC_VAR_C'
                    dictM[fooName] = fooVal * m_ac_var_sf if fooVal < 32768 else 0.0
                    # AC Power Factor scale factor
                    m_ac_pf_sf = client.convert_from_registers([reg_block[35]], data_type="int16", byteorder=">", wordorder=">")
                    m_ac_pf_sf = 10 ** m_ac_pf_sf
                    # AC Power Factor
                    fooVal = trunc_float(client.convert_from_registers([reg_block[31]], data_type="int16", byteorder=">", wordorder=">"))
                    fooName = 'M_AC_PF'
                    dictM[fooName] = fooVal * m_ac_pf_sf if fooVal < 32768 else 0.0
                    fooVal = trunc_float(client.convert_from_registers([reg_block[32]], data_type="int16", byteorder=">", wordorder=">"))
                    fooName = 'M_AC_PF_A'
                    dictM[fooName] = fooVal * m_ac_pf_sf if fooVal < 32768 else 0.0
                    fooVal = trunc_float(client.convert_from_registers([reg_block[33]], data_type="int16", byteorder=">", wordorder=">"))
                    fooName = 'M_AC_PF_B'
                    dictM[fooName] = fooVal * m_ac_pf_sf if fooVal < 32768 else 0.0
                    fooVal = trunc_float(client.convert_from_registers([reg_block[34]], data_type="int16", byteorder=">", wordorder=">"))
                    fooName = 'M_AC_PF_C'
                    dictM[fooName] = fooVal * m_ac_pf_sf if fooVal < 32768 else 0.0
                    # Real Energy scale factor
                    m_energy_sf = client.convert_from_registers([reg_block[52]], data_type="int16", byteorder=">", wordorder=">")
                    m_energy_sf = 10 ** m_energy_sf
                    # Accumulated AC Real Energy
                    fooVal = trunc_float(client.convert_from_registers(reg_block[36:38], data_type="uint32", byteorder=">", wordorder=">"))
                    fooName = 'M_Exported'
                    dictM[fooName] = fooVal * m_energy_sf if fooVal < 4294967295 else 0.0
                    fooVal = trunc_float(client.convert_from_registers(reg_block[38:40], data_type="uint32", byteorder=">", wordorder=">"))
                    fooName = 'M_Exported_A'
                    dictM[fooName] = fooVal * m_energy_sf if fooVal < 4294967295 else 0.0
                    fooVal = trunc_float(client.convert_from_registers(reg_block[40:42], data_type="uint32", byteorder=">", wordorder=">"))
                    fooName = 'M_Exported_B'
                    dictM[fooName] = fooVal * m_energy_sf if fooVal < 4294967295 else 0.0
                    fooVal = trunc_float(client.convert_from_registers(reg_block[42:44], data_type="uint32", byteorder=">", wordorder=">"))
                    fooName = 'M_Exported_C'
                    dictM[fooName] = fooVal * m_energy_sf if fooVal < 4294967295 else 0.0
                    fooVal = trunc_float(client.convert_from_registers(reg_block[44:46], data_type="uint32", byteorder=">", wordorder=">"))
                    fooName = 'M_Imported'
                    dictM[fooName] = fooVal * m_energy_sf if fooVal < 4294967295 else 0.0
                    fooVal = trunc_float(client.convert_from_registers(reg_block[46:48], data_type="uint32", byteorder=">", wordorder=">"))
                    fooName = 'M_Imported_A'
                    dictM[fooName] = fooVal * m_energy_sf if fooVal < 4294967295 else 0.0
                    fooVal = trunc_float(client.convert_from_registers(reg_block[48:50], data_type="uint32", byteorder=">", wordorder=">"))
                    fooName = 'M_Imported_B'
                    dictM[fooName] = fooVal * m_energy_sf if fooVal < 4294967295 else 0.0
                    fooVal = trunc_float(client.convert_from_registers(reg_block[50:52], data_type="uint32", byteorder=">", wordorder=">"))
                    fooName = 'M_Imported_C'
                    dictM[fooName] = fooVal * m_energy_sf if fooVal < 4294967295 else 0.0
                    # Add the ScaleFactor elements
                    dictM['M_AC_Current_SF'] = 0.0
                    dictM['M_AC_Voltage_SF'] = 0.0
                    dictM['M_AC_Frequency_SF'] = 0.0
                    dictM['M_AC_Power_SF'] = 0.0
                    dictM['M_AC_VA_SF'] = 0.0
                    dictM['M_AC_VAR_SF'] = 0.0
                    dictM['M_AC_PF_SF'] = 0.0
                    dictM['M_Energy_W_SF'] = 0.0
                    publish_metrics(dictM, 'meter', metriclabel, x, legacysupport)
                    datapoint['time'] = str(datetime.datetime.now(datetime.UTC).isoformat())
                    logger.debug(f'Meter: {metriclabel}')
                    for j, k in dictM.items():
                        logger.debug(f'  {j}: {k}')
                    logger.debug(f'Writing to Influx: {str(datapoint)}')
                    try:
                        write_point(datapoint)
                        logger.info(f'Wrote Meter {x} ({metriclabel}) datapoint to Influx.')
                    except Exception as e:
                        logger.error(f'Failed to write meter {x} ({metriclabel}) data to InfluxDB: {e}')

                else:
                    continue
     
            for x in range(1, mbbatteries+1):
                # Now loop through this for each battery that is attached.
                logger.debug(f'Battery={str(x)}')
                reg_block = {}
                dictB = {}

                # Start point is different for each battery
                if x==1:
                    reg_block = await read_regs(57666, 72, f"battery{x}")
                if x==2:
                    reg_block = await read_regs(57922, 72, f"battery{x}")
                if reg_block:
                    # print(reg_block)
                    # reg_block[0] = Battery Rated Energy
                    # reg_block[1] = Battery Max Charge Continues Power
                    # reg_block[2] = Batter Max Discharge Continues Power
                    # reg_block[3] = Batter Max Charge Peak Power
                    # reg_block[4] = Batter Max Discharge Peak Power
                    # reg_block[5] = Reserved
                    # reg_block[6] = Reserved
                    # reg_block[7] = Reserved
                    # reg_block[8] = Reserved
                    # reg_block[9] = Reserved
                    # reg_block[10] = Reserved
                    # reg_block[11] = Reserved
                    # reg_block[12] = Reserved
                    # reg_block[13] = Reserved
                    # reg_block[14] = Reserved
                    # reg_block[15] = Reserved
                    # reg_block[16] = Reserved
                    # reg_block[17] = Reserved
                    # reg_block[18] = Reserved
                    # reg_block[19] = Reserved
                    # reg_block[20] = Reserved
                    # reg_block[21] = Batter Average Temperature
                    # reg_block[22] = Batter Max Temperature
                    # reg_block[23] = Batter Instantaneous Voltage
                    # reg_block[24] = Batter Instantaneous Current
                    # reg_block[25] = Batter Instantaneous Power
                    # reg_block[26] = Batter Lifetime Export Energy Counter
                    # reg_block[27] = Batter Lifetime Export Energy Counter
                    # reg_block[28] = Batter Lifetime Import Energy Counter
                    # reg_block[29] = Batter Lifetime Import Energy Counter
                    # reg_block[30] = Batter Max Energy
                    # reg_block[31] = Batter Available Energy
                    # reg_block[32] = Batter State of Health
                    # reg_block[33] = Batter State of Energy
                    # reg_block[34] = Batter Status
                    # reg_block[35] = Batter Status Internal
                    # ... ignore the rest
                    # reg_block[36] = Batter Events Log
                    # reg_block[38] = Batter Events Log
                    # reg_block[40] = Batter Events Log
                    # reg_block[42] = Batter Events Log
                    # reg_block[44] = Batter Events Log Internal
                    # reg_block[46] = Batter Events Log Internal
                    # reg_block[48] = Batter Events Log Internal
                    # reg_block[50] = Batter Events Log Internal
                    # reg_block[52] = Reserved....
                    logger.debug(f'meter reg_block: {str(reg_block)}')
                
                    # Set the Label to use for the Battery Metrics for Prometheus
                    metriclabel = dictBattery[x-1]
                    # Clear data from inverter, otherwise we publish that again!
                    datapoint = {
                        'measurement': dictBattery[x-1],
                        'fields': {}
                    }
                    
                    # Battery Rated Energy
                    fooVal = client.convert_from_registers(reg_block[0:2], data_type="float32", byteorder=">", wordorder=">")
                    fooName = 'B_Rated_Energy'
                    dictB[fooName] = fooVal
                    # Battery Max Charge Continues Power
                    fooVal = client.convert_from_registers(reg_block[2:4], data_type="float32", byteorder=">", wordorder=">")
                    fooName = 'B_Max_Charge_Continues_Power'
                    dictB[fooName] = fooVal
                    # Battery Max Discharge Continues Power
                    fooVal = client.convert_from_registers(reg_block[4:6], data_type="float32", byteorder=">", wordorder=">")
                    fooName = 'B_Max_Discharge_Continues_Power'
                    dictB[fooName] = fooVal
                    # Battery Max Charge Peak Power
                    fooVal = client.convert_from_registers(reg_block[6:8], data_type="float32", byteorder=">", wordorder=">")
                    fooName = 'B_Max_Charge_Peak_Power'
                    dictB[fooName] = fooVal
                    # Battery Max Discharge Peak Power
                    fooVal = client.convert_from_registers(reg_block[8:10], data_type="float32", byteorder=">", wordorder=">")
                    fooName = 'B_Max_Discharge_Peak_Power'
                    dictB[fooName] = fooVal
                    # Battery Average Temperature
                    fooVal = client.convert_from_registers(reg_block[21:23], data_type="float32", byteorder=">", wordorder=">")
                    fooName = 'B_Average_Temperature'
                    dictB[fooName] = fooVal
                    # Battery Max Temperature
                    fooVal = client.convert_from_registers(reg_block[23:25], data_type="float32", byteorder=">", wordorder=">")
                    fooName = 'B_Max_Temperature'
                    dictB[fooName] = fooVal
                    # Battery Instantaneous Voltage
                    fooVal = client.convert_from_registers(reg_block[25:27], data_type="float32", byteorder=">", wordorder=">")
                    fooName = 'B_Instantaneous_Voltage'
                    dictB[fooName] = fooVal
                    # Battery Instantaneous Current
                    fooVal = client.convert_from_registers(reg_block[27:29], data_type="float32", byteorder=">", wordorder=">")
                    fooName = 'B_Instantaneous_Current'
                    dictB[fooName] = fooVal
                    # Battery Instantaneous Power
                    fooVal = client.convert_from_registers(reg_block[29:31], data_type="float32", byteorder=">", wordorder=">")
                    fooName = 'B_Instantaneous_Power'
                    dictB[fooName] = fooVal
                    # Battery Lifetime Export Energy Counter
                    fooVal = client.convert_from_registers(reg_block[31:35], data_type="uint64", byteorder=">", wordorder=">")
                    fooName = 'B_Lifetime_Export_Energy_Counter'
                    dictB[fooName] = fooVal
                    # Battery Lifetime Import Energy Counter
                    fooVal = client.convert_from_registers(reg_block[35:39], data_type="uint64", byteorder=">", wordorder=">")
                    fooName = 'B_Lifetime_Import_Energy_Counter'
                    dictB[fooName] = fooVal
                    # Battery Max Energy
                    fooVal = client.convert_from_registers(reg_block[39:41], data_type="float32", byteorder=">", wordorder=">")
                    fooName = 'B_Max_Energy'
                    dictB[fooName] = fooVal
                    # Battery Available Energy
                    fooVal = client.convert_from_registers(reg_block[41:43], data_type="float32", byteorder=">", wordorder=">")
                    fooName = 'B_Available_Energy'
                    dictB[fooName] = fooVal
                    # Battery State of Health
                    fooVal = client.convert_from_registers(reg_block[43:45], data_type="float32", byteorder=">", wordorder=">")
                    fooName = 'B_State_of_Health'
                    dictB[fooName] = fooVal
                    # Battery State of Energy
                    fooVal = client.convert_from_registers(reg_block[45:47], data_type="float32", byteorder=">", wordorder=">")
                    fooName = 'B_State_of_Energy'
                    dictB[fooName] = fooVal
                    # Battery Status
                    fooVal = client.convert_from_registers(reg_block[47:49], data_type="uint32", byteorder=">", wordorder=">")
                    fooName = 'B_Status'
                    dictB[fooName] = fooVal
                    # Battery Status Internal
                    fooVal = client.convert_from_registers(reg_block[49:51], data_type="uint32", byteorder=">", wordorder=">")
                    fooName = 'B_Status_Internal'
                    dictB[fooName] = fooVal
                    publish_metrics(dictB, 'battery', metriclabel, x, legacysupport)
                    datapoint['time'] = str(datetime.datetime.now(datetime.UTC).isoformat())
                    logger.debug(f'Battery: {metriclabel}')
                    for j, k in dictB.items():
                        logger.debug(f'  {j}: {k}')
                    logger.debug(f'Writing to Influx: {str(datapoint)}')
                    try:
                        write_point(datapoint)
                        logger.info(f'Wrote Battery {x} ({metriclabel}) datapoint to Influx.')
                    except Exception as e:
                        logger.error(f'Failed to write battery {x} ({metriclabel}) data to InfluxDB: {e}')

                else:
                    continue
           
        # InfluxDBWriteError no longer exists; remove this except block
        except IOError as e:
            logger.error(f'I/O exception during operation: {e}')
        except Exception as e:
            logger.error(f'Unhandled exception: {e}')

        await asyncio.sleep(period)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--influx_server', default='localhost')
    parser.add_argument('--influx_port', type=int, default=8086)
    parser.add_argument('--influx_database', default='solaredge')
    parser.add_argument('--influx_user', help='InfluxDB username')
    parser.add_argument('--influx_pass', help='InfluxDB password')
    parser.add_argument('--inverter_port', type=int, default=502, help='ModBus TCP port number to use')
    parser.add_argument('--unitid', type=int, default=1, help='ModBus unit id to use in communication')
    parser.add_argument('--meters', type=int, default=0, help='Number of ModBus meters attached to inverter (0-3)')
    parser.add_argument('--batteries', type=int, default=0, help='Number of ModBus batteries attached to inverter (0-2)')
    parser.add_argument('--prometheus_exporter_port', type=int, default=2112, help='Port on which the prometheus exporter will listen on')
    parser.add_argument('--interval', type=int, default=5, help='Time (seconds) between polling')
    parser.add_argument('--legacy_support', type=bool, default=False, help='Set to true so Meter 1 prometheus metrics start with "M_" vs "M1_"')
    parser.add_argument('inverter_ip', metavar='SolarEdge IP', help='IP address of the SolarEdge inverter to monitor')
    parser.add_argument('--debug', '-d', action='count')
    args = parser.parse_args()

    logging.basicConfig()
    if args.debug and args.debug >= 1:
        logging.getLogger('solaredge').setLevel(logging.DEBUG)

    print(f'*' * 60)
    print(f'* Starting parameters')
    print(f'*' * 60)
    print(f'Inverter:\tAddress: {args.inverter_ip}\n\t\tPort: {args.inverter_port}\n\t\tID: {args.unitid}')
    print(f'Meters:\t\t{args.meters}')
    print(f'InfluxDB:\tServer: {args.influx_server}:{args.influx_port}\n\t\tDatabase: {args.influx_database}')
    print(f'Prometheus:\tExporter Port: {args.prometheus_exporter_port}\n')
    print(f'Legacy Support:\t{args.legacy_support}\n')
    logger.debug(f'Starting Prometheus exporter on port {args.prometheus_exporter_port}...')
    start_http_server(args.prometheus_exporter_port)
    #define_prometheus_metrics(args.meters)
    logger.debug('Running eventloop')
    asyncio.get_event_loop().run_until_complete(write_to_influx(args.influx_server, args.influx_port, args.meters, args.batteries, args.interval, args.influx_database, args.legacy_support, args.influx_user, args.influx_pass))
