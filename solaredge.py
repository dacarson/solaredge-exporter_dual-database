#!/usr/bin/env python3
import argparse
import datetime
import logging

from aiohttp import ClientConnectionError
from pyModbusTCP.client import ModbusClient
from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadDecoder
import asyncio
from aioinflux import InfluxDBClient, InfluxDBWriteError
from prometheus_client import Gauge
from prometheus_client import start_http_server

datapoint = {
    'measurement': 'SolarEdge',
    'tags': {},
    'fields': {}
}
reg_block = {}
promInv = {}
promMeter = {}
logger = logging.getLogger('solaredge')

############################################################

def publish_metrics(dictobj, objtype, metriclabel, meternum=0, legacysupport=False):

    global datapoint

    if objtype == 'inverter':
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


async def write_to_influx(dbhost, dbport, mbmeters, period, dbname, legacysupport):
    global client
    global datapoint
    global reg_block
    global promInv
    global promMeter

    def trunc_float(floatval):
        return float('%.2f' % floatval)

    try:
        solar_client = InfluxDBClient(host=dbhost, port=dbport, db=dbname)
        await solar_client.create_database(db=dbname)
    except ClientConnectionError as e:
        logger.error(f'Error during connection to InfluxDb {dbhost}: {e}')
        return
    logger.info('Database opened and initialized')
 
    # Read the common blocks on the Inverter
    while True:
        reg_block = {}
        reg_block = client.read_holding_registers(40004, 65)
        if reg_block:
            decoder = BinaryPayloadDecoder.fromRegisters(reg_block, byteorder=Endian.Big, wordorder=Endian.Big)
            InvManufacturer = decoder.decode_string(32).decode('UTF-8') #decoder.decode_32bit_float(),
            InvModel = decoder.decode_string(32).decode('UTF-8') #decoder.decode_32bit_int(),
            Invfoo = decoder.decode_string(16).decode('UTF-8')
            InvVersion = decoder.decode_string(16).decode('UTF-8') #decoder.decode_bits()
            InvSerialNumber = decoder.decode_string(32).decode('UTF-8')
            InvDeviceAddress = decoder.decode_16bit_uint()

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
            # Error during data receive
            if client.last_error() == 2:
                logger.error(f'Failed to connect to SolarEdge inverter {client.host()}!')
            elif client.last_error() == 3 or client.last_error() == 4:
                logger.error('Send or receive error!')
            elif client.last_error() == 5:
                logger.error('Timeout during send or receive operation!')
            await asyncio.sleep(period)

    # Read the common blocks on the meter/s (if present)
    connflag = False
    if mbmeters >= 1:
        while True:
            dictMeterLabel = []
            for x in range(1, mbmeters+1):
                reg_block = {}
                if x==1:
                    reg_block = client.read_holding_registers(40123, 65)
                if x==2:
                    reg_block = client.read_holding_registers(40297, 65)
                if x==3:
                    reg_block = client.read_holding_registers(40471, 65)       
                if reg_block:
                    decoder = BinaryPayloadDecoder.fromRegisters(reg_block, byteorder=Endian.Big, wordorder=Endian.Big)
                    MManufacturer = decoder.decode_string(32).decode('UTF-8') #decoder.decode_32bit_float(),
                    MModel = decoder.decode_string(32).decode('UTF-8') #decoder.decode_32bit_int(),
                    MOption = decoder.decode_string(16).decode('UTF-8')
                    MVersion = decoder.decode_string(16).decode('UTF-8') #decoder.decode_bits()
                    MSerialNumber = decoder.decode_string(32).decode('UTF-8')
                    MDeviceAddress = decoder.decode_16bit_uint()
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
                    # Error during data receive
                    if client.last_error() == 2:
                        logger.error(f'Failed to connect to SolarEdge inverter {client.host()}!')
                    elif client.last_error() == 3 or client.last_error() == 4:
                        logger.error('Send or receive error!')
                    elif client.last_error() == 5:
                        logger.error('Timeout during send or receive operation!')
                    await asyncio.sleep(period)
            if connflag:
                break

    # Start the loop for collecting the metrics...
    while True:
        try:
            reg_block = {}
            dictInv = {}
            reg_block = client.read_holding_registers(40069, 40)
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
                    'measurement': 'SolarEdge',
                    'tags': {},
                    'fields': {}
                }
                logger.debug(f'inverter reg_block: {str(reg_block)}')
                datapoint['tags']['inverter'] = str(1)

                data = BinaryPayloadDecoder.fromRegisters(reg_block, byteorder=Endian.Big, wordorder=Endian.Big)

                # SunSpec DID
                # Register 40069
                fooVal = trunc_float(data.decode_16bit_uint())
                fooName = 'SunSpec_DID'
                if fooVal < 65535:
                    dictInv[fooName] = fooVal
                else:
                    dictInv[fooName] = 0.0

                # SunSpec Length
                # Register 40070
                fooVal = trunc_float(data.decode_16bit_uint())
                fooName = 'SunSpec_Length'
                if fooVal < 65535:
                    dictInv[fooName] = fooVal
                else:
                    dictInv[fooName] = 0.0
                
                # AC Current
                data.skip_bytes(8)
                # Register 40075
                scalefactor = 10**data.decode_16bit_int()
                data.skip_bytes(-10)
                # Register 40071-40074
                fooVal = trunc_float(data.decode_16bit_uint())
                fooName = 'AC_Current'
                if fooVal < 65535:
                    dictInv[fooName] = fooVal * scalefactor
                else:
                    dictInv[fooName] = 0.0
                fooVal = trunc_float(data.decode_16bit_uint())
                fooName = 'AC_CurrentA'
                if fooVal < 65535:
                    dictInv[fooName] = fooVal * scalefactor
                else:
                    dictInv[fooName] = 0.0
                fooVal = trunc_float(data.decode_16bit_uint())
                fooName = 'AC_CurrentB'
                if fooVal < 65535:
                    dictInv[fooName] = fooVal * scalefactor
                else:
                    dictInv[fooName] = 0.0
                fooVal = trunc_float(data.decode_16bit_uint())
                fooName = 'AC_CurrentC'
                if fooVal < 65535:
                    dictInv[fooName] = fooVal * scalefactor
                else:
                    dictInv[fooName] = 0.0

                # AC Voltage
                data.skip_bytes(14)
                # Register 40082
                scalefactor = 10**data.decode_16bit_int()
                data.skip_bytes(-14)
                # Register 40077
                fooVal = trunc_float(data.decode_16bit_uint())
                fooName = 'AC_VoltageAB'
                if fooVal < 65535:
                    dictInv[fooName] = fooVal * scalefactor
                else:
                    dictInv[fooName] = 0.0
                fooVal = trunc_float(data.decode_16bit_uint())
                fooName = 'AC_VoltageBC'
                if fooVal < 65535:
                    dictInv[fooName] = fooVal * scalefactor
                else:
                    dictInv[fooName] = 0.0
                fooVal = trunc_float(data.decode_16bit_uint())
                fooName = 'AC_VoltageCA'
                if fooVal < 65535:
                    dictInv[fooName] = fooVal * scalefactor
                else:
                    dictInv[fooName] = 0.0
                fooVal = trunc_float(data.decode_16bit_uint())
                fooName = 'AC_VoltageAN'
                if fooVal < 65535:
                    dictInv[fooName] = fooVal * scalefactor
                else:
                    dictInv[fooName] = 0.0
                fooVal = trunc_float(data.decode_16bit_uint())
                fooName = 'AC_VoltageBN'
                if fooVal < 65535:
                    dictInv[fooName] = fooVal * scalefactor
                else:
                    dictInv[fooName] = 0.0
                fooVal = trunc_float(data.decode_16bit_uint())
                fooName = 'AC_VoltageCN'
                if fooVal < 65535:
                    dictInv[fooName] = fooVal * scalefactor
                else:
                    dictInv[fooName] = 0.0

                # AC Power
                data.skip_bytes(4)
                # Register 40084
                scalefactor = 10**data.decode_16bit_int()
                data.skip_bytes(-4)
                # Register 40083
                fooVal = trunc_float(data.decode_16bit_int())
                fooName = 'AC_Power'
                if fooVal < 65535:
                    dictInv[fooName] = fooVal * scalefactor
                else:
                    dictInv[fooName] = 0.0

               # AC Frequency
                data.skip_bytes(4)
                # Register 40086
                scalefactor = 10**data.decode_16bit_int()
                data.skip_bytes(-4)
                # Register 40085
                fooVal = trunc_float(data.decode_16bit_uint())
                fooName = 'AC_Frequency'
                if fooVal < 65535:
                    dictInv[fooName] = fooVal * scalefactor
                else:
                    dictInv[fooName] = 0.0

                # AC Apparent Power
                data.skip_bytes(4)
                # Register 40088
                scalefactor = 10**data.decode_16bit_int()
                data.skip_bytes(-4)
                # Register 40087
                fooVal = trunc_float(data.decode_16bit_int())
                fooName = 'AC_VA'
                if fooVal < 65535:
                    dictInv[fooName] = fooVal * scalefactor
                else:
                    dictInv[fooName] = 0.0
                
                # AC Reactive Power
                data.skip_bytes(4)
                # Register 40090
                scalefactor = 10**data.decode_16bit_int()
                data.skip_bytes(-4)
                # Register 40089
                fooVal = trunc_float(data.decode_16bit_int())
                fooName = 'AC_VAR'
                if fooVal < 65535:
                    dictInv[fooName] = fooVal * scalefactor
                else:
                    dictInv[fooName] = 0.0
                
                # AC Power Factor
                data.skip_bytes(4)
                # Register 40092
                scalefactor = 10**data.decode_16bit_int()
                data.skip_bytes(-4)
                # Register 40091
                fooVal = trunc_float(data.decode_16bit_int())
                fooName = 'AC_PF'
                if fooVal < 65535:
                    dictInv[fooName] = fooVal * scalefactor
                else:
                    dictInv[fooName] = 0.0

                # AC Lifetime Energy Production
                data.skip_bytes(6)
                # Register 40095
                scalefactor = 10**data.decode_16bit_uint()
                data.skip_bytes(-6)
                # Register 40093
                fooVal = trunc_float(data.decode_32bit_uint())
                fooName = 'AC_Energy_WH'
                if fooVal < 4294967295:
                    dictInv[fooName] = fooVal * scalefactor
                else:
                    dictInv[fooName] = 0.0
                
                # DC Current
                data.skip_bytes(4)
                # Register 40097
                scalefactor = 10**data.decode_16bit_int()
                data.skip_bytes(-4)
                # Register 40096
                fooVal = trunc_float(data.decode_16bit_uint())
                fooName = 'DC_Current'
                if fooVal < 65535:
                    dictInv[fooName] = fooVal * scalefactor
                else:
                    dictInv[fooName] = 0.0

                # DC Voltage
                data.skip_bytes(4)
                # Register 40099
                scalefactor = 10**data.decode_16bit_int()
                data.skip_bytes(-4)
                # Register 40098
                fooVal = trunc_float(data.decode_16bit_uint())
                fooName = 'DC_Voltage'
                if fooVal < 65535:
                    dictInv[fooName] = fooVal * scalefactor
                else:
                    dictInv[fooName] = 0.0

                # DC Power
                data.skip_bytes(4)
                # Register 40101
                scalefactor = 10**data.decode_16bit_int()
                data.skip_bytes(-4)
                # Register 40100
                fooVal = trunc_float(data.decode_16bit_int())
                fooName = 'DC_Power'
                if fooVal < 65535:
                    dictInv[fooName] = fooVal * scalefactor
                else:
                    dictInv[fooName] = 0.0
                
                # Inverter Temp 
                data.skip_bytes(10)
                # Register 40106
                scalefactor = 10**data.decode_16bit_int()
                data.skip_bytes(-8)
                # Register 40103
                fooVal = trunc_float(data.decode_16bit_int())
                fooName = 'Temp_Sink'
                if fooVal < 65535:
                    dictInv[fooName] = fooVal * scalefactor
                else:
                    dictInv[fooName] = 0.0
                
                # Inverter Operating State
                data.skip_bytes(6)
                # Register 40107
                fooVal = trunc_float(data.decode_16bit_uint())
                fooName = 'Status'
                if fooVal < 65535:
                    dictInv[fooName] = fooVal
                else:
                    dictInv[fooName] = 0.0
                
                # Inverter Operating Status Code
                # Register 40108
                fooVal = trunc_float(data.decode_16bit_uint())
                fooName = 'Status_Vendor'
                if fooVal < 65535:
                    dictInv[fooName] = fooVal
                else:
                    dictInv[fooName] = 0.0

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
                             
                datapoint['time'] = str(datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat())

                logger.debug(f'Writing to Influx: {str(datapoint)}')
                await solar_client.write(datapoint)

            else:
                # Error during data receive
                if client.last_error() == 2:
                    logger.error(f'Failed to connect to SolarEdge inverter {client.host()}!')
                elif client.last_error() == 3 or client.last_error() == 4:
                    logger.error('Send or receive error!')
                elif client.last_error() == 5:
                    logger.error('Timeout during send or receive operation!')
                await asyncio.sleep(period)
                    
            for x in range(1, mbmeters+1):
                # Now loop through this for each meter that is attached.
                logger.debug(f'Meter={str(x)}')
                reg_block = {}
                dictM = {}

                # Start point is different for each meter
                if x==1:
                    reg_block = client.read_holding_registers(40188, 103)
                if x==2:
                    reg_block = client.read_holding_registers(40362, 103)
                if x==3:
                    reg_block = client.read_holding_registers(40537, 103)
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
                        'measurement': 'SolarEdge',
                        'tags': {
                            'meter': dictMeterLabel[x-1]
                        },
                        'fields': {}
                    }
                    
                    data = BinaryPayloadDecoder.fromRegisters(reg_block, byteorder=Endian.Big, wordorder=Endian.Big)

                    # SunSpec DID
                    # Register 40188
                    fooVal = trunc_float(data.decode_16bit_uint())
                    fooName = 'M_SunSpec_DID'
                    if fooVal < 65535:
                        dictM[fooName] = fooVal
                    else:
                        dictM[fooName] = 0.0

                    # SunSpec Length
                    # Register 40070
                    fooVal = trunc_float(data.decode_16bit_uint())
                    fooName = 'M_SunSpec_Length'
                    if fooVal < 65535:
                        dictM[fooName] = fooVal
                    else:
                        dictM[fooName] = 0.0

                    # AC Current
                    data.skip_bytes(8)
                    # Register 40194
                    scalefactor = 10**data.decode_16bit_int()
                    data.skip_bytes(-10)
                    # Register 40190-40193
                    fooVal = trunc_float(data.decode_16bit_int())
                    fooName = 'M_AC_Current'
                    if fooVal < 32768:
                        dictM[fooName] = fooVal * scalefactor
                    else:
                        dictM[fooName] = 0.0
                    fooVal = trunc_float(data.decode_16bit_int())
                    fooName = 'M_AC_CurrentA'
                    if fooVal < 32768:
                        dictM[fooName] = fooVal * scalefactor
                    else:
                        dictM[fooName] = 0.0
                    fooVal = trunc_float(data.decode_16bit_int())
                    fooName = 'M_AC_CurrentB'
                    if fooVal < 32768:
                        dictM[fooName] = fooVal * scalefactor
                    else:
                        dictM[fooName] = 0.0
                    fooVal = trunc_float(data.decode_16bit_int())
                    fooName = 'M_AC_CurrentC'
                    if fooVal < 32768:
                        dictM[fooName] = fooVal * scalefactor
                    else:
                        dictM[fooName] = 0.0

                   # AC Voltage
                    data.skip_bytes(18)
                    # Register 40203
                    scalefactor = 10**data.decode_16bit_int()
                    data.skip_bytes(-18)
                    # Register 40195-40202
                    fooVal = trunc_float(data.decode_16bit_int())
                    fooName = 'M_AC_VoltageLN'
                    if fooVal < 32768:
                        dictM[fooName] = fooVal * scalefactor
                    else:
                        dictM[fooName] = 0.0
                    fooVal = trunc_float(data.decode_16bit_int())
                    fooName = 'M_AC_VoltageAN'
                    if fooVal < 32768:
                        dictM[fooName] = fooVal * scalefactor
                    else:
                        dictM[fooName] = 0.0
                    fooVal = trunc_float(data.decode_16bit_int())
                    fooName = 'M_AC_VoltageBN'
                    if fooVal < 32768:
                        dictM[fooName] = fooVal * scalefactor
                    else:
                        dictM[fooName] = 0.0
                    fooVal = trunc_float(data.decode_16bit_int())
                    fooName = 'M_AC_VoltageCN'
                    if fooVal < 32768:
                        dictM[fooName] = fooVal * scalefactor
                    else:
                        dictM[fooName] = 0.0
                    fooVal = trunc_float(data.decode_16bit_int())
                    fooName = 'M_AC_VoltageLL'
                    if fooVal < 32768:
                        dictM[fooName] = fooVal * scalefactor
                    else:
                        dictM[fooName] = 0.0
                    fooVal = trunc_float(data.decode_16bit_int())
                    fooName = 'M_AC_VoltageAB'
                    if fooVal < 32768:
                        dictM[fooName] = fooVal * scalefactor
                    else:
                        dictM[fooName] = 0.0
                    fooVal = trunc_float(data.decode_16bit_int())
                    fooName = 'M_AC_VoltageBC'
                    if fooVal < 32768:
                        dictM[fooName] = fooVal * scalefactor
                    else:
                        dictM[fooName] = 0.0
                    fooVal = trunc_float(data.decode_16bit_int())
                    fooName = 'M_AC_VoltageCA'
                    if fooVal < 32768:
                        dictM[fooName] = fooVal * scalefactor
                    else:
                        dictM[fooName] = 0.0

                    # AC Frequency
                    data.skip_bytes(4)
                    # Register 40205
                    scalefactor = 10**data.decode_16bit_int()
                    data.skip_bytes(-4)
                    # Register 40204
                    fooVal = trunc_float(data.decode_16bit_int())
                    fooName = 'M_AC_Frequency'
                    if fooVal < 32768:
                        dictM[fooName] = fooVal * scalefactor
                    else:
                        dictM[fooName] = 0.0
                    
                    # AC Real Power
                    data.skip_bytes(10)
                    # Register 40210
                    scalefactor = 10**data.decode_16bit_int()
                    data.skip_bytes(-10)
                    # Register 40206-40209
                    fooVal = trunc_float(data.decode_16bit_int())
                    fooName = 'M_AC_Power'
                    if fooVal < 32768:
                        dictM[fooName] = fooVal * scalefactor
                    else:
                        dictM[fooName] = 0.0
                    fooVal = trunc_float(data.decode_16bit_int())
                    fooName = 'M_AC_Power_A'
                    if fooVal < 32768:
                        dictM[fooName] = fooVal * scalefactor
                    else:
                        dictM[fooName] = 0.0
                    fooVal = trunc_float(data.decode_16bit_int())
                    fooName = 'M_AC_Power_B'
                    if fooVal < 32768:
                        dictM[fooName] = fooVal * scalefactor
                    else:
                        dictM[fooName] = 0.0
                    fooVal = trunc_float(data.decode_16bit_int())
                    fooName = 'M_AC_Power_C'
                    if fooVal < 32768:
                        dictM[fooName] = fooVal * scalefactor
                    else:
                        dictM[fooName] = 0.0
                    
                    # AC Apparent Power
                    data.skip_bytes(10)
                    # Register 40215
                    scalefactor = 10**data.decode_16bit_int()
                    data.skip_bytes(-10)
                    # Register 40211-40214
                    fooVal = trunc_float(data.decode_16bit_int())
                    fooName = 'M_AC_VA'
                    if fooVal < 32768:
                        dictM[fooName] = fooVal * scalefactor
                    else:
                        dictM[fooName] = 0.0
                    fooVal = trunc_float(data.decode_16bit_int())
                    fooName = 'M_AC_VA_A'
                    if fooVal < 32768:
                        dictM[fooName] = fooVal * scalefactor
                    else:
                        dictM[fooName] = 0.0
                    fooVal = trunc_float(data.decode_16bit_int())
                    fooName = 'M_AC_VA_B'
                    if fooVal < 32768:
                        dictM[fooName] = fooVal * scalefactor
                    else:
                        dictM[fooName] = 0.0
                    fooVal = trunc_float(data.decode_16bit_int())
                    fooName = 'M_AC_VA_C'
                    if fooVal < 32768:
                        dictM[fooName] = fooVal * scalefactor
                    else:
                        dictM[fooName] = 0.0

                    # AC Reactive Power
                    data.skip_bytes(10)
                    # Register 40220
                    scalefactor = 10**data.decode_16bit_int()
                    data.skip_bytes(-10)
                    # Register 40216-40219
                    fooVal = trunc_float(data.decode_16bit_int())
                    fooName = 'M_AC_VAR'
                    if fooVal < 32768:
                        dictM[fooName] = fooVal * scalefactor
                    else:
                        dictM[fooName] = 0.0
                    fooVal = trunc_float(data.decode_16bit_int())
                    fooName = 'M_AC_VAR_A'
                    if fooVal < 32768:
                        dictM[fooName] = fooVal * scalefactor
                    else:
                        dictM[fooName] = 0.0
                    fooVal = trunc_float(data.decode_16bit_int())
                    fooName = 'M_AC_VAR_B'
                    if fooVal < 32768:
                        dictM[fooName] = fooVal * scalefactor
                    else:
                        dictM[fooName] = 0.0
                    fooVal = trunc_float(data.decode_16bit_int())
                    fooName = 'M_AC_VAR_C'
                    if fooVal < 32768:
                        dictM[fooName] = fooVal * scalefactor
                    else:
                        dictM[fooName] = 0.0

                    # AC Power Factor
                    data.skip_bytes(10)
                    # Register 40225
                    scalefactor = 10**data.decode_16bit_int()
                    data.skip_bytes(-10)
                    # Register 40221-40224
                    fooVal = trunc_float(data.decode_16bit_int())
                    fooName = 'M_AC_PF'
                    if fooVal < 32768:
                        dictM[fooName] = fooVal * scalefactor
                    else:
                        dictM[fooName] = 0.0
                    fooVal = trunc_float(data.decode_16bit_int())
                    fooName = 'M_AC_PF_A'
                    if fooVal < 32768:
                        dictM[fooName] = fooVal * scalefactor
                    else:
                        dictM[fooName] = 0.0
                    fooVal = trunc_float(data.decode_16bit_int())
                    fooName = 'M_AC_PF_B'
                    if fooVal < 32768:
                        dictM[fooName] = fooVal * scalefactor
                    else:
                        dictM[fooName] = 0.0
                    fooVal = trunc_float(data.decode_16bit_int())
                    fooName = 'M_AC_PF_C'
                    if fooVal < 32768:
                        dictM[fooName] = fooVal * scalefactor
                    else:
                        dictM[fooName] = 0.0

                    # Accumulated AC Real Energy
                    data.skip_bytes(34)
                    # Register 40242
                    scalefactor = 10**data.decode_16bit_int()
                    data.skip_bytes(-34)
                    # Register 40226-40240
                    fooVal = trunc_float(data.decode_32bit_uint())
                    fooName = 'M_Exported'
                    if fooVal < 4294967295:
                        dictM[fooName] = fooVal * scalefactor
                    else:
                        dictM[fooName] = 0.0
                    fooVal = trunc_float(data.decode_32bit_uint())
                    fooName = 'M_Exported_A'
                    if fooVal < 4294967295:
                        dictM[fooName] = fooVal * scalefactor
                    else:
                        dictM[fooName] = 0.0
                    fooVal = trunc_float(data.decode_32bit_uint())
                    fooName = 'M_Exported_B'
                    if fooVal < 4294967295:
                        dictM[fooName] = fooVal * scalefactor
                    else:
                        dictM[fooName] = 0.0
                    fooVal = trunc_float(data.decode_32bit_uint())
                    fooName = 'M_Exported_C'
                    if fooVal < 4294967295:
                        dictM[fooName] = fooVal * scalefactor
                    else:
                        dictM[fooName] = 0.0
                    fooVal = trunc_float(data.decode_32bit_uint())
                    fooName = 'M_Imported'
                    if fooVal < 4294967295:
                        dictM[fooName] = fooVal * scalefactor
                    else:
                        dictM[fooName] = 0.0
                    fooVal = trunc_float(data.decode_32bit_uint())
                    fooName = 'M_Imported_A'
                    if fooVal < 4294967295:
                        dictM[fooName] = fooVal * scalefactor
                    else:
                        dictM[fooName] = 0.0
                    fooVal = trunc_float(data.decode_32bit_uint())
                    fooName = 'M_Imported_B'
                    if fooVal < 4294967295:
                        dictM[fooName] = fooVal * scalefactor
                    else:
                        dictM[fooName] = 0.0
                    fooVal = trunc_float(data.decode_32bit_uint())
                    fooName = 'M_Imported_C'
                    if fooVal < 4294967295:
                        dictM[fooName] = fooVal * scalefactor
                    else:
                        dictM[fooName] = 0.0
                   
                    # Accumulated AC Apparent Energy
                    #logger.debug(f'Apparent Energy SF: {str(np.int16(reg_block[69]))}')
                    #scalefactor = np.float_power(10,np.int16(reg_block[69]))
                    #datapoint['fields']['M_Exported_VA'] = trunc_float(((reg_block[53] << 16) + reg_block[54]) * scalefactor) 
                    #datapoint['fields']['M_Exported_VA_A'] = trunc_float(((reg_block[55] << 16) + reg_block[56]) * scalefactor)
                    #datapoint['fields']['M_Exported_VA_B'] = trunc_float(((reg_block[57] << 16) + reg_block[58]) * scalefactor)
                    #datapoint['fields']['M_Exported_VA_C'] = trunc_float(((reg_block[59] << 16) + reg_block[60]) * scalefactor)
                    #datapoint['fields']['M_Imported_VA'] = trunc_float(((reg_block[61] << 16) + reg_block[62]) * scalefactor)
                    #datapoint['fields']['M_Imported_VA_A'] = trunc_float(((reg_block[63] << 16) + reg_block[64]) * scalefactor)
                    #datapoint['fields']['M_Imported_VA_B'] = trunc_float(((reg_block[65] << 16) + reg_block[66]) * scalefactor)
                    #datapoint['fields']['M_Imported_VA_C'] = trunc_float(((reg_block[67] << 16) + reg_block[68]) * scalefactor)

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

                    datapoint['time'] = str(datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat())

                    logger.debug(f'Meter: {metriclabel}')
                    for j, k in dictM.items():
                        logger.debug(f'  {j}: {k}')
                        
                    logger.debug(f'Writing to Influx: {str(datapoint)}')
                    await solar_client.write(datapoint)

                else:
                    # Error during data receive
                    if client.last_error() == 2:
                        logger.error(f'Failed to connect to SolarEdge inverter {client.host()}!')
                    elif client.last_error() == 3 or client.last_error() == 4:
                        logger.error('Send or receive error!')
                    elif client.last_error() == 5:
                        logger.error('Timeout during send or receive operation!')
                    await asyncio.sleep(period)
                
        except InfluxDBWriteError as e:
            logger.error(f'Failed to write to InfluxDb: {e}')
        except IOError as e:
            logger.error(f'I/O exception during operation: {e}')
        except Exception as e:
            logger.error(f'Unhandled exception: {e}')

        await asyncio.sleep(period)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--influx_server', default='192.168.192.41')
    parser.add_argument('--influx_port', type=int, default=8086)
    parser.add_argument('--influx_database', default='solaredgetemp')
    parser.add_argument('--inverter_port', type=int, default=1502, help='ModBus TCP port number to use')
    parser.add_argument('--unitid', type=int, default=1, help='ModBus unit id to use in communication')
    parser.add_argument('--meters', type=int, default=0, help='Number of ModBus meters attached to inverter (0-3)')
    parser.add_argument('--prometheus_exporter_port', type=int, default=2112, help='Port on which the prometheus exporter will listen on')
    parser.add_argument('--interval', type=int, default=5, help='Time (seconds) between polling')
    parser.add_argument('--legacy_support', type=bool, default=False, help='Set to true so Meter 1 prometheus metrics start with "M_" vs "M1_"')
    parser.add_argument('inverter_ip', metavar='SolarEdge IP', help='IP address of the SolarEdge inverter to monitor')
    parser.add_argument('--debug', '-d', action='count')
    args = parser.parse_args()

    logging.basicConfig()
    if args.debug and args.debug >= 1:
        logging.getLogger('solaredge').setLevel(logging.DEBUG)
    if args.debug and args.debug == 2:
        logging.getLogger('aioinflux').setLevel(logging.DEBUG)

    print(f'*' * 60)
    print(f'* Starting parameters')
    print(f'*' * 60)
    print(f'Inverter:\tAddress: {args.inverter_ip}\n\t\tPort: {args.inverter_port}\n\t\tID: {args.unitid}')
    print(f'Meters:\t\t{args.meters}')
    print(f'InfluxDB:\tServer: {args.influx_server}:{args.influx_port}\n\t\tDatabase: {args.influx_database}')
    print(f'Prometheus:\tExporter Port: {args.prometheus_exporter_port}\n')
    print(f'Legacy Support:\t{args.legacy_support}\n')
    client = ModbusClient(args.inverter_ip, port=args.inverter_port, unit_id=args.unitid, auto_open=True)
    logger.debug('Starting Prometheus exporter on port {args.prometheus_exporter_port}...')
    start_http_server(args.prometheus_exporter_port)
    #define_prometheus_metrics(args.meters)
    logger.debug('Running eventloop')
    asyncio.get_event_loop().run_until_complete(write_to_influx(args.influx_server, args.influx_port, args.meters, args.interval, args.influx_database, args.legacy_support))
