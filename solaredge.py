#!/usr/bin/env python3
import argparse
import datetime
import logging
import numpy as np

from aiohttp import ClientConnectionError
from pyModbusTCP.client import ModbusClient
from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadDecoder
import asyncio
from aioinflux import InfluxDBClient, InfluxDBWriteError

datapoint = {
    'measurement': 'SolarEdge',
    'tags': {},
    'fields': {}
}
reg_block = {}
logger = logging.getLogger('solaredge')


async def write_to_influx(dbhost, dbport, mbmeters, dbname='solaredgetemp'):
    global client
    global datapoint
    global reg_block

    def trunc_float(floatval):
        return float('%.2f' % floatval)

    try:
        solar_client = InfluxDBClient(host=dbhost, port=dbport, db=dbname)
        await solar_client.create_database(db=dbname)
    except ClientConnectionError as e:
        logger.error(f'Error during connection to InfluxDb {dbhost}: {e}')
        return

    logger.info('Database opened and initialized')
    while True:
        try:
            reg_block = {}
            reg_block = client.read_holding_registers(40069, 40)
            if reg_block:
                datapoint = {
                    'measurement': 'SolarEdge',
                    'tags': {},
                    'fields': {}
                }
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
                logger.debug(f'inverter reg_block: {str(reg_block)}')
                datapoint['tags']['inverter'] = 1

                # AC Current
                logger.debug(f'Block6: {str(reg_block[6])}')
                logger.debug(f'AC Current SF: {str(np.int16(reg_block[6]))}')
                scalefactor = np.float_power(10,np.int16(reg_block[6]))
                logger.debug(f'AC Current mult: {str(scalefactor)}')
                if reg_block[2]<65535:
                    datapoint['fields']['AC_Current'] = trunc_float(reg_block[2] * scalefactor)
                if reg_block[3] <65535:
                    datapoint['fields']['AC_CurrentA'] = trunc_float(reg_block[3] * scalefactor)
                if reg_block[4]<65535:
                    datapoint['fields']['AC_CurrentB'] = trunc_float(reg_block[4] * scalefactor)
                if reg_block[5]<65535:
                    datapoint['fields']['AC_CurrentC'] = trunc_float(reg_block[5] * scalefactor)

                # AC Voltage
                logger.debug(f'Block13: {str(reg_block[13])}')
                logger.debug(f'AC Voltage SF: {str(np.int16(reg_block[13]))}')
                scalefactor = np.float_power(10,np.int16(reg_block[13]))
                logger.debug(f'AC Voltage mult: {str(scalefactor)}')
                if reg_block[7]<65535:
                    datapoint['fields']['AC_VoltageAB'] = trunc_float(reg_block[7] * scalefactor)
                if reg_block[8]<65535:
                    datapoint['fields']['AC_VoltageBC'] = trunc_float(reg_block[8] * scalefactor)
                if reg_block[9]<65535:
                    datapoint['fields']['AC_VoltageCA'] = trunc_float(reg_block[9] * scalefactor)
                if reg_block[10]<65535:
                    datapoint['fields']['AC_VoltageAN'] = trunc_float(reg_block[10] * scalefactor)
                if reg_block[11]<65535:
                    datapoint['fields']['AC_VoltageBN'] = trunc_float(reg_block[11] * scalefactor)
                if reg_block[12]<65535:
                    datapoint['fields']['AC_VoltageCN'] = trunc_float(reg_block[12] * scalefactor)

                # AC Power
                logger.debug(f'Block15: {str(reg_block[15])}')
                logger.debug(f'AC Power SF: {str(np.int16(reg_block[15]))}')
                scalefactor = np.float_power(10,np.int16(reg_block[15]))
                logger.debug(f'AC Power mult: {str(scalefactor)}')
                if reg_block[14]<65535:
                    datapoint['fields']['AC_Power'] = trunc_float(reg_block[14] * scalefactor)
                    
                # AC Frequency
                logger.debug(f'AC Frequency SF: {str(np.int16(reg_block[17]))}')
                scalefactor = np.float_power(10,np.int16(reg_block[17]))
                if reg_block[16]<65535:
                    datapoint['fields']['AC_Frequency'] = trunc_float(reg_block[16] * scalefactor)

                # AC Apparent Power
                logger.debug(f'Apparent Power SF: {str(np.int16(reg_block[19]))}')
                scalefactor = np.float_power(10,np.int16(reg_block[19]))
                if reg_block[18]<65535:
                    datapoint['fields']['AC_VA'] = trunc_float(reg_block[18] * scalefactor)                    

                # AC Reactive Power
                logger.debug(f'Reactive Power SF: {str(np.int16(reg_block[21]))}')
                scalefactor = np.float_power(10,np.int16(reg_block[21]))
                if reg_block[20]<65535:
                    datapoint['fields']['AC_VAR'] = trunc_float(reg_block[20] * scalefactor)                    

                # AC Power Factor
                logger.debug(f'Power Factor SF: {str(np.int16(reg_block[23]))}')
                scalefactor = np.float_power(10,np.int16(reg_block[23]))
                if reg_block[22]<65535:
                    datapoint['fields']['AC_PF'] = trunc_float(reg_block[22] * scalefactor)                    

                # AC Lifetime Energy Production
                logger.debug(f'Lifetime Energy Production SF: {str(np.uint16(reg_block[26]))}')
                scalefactor = np.float_power(10,np.uint16(reg_block[26]))
                if reg_block[24]<65535:
                    datapoint['fields']['AC_Energy_WH'] = trunc_float(((reg_block[24] << 16) + reg_block[25]) * scalefactor)   
                    
                # DC Current
                logger.debug(f'Block28: {str(reg_block[28])}')
                logger.debug(f'DC Current SF: {str(np.int16(reg_block[28]))}')
                scalefactor = np.float_power(10,np.int16(reg_block[28]))
                logger.debug(f'DC Current mult: {str(scalefactor)}')
                if reg_block[27]<65535:
                    datapoint['fields']['DC_Current'] = trunc_float(reg_block[27] * scalefactor)

                # DC Voltage
                logger.debug(f'Block30: {str(reg_block[30])}')
                logger.debug(f'DC voltage SF: {str(np.int16(reg_block[30]))}')
                scalefactor = np.float_power(10,np.int16(reg_block[30]))
                logger.debug(f'DC Voltage mult: {str(scalefactor)}')
                if reg_block[29]<65535:
                    datapoint['fields']['DC_Voltage'] = trunc_float(reg_block[29] * scalefactor)

                # DC Power
                logger.debug(f'Block32: {str(reg_block[32])}')
                logger.debug(f'DC Power SF: {str(np.int16(reg_block[32]))}')
                scalefactor = np.float_power(10,np.int16(reg_block[32]))
                logger.debug(f'DC Power mult: {str(scalefactor)}')
                if reg_block[31]<65535:
                    datapoint['fields']['DC_Power'] = trunc_float(reg_block[31] * scalefactor)

                # Inverter Temp 
                logger.debug(f'Block37: {str(reg_block[37])}')
                logger.debug(f'Temp SF: {str(np.int16(reg_block[37]))}')
                scalefactor = np.float_power(10,np.int16(reg_block[37]))
                logger.debug(f'Temp mult: {str(scalefactor)}')
                if reg_block[34]<65535:
                    datapoint['fields']['Temp_Sink'] = trunc_float(reg_block[34] * scalefactor)

                # Inverter Operating State
                logger.debug(f'Operating State: {str(np.uint16(reg_block[38]))}')
                if reg_block[38]<65535:
                    datapoint['fields']['Status'] = trunc_float(reg_block[38])                    

                # Inverter Operating Status Code
                logger.debug(f'Operating Status Code: {str(np.uint16(reg_block[39]))}')
                if reg_block[39]<65535:
                    datapoint['fields']['Status_Vendor'] = trunc_float(reg_block[39])                     
                    
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
                    
            for x in range(1, mbmeters+1):
                # Now loop through this for each meter that is attached.
                logger.debug(f'Meter={str(x)}')
                reg_block = {}
                
                # Clear data from inverter, otherwise we publish that again!
                datapoint = {
                    'measurement': 'SolarEdge',
                    'tags': {
                        'meter': x
                    },
                    'fields': {}
                }

                # Start point is different for each meter
                if x==1:
                    reg_block = client.read_holding_registers(40190, 103)
                if x==2:
                    reg_block = client.read_holding_registers(40364, 103)
                if x==3:
                    reg_block = client.read_holding_registers(40539, 103)
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

                    # AC Current
                    logger.debug(f'AC Current SF: {str(np.int16(reg_block[4]))}')
                    scalefactor = np.float_power(10,np.int16(reg_block[4]))
                    if reg_block[0]<65535:
                        datapoint['fields']['M_AC_Current'] = trunc_float(np.int16(reg_block[0]) * scalefactor)
                    if reg_block[1]<65535:
                        datapoint['fields']['M_AC_Current_A'] = trunc_float(np.int16(reg_block[1]) * scalefactor)
                    if reg_block[2]<65535:
                        datapoint['fields']['M_AC_Current_B'] = trunc_float(np.int16(reg_block[2]) * scalefactor)
                    if reg_block[3]<65535:
                        datapoint['fields']['M_AC_Current_C'] = trunc_float(np.int16(reg_block[3]) * scalefactor)

                    # AC Voltage
                    logger.debug(f'AC Voltage SF: {str(np.int16(reg_block[13]))}')
                    scalefactor = np.float_power(10,np.int16(reg_block[13]))
                    datapoint['fields']['M_AC_Voltage_LN'] = trunc_float(np.int16(reg_block[5]) * scalefactor)
                    datapoint['fields']['M_AC_Voltage_AN'] = trunc_float(np.int16(reg_block[6]) * scalefactor)
                    datapoint['fields']['M_AC_Voltage_BN'] = trunc_float(np.int16(reg_block[7]) * scalefactor)
                    datapoint['fields']['M_AC_Voltage_CN'] = trunc_float(np.int16(reg_block[8]) * scalefactor)
                    datapoint['fields']['M_AC_Voltage_LL'] = trunc_float(np.int16(reg_block[9]) * scalefactor)
                    datapoint['fields']['M_AC_Voltage_AB'] = trunc_float(np.int16(reg_block[10]) * scalefactor)
                    datapoint['fields']['M_AC_Voltage_BC'] = trunc_float(np.int16(reg_block[11]) * scalefactor)
                    datapoint['fields']['M_AC_Voltage_CA'] = trunc_float(np.int16(reg_block[12]) * scalefactor)

                    # AC Frequency
                    logger.debug(f'AC Frequency SF: {str(np.int16(reg_block[15]))}')
                    scalefactor = np.float_power(10,np.int16(reg_block[15]))
                    datapoint['fields']['M_AC_Freq'] = trunc_float(np.int16(reg_block[14]) * scalefactor)
                    
                    # AC Real Power
                    logger.debug(f'AC Real Power SF: {str(np.int16(reg_block[20]))}')
                    scalefactor = np.float_power(10,np.int16(reg_block[20]))
                    datapoint['fields']['M_AC_Power'] = trunc_float(np.int16(reg_block[16]) * scalefactor)
                    datapoint['fields']['M_AC_Power_A'] = trunc_float(np.int16(reg_block[17]) * scalefactor)
                    datapoint['fields']['M_AC_Power_B'] = trunc_float(np.int16(reg_block[18]) * scalefactor)
                    datapoint['fields']['M_AC_Power_C'] = trunc_float(np.int16(reg_block[19]) * scalefactor)
                    
                    # AC Apparent Power
                    logger.debug(f'AC Apparent Power SF: {str(np.int16(reg_block[25]))}')
                    scalefactor = np.float_power(10,np.int16(reg_block[25]))
                    datapoint['fields']['M_AC_VA'] = trunc_float(np.int16(reg_block[21]) * scalefactor)
                    datapoint['fields']['M_AC_VA_A'] = trunc_float(np.int16(reg_block[22]) * scalefactor)
                    datapoint['fields']['M_AC_VA_B'] = trunc_float(np.int16(reg_block[23]) * scalefactor)
                    datapoint['fields']['M_AC_VA_C'] = trunc_float(np.int16(reg_block[24]) * scalefactor)

                    # AC Reactive Power
                    logger.debug(f'AC Reactive Power SF: {str(np.int16(reg_block[30]))}')
                    scalefactor = np.float_power(10,np.int16(reg_block[30]))
                    datapoint['fields']['M_AC_VAR'] = trunc_float(np.int16(reg_block[26]) * scalefactor)
                    datapoint['fields']['M_AC_VAR_A'] = trunc_float(np.int16(reg_block[27]) * scalefactor)
                    datapoint['fields']['M_AC_VAR_B'] = trunc_float(np.int16(reg_block[28]) * scalefactor)
                    datapoint['fields']['M_AC_VAR_C'] = trunc_float(np.int16(reg_block[29]) * scalefactor)

                    # AC Power Factor
                    logger.debug(f'AC Power Factor SF: {str(np.int16(reg_block[30]))}')
                    scalefactor = np.float_power(10,np.int16(reg_block[35]))
                    datapoint['fields']['M_AC_PF'] = trunc_float(np.int16(reg_block[31]) * scalefactor)
                    datapoint['fields']['M_AC_PF_A'] = trunc_float(np.int16(reg_block[32]) * scalefactor)
                    datapoint['fields']['M_AC_PF_B'] = trunc_float(np.int16(reg_block[33]) * scalefactor)
                    datapoint['fields']['M_AC_PF_C'] = trunc_float(np.int16(reg_block[34]) * scalefactor)

                    # Accumulated AC Real Energy
                    logger.debug(f'Real Energy SF: {str(np.int16(reg_block[52]))}')
                    scalefactor = np.float_power(10,np.int16(reg_block[52]))
                    datapoint['fields']['M_Exported'] = trunc_float(((reg_block[36] << 16) + reg_block[37]) * scalefactor) 
                    datapoint['fields']['M_Exported_A'] = trunc_float(((reg_block[38] << 16) + reg_block[39]) * scalefactor)
                    datapoint['fields']['M_Exported_B'] = trunc_float(((reg_block[40] << 16) + reg_block[41]) * scalefactor)
                    datapoint['fields']['M_Exported_C'] = trunc_float(((reg_block[42] << 16) + reg_block[43]) * scalefactor)
                    datapoint['fields']['M_Imported'] = trunc_float(((reg_block[44] << 16) + reg_block[45]) * scalefactor)
                    datapoint['fields']['M_Imported_A'] = trunc_float(((reg_block[46] << 16) + reg_block[47]) * scalefactor)
                    datapoint['fields']['M_Imported_B'] = trunc_float(((reg_block[48] << 16) + reg_block[49]) * scalefactor)
                    datapoint['fields']['M_Imported_C'] = trunc_float(((reg_block[50] << 16) + reg_block[51]) * scalefactor)

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
                
                
                
                
        except InfluxDBWriteError as e:
            logger.error(f'Failed to write to InfluxDb: {e}')
        except IOError as e:
            logger.error(f'I/O exception during operation: {e}')
        except Exception as e:
            logger.error(f'Unhandled exception: {e}')

        await asyncio.sleep(5)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--influxdb', default='192.168.192.41')
    parser.add_argument('--influxport', type=int, default=8086)
    parser.add_argument('--port', type=int, default=1502, help='ModBus TCP port number to use')
    parser.add_argument('--unitid', type=int, default=1, help='ModBus unit id to use in communication')
    parser.add_argument('--meters', type=int, default=3, help='Number of ModBus meters attached to inverter (0-3)')
    parser.add_argument('solaredge', metavar='SolarEdge IP', help='IP address of the SolarEdge inverter to monitor')
    parser.add_argument('--debug', '-d', action='count')
    args = parser.parse_args()

    logging.basicConfig()
    if args.debug and args.debug >= 1:
        logging.getLogger('solaredge').setLevel(logging.DEBUG)
    if args.debug and args.debug == 2:
        logging.getLogger('aioinflux').setLevel(logging.DEBUG)

    print('Starting up solaredge monitoring')
    print(f'Connecting to Solaredge inverter {args.solaredge} on port {args.port} using unitid {args.unitid}')
    print(f'Writing data to influxDb {args.influxdb} on port {args.influxport}')
    print(f'Number of meters is {args.meters}')
    client = ModbusClient(args.solaredge, port=args.port, unit_id=args.unitid, auto_open=True)
    logger.debug('Running eventloop')
    asyncio.get_event_loop().run_until_complete(write_to_influx(args.influxdb, args.influxport, args.meters))
