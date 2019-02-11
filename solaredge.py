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


async def write_to_influx(dbhost, dbport, mbmeters, dbname='solaredge'):
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
            reg_block = client.read_holding_registers(40069, 38)
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
                # reg_block[27] = DC Current value
                # reg_block[28] = DC Current scale factor
                # reg_block[29] = DC Voltage value
                # reg_block[30] = DC Voltage scale factor
                # reg_block[31] = DC Power value
                # reg_block[32] = DC Power scale factor
                # reg_block[34] = Inverter temp
                # reg_block[37] = Inverter temp scale factor
                datapoint['tags']['inverter'] = 1

                # AC Current
                logger.debug(f'Block6: {str(reg_block[6])}')
                logger.debug(f'AC Current SF: {str(np.int16(reg_block[6]))}')
                scalefactor = np.float_power(10,np.int16(reg_block[6]))
                logger.debug(f'AC Current mult: {str(scalefactor)}')
                if reg_block[2]<65535:
                    datapoint['fields']['AC Total Current'] = trunc_float(reg_block[2] * scalefactor)
                if reg_block[3] <65535:
                    datapoint['fields']['AC Current phase A'] = trunc_float(reg_block[3] * scalefactor)
                if reg_block[4]<65535:
                    datapoint['fields']['AC Current phase B'] = trunc_float(reg_block[4] * scalefactor)
                if reg_block[5]<65535:
                    datapoint['fields']['AC Current phase C'] = trunc_float(reg_block[5] * scalefactor)

                # AC Voltage
                logger.debug(f'Block13: {str(reg_block[13])}')
                logger.debug(f'AC Voltage SF: {str(np.int16(reg_block[13]))}')
                scalefactor = np.float_power(10,np.int16(reg_block[13]))
                logger.debug(f'AC Voltage mult: {str(scalefactor)}')
                if reg_block[7]<65535:
                    datapoint['fields']['AC Voltage phase A-B'] = trunc_float(reg_block[7] * scalefactor)
                if reg_block[8]<65535:
                    datapoint['fields']['AC Voltage phase B-C'] = trunc_float(reg_block[8] * scalefactor)
                if reg_block[9]<65535:
                    datapoint['fields']['AC Voltage phase C-A'] = trunc_float(reg_block[9] * scalefactor)
                if reg_block[10]<65535:
                    datapoint['fields']['AC Voltage phase A-N'] = trunc_float(reg_block[10] * scalefactor)
                if reg_block[11]<65535:
                    datapoint['fields']['AC Voltage phase B-N'] = trunc_float(reg_block[11] * scalefactor)
                if reg_block[12]<65535:
                    datapoint['fields']['AC Voltage phase C-N'] = trunc_float(reg_block[12] * scalefactor)

                # AC Frequency
                logger.debug(f'AC Frequency SF: {str(np.int16(reg_block[17]))}')
                scalefactor = np.float_power(10,np.int16(reg_block[17]))
                if reg_block[16]<65535:
                    datapoint['fields']['AC Frequency'] = trunc_float(reg_block[16] * scalefactor)

                    
                # AC Power
                logger.debug(f'Block15: {str(reg_block[15])}')
                logger.debug(f'AC Power SF: {str(np.int16(reg_block[15]))}')
                scalefactor = np.float_power(10,np.int16(reg_block[15]))
                logger.debug(f'AC Power mult: {str(scalefactor)}')
                if reg_block[14]<65535:
                    datapoint['fields']['AC Power output'] = trunc_float(reg_block[14] * scalefactor)

                # DC Current
                logger.debug(f'Block28: {str(reg_block[28])}')
                logger.debug(f'DC Current SF: {str(np.int16(reg_block[28]))}')
                scalefactor = np.float_power(10,np.int16(reg_block[28]))
                logger.debug(f'DC Current mult: {str(scalefactor)}')
                if reg_block[27]<65535:
                    datapoint['fields']['DC Current'] = trunc_float(reg_block[27] * scalefactor)

                # DC Voltage
                logger.debug(f'Block30: {str(reg_block[30])}')
                logger.debug(f'DC voltage SF: {str(np.int16(reg_block[30]))}')
                scalefactor = np.float_power(10,np.int16(reg_block[30]))
                logger.debug(f'DC Voltage mult: {str(scalefactor)}')
                if reg_block[29]<65535:
                    datapoint['fields']['DC Voltage'] = trunc_float(reg_block[29] * scalefactor)

                # DC Power
                logger.debug(f'Block32: {str(reg_block[32])}')
                logger.debug(f'DC Power SF: {str(np.int16(reg_block[32]))}')
                scalefactor = np.float_power(10,np.int16(reg_block[32]))
                logger.debug(f'DC Power mult: {str(scalefactor)}')
                if reg_block[31]<65535:
                    datapoint['fields']['DC Power input'] = trunc_float(reg_block[31] * scalefactor)

                # Inverter Temp 
                logger.debug(f'Block37: {str(reg_block[37])}')
                logger.debug(f'Temp SF: {str(np.int16(reg_block[37]))}')
                scalefactor = np.float_power(10,np.int16(reg_block[37]))
                logger.debug(f'Temp mult: {str(scalefactor)}')
                if reg_block[34]<65535:
                    datapoint['fields']['Inverter Temperature'] = trunc_float(reg_block[34] * scalefactor)

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
                    reg_block = client.read_holding_registers(40190, 36)
                if x==2:
                    reg_block = client.read_holding_registers(40364, 36)
                if x==3:
                    reg_block = client.read_holding_registers(40539, 36)
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
                    
                    logger.debug(f'meter reg_block: {str(reg_block)}')

                    # AC Current
                    logger.debug(f'AC Current SF: {str(np.int16(reg_block[4]))}')
                    scalefactor = np.float_power(10,np.int16(reg_block[4]))
                    datapoint['fields']['AC Total Current'] = trunc_float(np.int16(reg_block[0]) * scalefactor)
                    datapoint['fields']['AC Current phase A'] = trunc_float(np.int16(reg_block[1]) * scalefactor)
                    datapoint['fields']['AC Current phase B'] = trunc_float(np.int16(reg_block[2]) * scalefactor)
                    datapoint['fields']['AC Current phase C'] = trunc_float(np.int16(reg_block[3]) * scalefactor)

                    # AC Voltage
                    logger.debug(f'AC Voltage SF: {str(np.int16(reg_block[13]))}')
                    scalefactor = np.float_power(10,np.int16(reg_block[13]))
                    datapoint['fields']['AC Voltage phase L-N'] = trunc_float(np.int16(reg_block[5]) * scalefactor)
                    datapoint['fields']['AC Voltage phase A-N'] = trunc_float(np.int16(reg_block[6]) * scalefactor)
                    datapoint['fields']['AC Voltage phase B-N'] = trunc_float(np.int16(reg_block[7]) * scalefactor)
                    datapoint['fields']['AC Voltage phase C-N'] = trunc_float(np.int16(reg_block[8]) * scalefactor)
                    datapoint['fields']['AC Voltage phase L-L'] = trunc_float(np.int16(reg_block[9]) * scalefactor)
                    datapoint['fields']['AC Voltage phase A-B'] = trunc_float(np.int16(reg_block[10]) * scalefactor)
                    datapoint['fields']['AC Voltage phase B-C'] = trunc_float(np.int16(reg_block[11]) * scalefactor)
                    datapoint['fields']['AC Voltage phase C-A'] = trunc_float(np.int16(reg_block[12]) * scalefactor)

                    # AC Frequency
                    logger.debug(f'AC Frequency SF: {str(np.int16(reg_block[15]))}')
                    scalefactor = np.float_power(10,np.int16(reg_block[15]))
                    datapoint['fields']['AC Frequency'] = trunc_float(np.int16(reg_block[14]) * scalefactor)
                    
                    # AC Real Power
                    logger.debug(f'AC Real Power SF: {str(np.int16(reg_block[20]))}')
                    scalefactor = np.float_power(10,np.int16(reg_block[20]))
                    datapoint['fields']['AC Total Real Power'] = trunc_float(np.int16(reg_block[16]) * scalefactor)
                    datapoint['fields']['AC Real Power Phase A'] = trunc_float(np.int16(reg_block[17]) * scalefactor)
                    datapoint['fields']['AC Real Power Phase B'] = trunc_float(np.int16(reg_block[18]) * scalefactor)
                    datapoint['fields']['AC Real Power Phase C'] = trunc_float(np.int16(reg_block[19]) * scalefactor)
                    
                    # AC Apparent Power
                    logger.debug(f'AC Apparent Power SF: {str(np.int16(reg_block[25]))}')
                    scalefactor = np.float_power(10,np.int16(reg_block[25]))
                    datapoint['fields']['AC Total Apparent Power'] = trunc_float(np.int16(reg_block[21]) * scalefactor)
                    datapoint['fields']['AC Apparent Power Phase A'] = trunc_float(np.int16(reg_block[22]) * scalefactor)
                    datapoint['fields']['AC Apparent Power Phase B'] = trunc_float(np.int16(reg_block[23]) * scalefactor)
                    datapoint['fields']['AC Apparent Power Phase C'] = trunc_float(np.int16(reg_block[24]) * scalefactor)

                    # AC Reactive Power
                    logger.debug(f'AC Reactive Power SF: {str(np.int16(reg_block[30]))}')
                    scalefactor = np.float_power(10,np.int16(reg_block[30]))
                    datapoint['fields']['AC Total Reactive Power'] = trunc_float(np.int16(reg_block[26]) * scalefactor)
                    datapoint['fields']['AC Reactive Power Phase A'] = trunc_float(np.int16(reg_block[27]) * scalefactor)
                    datapoint['fields']['AC Reactive Power Phase B'] = trunc_float(np.int16(reg_block[28]) * scalefactor)
                    datapoint['fields']['AC Reactive Power Phase C'] = trunc_float(np.int16(reg_block[29]) * scalefactor)

                    # AC Power Factor
                    logger.debug(f'AC Power Factor SF: {str(np.int16(reg_block[30]))}')
                    scalefactor = np.float_power(10,np.int16(reg_block[35]))
                    datapoint['fields']['AC Average Power Factor'] = trunc_float(np.int16(reg_block[31]) * scalefactor)
                    datapoint['fields']['AC Power Factor Phase A'] = trunc_float(np.int16(reg_block[32]) * scalefactor)
                    datapoint['fields']['AC Power Factor Phase B'] = trunc_float(np.int16(reg_block[33]) * scalefactor)
                    datapoint['fields']['AC Power Factor Phase C'] = trunc_float(np.int16(reg_block[34]) * scalefactor)

                    
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
    parser.add_argument('--influxdb', default='localhost')
    parser.add_argument('--influxport', type=int, default=8086)
    parser.add_argument('--port', type=int, default=502, help='ModBus TCP port number to use')
    parser.add_argument('--unitid', type=int, default=1, help='ModBus unit id to use in communication')
    parser.add_argument('--meters', type=int, default=0, help='Number of ModBus meters attached to inverter (0-3)')
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
