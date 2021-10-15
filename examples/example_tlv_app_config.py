# Copyright 2021 Wirepas Ltd. All Rights Reserved.
#
# See file LICENSE.txt for full license details.
#
import argparse
from wirepas_mqtt_library import WirepasNetworkInterface, WirepasTLVAppConfigHelper
import wirepas_mesh_messaging as wmm

import logging

if __name__ == "__main__":
    parser = argparse.ArgumentParser(fromfile_prefix_chars='@')
    parser.add_argument('--host',
                        help="MQTT broker address")
    parser.add_argument('--port', default=8883,
                        type=int,
                        help="MQTT broker port")
    parser.add_argument('--username', default='mqttmasteruser',
                        help="MQTT broker username")
    parser.add_argument('--password',
                        help="MQTT broker password")
    parser.add_argument('--insecure',
                        dest='insecure',
                        action='store_true',
                        help="MQTT use unsecured connection")

    parser.add_argument('--network',
                        type=int,
                        help="Network address concerned by update",
                        required=True)

    args = parser.parse_args()

    logging.basicConfig(format='%(levelname)s %(asctime)s %(message)s', level=logging.INFO)

    wni = WirepasNetworkInterface(args.host,
                                  args.port,
                                  args.username,
                                  args.password,
                                  insecure=args.insecure)
    
    app_config_helper = WirepasTLVAppConfigHelper(wni,
                                                  network=args.network)

    # Display current app config
    print(app_config_helper)

    # The following example will add two new entries in TLV format in all app config from
    # network args.network and remove one if it exists.
    # It will generate an error and not update the app config if one of the gateway doesn't
    # have enough room for it
    res = app_config_helper.add_raw_entry(0x6666, b'\x00\x00\x12\x34')\
                           .add_raw_entry(0x32, b'\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A')\
                           .remove_raw_entry(0x55555)\
                           .update_entries()

    print("Updating app_config result = %s" % res)

    # Display new config
    print(app_config_helper)
