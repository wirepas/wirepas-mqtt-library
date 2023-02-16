# Copyright 2021 Wirepas Ltd. All Rights Reserved.
#
# See file LICENSE.txt for full license details.
#
import argparse
from wirepas_mqtt_library import WirepasNetworkInterface
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

    args = parser.parse_args()

    logging.basicConfig(format='%(levelname)s %(asctime)s %(message)s', level=logging.INFO)

    wni = WirepasNetworkInterface(args.host,
                                  args.port,
                                  args.username,
                                  args.password,
                                  insecure=args.insecure)

    def on_downlink_data_received(data, res):
        logging.info("Message to the Mesh network: %s", data)
        logging.info("Status: %s", res)

    def on_uplink_data_transmitted(data):
        logging.info("Message from the Mesh network: %s", data)

    # Register a callback for downlink traffic
    wni.register_downlink_traffic_cb(on_downlink_data_received)
    
    # Register a callback for uplink traffic
    wni.register_uplink_traffic_cb(on_uplink_data_transmitted)

    while True:
        message = input("Write a message to send to all sinks\n")
        for gw, sink, config in wni.get_sinks():
            try:
                res = wni.send_message(gw, sink, config["node_address"], 1, 1, message.encode())
                if res != wmm.GatewayResultCode.GW_RES_OK:
                    print("Cannot send data to %s:%s res=%s" % (gw, sink, res))
            except TimeoutError:
                print("Cannot send data to %s:%s", gw, sink)
