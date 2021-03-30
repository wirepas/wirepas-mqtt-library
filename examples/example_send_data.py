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

    def on_data_received(data):
        print("RX from %s:%s => %s" % (data.gw_id, data.sink_id, data.data_payload))

    # Register for any data
    wni.register_data_cb(on_data_received)

    while True:
        message = input("Write a message to send to all sinks\n")
        for gw, sink, config in wni.get_sinks():
            try:
                res = wni.send_message(gw, sink, config["node_address"], 1, 1, message.encode())
                if res != wmm.GatewayResultCode.GW_RES_OK:
                    print("Cannot send data to %s:%s res=%s" % (gw, sink, res))
            except TimeoutError:
                print("Cannot send data to %s:%s", gw, sink)
