# Copyright 2021 Wirepas Ltd. All Rights Reserved.
#
# See file LICENSE.txt for full license details.
#
import argparse
from wirepas_mqtt_library import WirepasNetworkInterface
from time import sleep, strftime
import os

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
                        help="Network address concerned by scratchpad")

    parser.add_argument('--delay_s',
                        type=int,
                        help="Delay in seconds to listen for network traffic and discover nodes")

    parser.add_argument('--output_folder',
                        help="Folder to store file containing list of nodes at the end, "
                             "otherwise list is printed on standard output")

    args = parser.parse_args()

    logging.basicConfig(format='%(levelname)s %(asctime)s %(message)s', level=logging.INFO)

    wni = WirepasNetworkInterface(args.host,
                                  args.port,
                                  args.username,
                                  args.password,
                                  insecure=args.insecure)

    if args.network is None:
        print("No network address provided")
        exit()

    if args.delay_s is None:
        print("No delay given to listen the network")
        exit()

    nodes = set()

    def on_data_rx(data):
        nodes.add(data.source_address)

    wni.register_data_cb(on_data_rx, network=args.network)

    print("Waiting for %d s to discover nodes" % args.delay_s)
    sleep(args.delay_s)

    print("In %d s, %d nodes were found in network 0x%x" % (args.delay_s, nodes.__len__(), args.network))

    if args.output_folder is not None:
        file = os.path.join(args.output_folder, "list_nodes_" + strftime("%Y%m%d-%H%M%S"))
        with open(file, "w") as f:
            for node in nodes:
                f.write("%d\n" % node)
        print("Result written to %s" % file)
    else:
        print(nodes)
