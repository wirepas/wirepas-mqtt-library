# Copyright 2021 Wirepas Ltd. All Rights Reserved.
#
# See file LICENSE.txt for full license details.
#
import argparse
from wirepas_mqtt_library import WirepasNetworkInterface, WirepasOtapHelper
from datetime import datetime

import logging


def type_to_str(type_int):
    map_to_str = ("Blank", "Present", "Process")
    try:
        return map_to_str[type_int]
    except IndexError:
        return "Unknown {}".format(type_int)


def status_to_str(status):
    if status == 255:
        return "New"
    if status == 0:
        return "Success"

    return "Error {}".format(status)


def print_node_list(nodes):
    print("\nList of nodes:")

    id = 0
    for node_id in list(nodes):
        # Convert utc time to date
        node = nodes[node_id]
        timestamp = datetime.utcfromtimestamp(node["ts"] / 1000.0)
        print(" {:5d} | {:10d} | {} | {}".format(id, node_id, timestamp, node))
        id += 1

    print()


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

    otapHelper = WirepasOtapHelper(wni,
                                   args.network)

    while True:
        choice = input("l to [l]ist nodes and s to [s]end remote status cmd as broadcast e to [e]xit\n")
        if choice == 'l':
            print_node_list(otapHelper.get_current_nodes_status())
            continue
        elif choice == 's':
            sinks = wni.get_sinks()
            otapHelper.send_remote_scratchpad_status()
            continue
        elif choice == 'e':
            break
        else:
            print("Wrong choice: s, l or e")
            continue
