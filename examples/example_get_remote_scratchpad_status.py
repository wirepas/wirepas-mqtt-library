# Copyright 2021 Wirepas Ltd. All Rights Reserved.
#
# See file LICENSE.txt for full license details.
#
import argparse
from wirepas_mqtt_library import WirepasNetworkInterface, WirepasOtapHelper
from datetime import datetime

import logging
import sys


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
        print(" {:5d} | {:10d} | {} | {}".format(
            id,
            node_id,
            timestamp,
            node
        ))
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
                        required=True,
                        help="Network address concerned by scratchpad")

    mutual_exclusive_group_1 = parser.add_mutually_exclusive_group()
    mutual_exclusive_group_1.add_argument("--gateway",
                        type=str,
                        nargs='+',
                        help="""Gateway list to use (space separator between entries).
                        If specified, the OTAP status will be queried for nodes connected
                        under given Gateway IDs.
                        Warning: Mutually exclusive with --gateway-from-file option""",
                        default=None)
    mutual_exclusive_group_1.add_argument('--gateway-from-file',
                        type=argparse.FileType('r', encoding='UTF-8'),
                        help="""Gateway list to use from a file (one gateway ID per line,
                        UTF-8 encoded with line ending with LF character).
                        If specified, the OTAP status will be queried for nodes connected
                        under given Gateway IDs.
                        Warning: Mutually exclusive with --gateway option""")

    # Log parameters
    parser.add_argument("--log-level", default="info", type=str,
                        choices=["debug", "info", "warning", "error", "critical"],
                        help="Default to 'info'. Log level to be displayed. "
                        "It has to be chosen between 'debug', 'info', 'warning', 'error' and 'critical'")

    # Script behavior
    parser.add_argument("--strict-mode",
                        dest='strict_mode',
                        action='store_true',
                        help="Stop execution at first generated error on gateway/sink operation")
    parser.add_argument("--query-mode",
                        action='store_true',
                        help="When provided, script will request nodes' OTAP status instead of passively collect them")
    parser.add_argument("--persist-otap-status-file",
                        type=str,
                        help="When provided, received nodes' OTAP status will be persisted.")

    args = parser.parse_args()

    logging.basicConfig(format='%(asctime)s | [%(levelname)s] %(filename)s:%(lineno)d:%(message)s', level=args.log_level.upper(),
                        handlers=[
                            logging.StreamHandler(sys.stdout),
                            logging.FileHandler("example_get_remote_scratchpad_status.log", mode="w")
                        ]
    )

    logging.debug(f"Script arguments: {args}")

    wni = WirepasNetworkInterface(args.host,
                                  args.port,
                                  args.username,
                                  args.password,
                                  insecure=args.insecure,
                                  strict_mode=args.strict_mode
                                )

    # Get gateway list
    gateways = None
    if args.gateway is None:
        if args.gateway_from_file is not None:
            # Load gateway list from file
            gateways = []
            try:
                for line in args.gateway_from_file:
                    gateways.append(line.strip('\n'))
            except ValueError:
                logging.error("Invalid file format. Must be UTF-8 with lines ending with LF character")
                exit()
    else:
        gateways = args.gateway

    if gateways is None:
       logging.info("Nodes' OTAP status can be received from all gateways under network %d" % args.network)
    else:
        logging.info(f"Nodes' OTAP status can be received from {len(gateways)} gateways under network {args.network}")
        logging.debug(f"Gateway list {gateways}")


    otapHelper = WirepasOtapHelper(wni,
                                   args.network,
                                   gateways,
                                   args.persist_otap_status_file)

    while True:
        choice = input("l to [l]ist nodes and s to [s]end remote status cmd as broadcast (if mode allows it) e to [e]xit\n")
        if choice == 'l':
            print_node_list(otapHelper.get_current_nodes_status())
            continue
        elif choice == 's':
            if args.query_mode:
                sinks = wni.get_sinks()
                otapHelper.send_remote_scratchpad_status()
            else:
                logging.warning("Operation not allowed! Please add --query-mode parameter to command line.")
            continue
        elif choice == 'e':
            break
        else:
            logging.warning("Wrong choice: s, l or e")
            continue
