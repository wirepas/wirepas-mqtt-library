# Copyright 2025 Wirepas Ltd. All Rights Reserved.
#
# See file LICENSE.txt for full license details.
#
import argparse
import logging

from wirepas_mqtt_library import WirepasGatewayMgmtInterface


def on_config_rx(data, gateway, sub_config_name):
    logging.info(f"Configuration '{sub_config_name}' has been received by {gateway}: {data}")


def on_event_rx(data, gateway, event):
    logging.info(f"Event '{event}' has been received by {gateway}: {data}")


def gw_list(value):
    try:
        return value.split(",")
    except Exception as e:
        logging.error("Provided gateway ids should be splitted with ','. Example: --gateway my_gateway,another_gateway: %s", str(e))


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
    parser.add_argument('--gateway',
                        type=gw_list,
                        default=None,
                        help="In-meter gateway ids to filter the message from. "
                        "The provided gateway ids should be splitted with ','. "
                        "Example: --gateway my_gateway,another_gateway")

    args = parser.parse_args()
    logging.basicConfig(format='%(levelname)s %(asctime)s %(message)s', level=logging.INFO)

    gw_mgmt = WirepasGatewayMgmtInterface(args.host,
                                          args.port,
                                          args.username,
                                          args.password,
                                          insecure=args.insecure)

    gw_mgmt.register_img_config_cb(on_config_rx, in_meter_gateway=args.gateway)
    gw_mgmt.register_img_event_cb(on_event_rx, in_meter_gateway=args.gateway)

    input("Insert 'enter' to stop the script!\n")
