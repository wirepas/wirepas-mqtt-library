# Copyright 2025 Wirepas Ltd. All Rights Reserved.
#
# See file LICENSE.txt for full license details.
#
import argparse
import logging

from wirepas_mqtt_library import WirepasGatewayMgmtInterface


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
                        type=str,
                        help="In-meter gateway id to send the request to.")

    args = parser.parse_args()
    logging.basicConfig(format='%(levelname)s %(asctime)s %(message)s', level=logging.INFO)
    gw_mgmt = WirepasGatewayMgmtInterface(args.host,
                                          args.port,
                                          args.username,
                                          args.password,
                                          insecure=args.insecure)

    gw_mgmt.img_reset_request(args.gateway)
