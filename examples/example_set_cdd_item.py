# Copyright 2025 Wirepas Ltd. All Rights Reserved.
#
# See file LICENSE.txt for full license details.
#
import argparse
from wirepas_mqtt_library import WirepasNetworkInterface
import wirepas_mesh_messaging as wmm
import logging

if __name__ == "__main__":
    """Sets a configuration data item for all sinks of a given gateway
    """
    parser = argparse.ArgumentParser(fromfile_prefix_chars='@')
    parser.add_argument('--host',
                        help="MQTT broker address",
                        required=True)
    parser.add_argument('--port', default=8883,
                        type=int,
                        help="MQTT broker port")
    parser.add_argument('--username',
                        help="MQTT broker username")
    parser.add_argument('--password',
                        help="MQTT broker password")
    parser.add_argument('--insecure',
                        dest='insecure', action='store_true',
                        help="MQTT use unsecured connection")

    parser.add_argument('--gateway',
                        help="ID of the gateway concerned by update",
                        required=True)
    parser.add_argument('--endpoint',
                        type=lambda x: int(x, 0),
                        help="CDD endpoint",
                        required=True)
    parser.add_argument('--payload',
                        default="",
                        help="CDD payload as a hex string")

    args = parser.parse_args()

    logging.basicConfig(format='%(levelname)s %(asctime)s %(message)s', level=logging.INFO)

    wni = WirepasNetworkInterface(args.host,
                                  args.port,
                                  args.username,
                                  args.password,
                                  insecure=args.insecure)

    for gw, sink, config in wni.get_sinks(gateway=args.gateway):
        print("Set new configuration data item on %s:%s with endpoint: 0x%04X" % (gw, sink, args.endpoint))
        try:
            res = wni.set_configuration_data_item(gw, sink, args.endpoint, bytes.fromhex(args.payload))
            if res != wmm.GatewayResultCode.GW_RES_OK:
                print("Cannot set configuration data item on %s:%s res=%s" % (gw, sink, res))
        except TimeoutError:
            print("Timeout when setting configuration data item on %s:%s" % (gw, sink))

    print("All done")

