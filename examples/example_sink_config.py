# Copyright 2021 Wirepas Ltd. All Rights Reserved.
#
# See file LICENSE.txt for full license details.
#
import argparse
from wirepas_mqtt_library import WirepasNetworkInterface
import wirepas_mesh_messaging as wmm
import logging

if __name__ == "__main__":
    """Example that set a new network channel for all sinks of a given network
    """
    parser = argparse.ArgumentParser(fromfile_prefix_chars='@')
    # Mqtt parameters
    parser.add_argument('--host',
                        help="MQTT broker address")
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

    parser.add_argument('--network',
                        type=int,
                        help="Network address concerned by update")

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

    for gw, sink, config in wni.get_sinks(network_address=args.network):
        print("Set new config to %s:%s" % (gw, sink))
        try:
            res = wni.set_sink_config(gw, sink, {"network_channel": 10})
            if res != wmm.GatewayResultCode.GW_RES_OK:
                print("Cannot set new config to %s:%s res=%s" % (gw, sink, res))
        except TimeoutError:
            print("Cannot set new config to %s:%s" % (gw, sink))

    print("All done")
