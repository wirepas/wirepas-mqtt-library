# Copyright 2021 Wirepas Ltd. All Rights Reserved.
#
# See file LICENSE.txt for full license details.
#
import argparse
from wirepas_mqtt_library import WirepasNetworkInterface
import wirepas_mesh_messaging as wmm
import logging

# Hardcoded config as an example
new_config = {
    "node_role": 17, # Sink with Low Latency flag: 0x11
    "network_address": 0x1AB2C3,
    "network_channel": 13,
    # Fake keys to illustrate the concept
    "cipher_key": bytes([0x11,0x22,0x33,0x44,0x55,0x66,0x77,0x88,0x11,0x22,0x33,0x44,0x55,0x66,0x77,0x88]),
    "authentication_key": bytes([0x11,0x22,0x33,0x44,0x55,0x66,0x77,0x88,0x11,0x22,0x33,0x44,0x55,0x66,0x77,0x88]),
    "started": True
}

if __name__ == "__main__":
    """Configure a given sink on a gateway to predefined settings
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
                        dest='insecure',
                        action='store_true',
                        help="MQTT use unsecured connection")

    parser.add_argument('--gateway',
                        required=True,
                        help="Gateway the sink to configure is attached")
    
    parser.add_argument('--sink',
                        required=True,
                        help="the sink to configure")
    
    parser.add_argument('--sink_address',
                        type=int,
                        help="the sink address. Other sink settings are hardcoded inside script")
    args = parser.parse_args()

    logging.basicConfig(format='%(levelname)s %(asctime)s %(message)s', level=logging.INFO)

    wni = WirepasNetworkInterface(args.host,
                                  args.port,
                                  args.username,
                                  args.password,
                                  insecure=args.insecure)

    found = False
    for gw, sink, config in wni.get_sinks():
        if gw != args.gateway:
            continue

        if sink != args.sink:
            continue

        # Sink is founded
        found = True
        print("Sink %s:%s is found" % (args.gateway, args.sink))

        if args.sink_address is not None:
            new_config["node_address"] = args.sink_address
            
        try:
            res = wni.set_sink_config(gw, sink, new_config)
            if res != wmm.GatewayResultCode.GW_RES_OK:
                print("Cannot set new config to %s:%s res=%s" % (gw, sink, res.res))
        except TimeoutError:
            print("Cannot set new config to %s:%s" % (gw, sink))

    if not found:
        print("Cannot find the sink to configure %s:%s" % (args.gateway, args.sink))
    else:
        print("Sink %s:%s is configured" % (args.gateway, args.sink))
