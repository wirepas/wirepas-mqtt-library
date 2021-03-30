# Copyright 2021 Wirepas Ltd. All Rights Reserved.
#
# See file LICENSE.txt for full license details.
#
import argparse
from wirepas_mqtt_library import WirepasNetworkInterface
import logging
import datetime

if __name__ == "__main__":
    parser = argparse.ArgumentParser(fromfile_prefix_chars='@')
    parser.add_argument('--host',
                        help="MQTT broker address")
    parser.add_argument('--port',
                        type=int,
                        default=8883,
                        help="MQTT broker port")
    parser.add_argument('--username',
                        help="MQTT broker username")
    parser.add_argument('--password',
                        help="MQTT broker password")
    parser.add_argument('--insecure',
                        dest='insecure',
                        action='store_true',
                        help="MQTT use unsecured connection")

    args = parser.parse_args()

    logging.basicConfig(format='%(levelname)s %(asctime)s %(message)s', level=logging.WARNING)

    wni = WirepasNetworkInterface(args.host,
                                  args.port,
                                  args.username,
                                  args.password,
                                  insecure=args.insecure)

    def on_config_changed():
        line = ""
        for gw, sink, _ in wni.get_sinks():
            line += "[%s:%s] " % (gw, sink)

        print(datetime.datetime.now())
        print(line)

    wni.set_config_changed_cb(on_config_changed)

    input()
