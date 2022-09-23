# Copyright 2021 Wirepas Ltd. All Rights Reserved.
#
# See file LICENSE.txt for full license details.
#
import argparse
from wirepas_mqtt_library import WirepasNetworkInterface, WirepasOtapHelper
import wirepas_mesh_messaging as wmm
import logging
from random import choice

if __name__ == "__main__":
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

    # Otap parameters
    parser.add_argument("cmd", choices=["sink_only",
                                        "propagate_only",
                                        "immediately",
                                        "delayed",
                                        "update_delay",
                                        "no_otap"])

    parser.add_argument('--network',
                        type=int,
                        help="Network address concerned by scratchpad")
    parser.add_argument('--file',
                        help="Scratcphad to use")
    args = parser.parse_args()

    logging.basicConfig(format='%(levelname)s %(asctime)s %(message)s', level=logging.WARNING)

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

    if args.cmd == "update_delay":
        delay = wmm.ProcessingDelay.DELAY_THIRTY_MINUTES
        print("Set new delay to %s" % delay)
        if not otapHelper.set_propagate_and_process_scratchpad_to_all_sinks(delay=delay):
            print("Cannot update delay")
        exit()

    if args.cmd == "no_otap":
        print("Set target to no otap")
        if not otapHelper.set_no_otap_to_all_sinks():
            print("Cannot set no otap on all sinks")
        exit()

    # Optional: find a "good" sequence
    current_target_seq_set = otapHelper.get_target_scratchpad_seq_list()

    print("Sequences already in used: ", current_target_seq_set)
    # Take a sequence from 1-254 that is not in the current set
    seq = choice([i for i in range(1,254) if i not in current_target_seq_set])

    print("Sequence chosen: ", seq)

    if not otapHelper.load_scratchpad_to_all_sinks(args.file, seq):
        print("Cannot load scratchpad to all sinks")

    if args.cmd == "sink_only":
        print("Processing scratchpad on all sinks")
        if not otapHelper.process_scratchpad_on_all_sinks():
            print("Cannot process scratchpad on all sinks")
    elif args.cmd == "propagate_only":
        print("Set propagate only")
        if not otapHelper.set_propagate_scratchpad_to_all_sinks():
            print("Cannot set propagate and process")
    elif args.cmd == "immediately":
        print("Set propagate and process")
        if not otapHelper.set_propagate_and_process_scratchpad_to_all_sinks():
            print("Cannot set propagate and process only for delay %s" % delay)
    elif args.cmd == "delayed":
        print("Set propagate and process with 5 days delay")
        if not otapHelper.set_propagate_and_process_scratchpad_to_all_sinks(delay=wmm.ProcessingDelay.DELAY_FIVE_DAYS):
            print("Cannot set propagate and process only for 5 days")


