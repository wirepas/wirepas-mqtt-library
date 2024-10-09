# Copyright 2021 Wirepas Ltd. All Rights Reserved.
#
# See file LICENSE.txt for full license details.
#
import argparse
from wirepas_mqtt_library import WirepasNetworkInterface, WirepasOtapHelper
import wirepas_mesh_messaging as wmm
import logging
from random import choice
import sys

otap_delay_choices = [
    "10_minutes",
    "30_minutes",
    "1_hour",
    "6_hours",
    "1_day",
    "2_days",
    "5_days"
]

otap_delay_choices_to_internal_values = {
    otap_delay_choices[0]: wmm.ProcessingDelay.DELAY_TEN_MINUTES,
    otap_delay_choices[1]: wmm.ProcessingDelay.DELAY_THIRTY_MINUTES,
    otap_delay_choices[2]: wmm.ProcessingDelay.DELAY_ONE_HOUR,
    otap_delay_choices[3]: wmm.ProcessingDelay.DELAY_SIX_HOURS,
    otap_delay_choices[4]: wmm.ProcessingDelay.DELAY_ONE_DAY,
    otap_delay_choices[5]: wmm.ProcessingDelay.DELAY_TWO_DAYS,
    otap_delay_choices[6]: wmm.ProcessingDelay.DELAY_FIVE_DAYS
}

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
                        required=True,
                        help="Network address concerned by scratchpad")
    parser.add_argument('--file',
                        help="Scratcphad to use")

    mutual_exclusive_group_1 = parser.add_mutually_exclusive_group()
    mutual_exclusive_group_1.add_argument("--gateway",
                        type=str,
                        nargs='+',
                        help="""Gateway list to use (space separator between entries).
                        If specified, the OTAP will be performed on given Gateway IDs.
                        Warning: Mutually exclusive with --gateway-from-file option""",
                        default=None)
    mutual_exclusive_group_1.add_argument('--gateway-from-file',
                        type=argparse.FileType('r', encoding='UTF-8'),
                        help="""Gateway list to use from a file (one gateway ID per line,
                        UTF-8 encoded with line ending with LF character).
                        If specified, the OTAP will be performed on given Gateway IDs.
                        Warning: Mutually exclusive with --gateway option""")
    parser.add_argument("--otap-delay", choices=otap_delay_choices,
                        help="OTAP action delay (only valid for cmd 'delayed' or 'update_delay')")

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
    args = parser.parse_args()

    logging.basicConfig(format='%(asctime)s | [%(levelname)s] %(filename)s:%(lineno)d:%(message)s', level=args.log_level.upper(),
                        handlers=[
                            logging.StreamHandler(sys.stdout),
                            logging.FileHandler("example_otap.log", mode="w")
                        ]
    )

    logging.debug(f"Script arguments: {args}")

    wni = WirepasNetworkInterface(args.host,
                                  args.port,
                                  args.username,
                                  args.password,
                                  insecure=args.insecure,
                                  strict_mode=args.strict_mode)

    if args.cmd == "delayed" or args.cmd == "update_delay":
        if args.otap_delay is None:
            logging.error("No value given for OTAP action with delay")
            exit()
        else:
            delay = otap_delay_choices_to_internal_values[args.otap_delay]
            delay_str = args.otap_delay.replace('_',' ')

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
       logging.info("OTAP to be performed on all gateways under network %d" % args.network)
    else:
        logging.info("OTAP to be performed on %d gateways" % len(gateways))
        logging.debug(f"Gateway list {gateways}")

    otapHelper = WirepasOtapHelper(wni,
                                   args.network,
                                   gateways)

    if args.cmd == "update_delay":
        logging.info("Set new delay to %s" % delay_str)
        if not otapHelper.set_propagate_and_process_scratchpad_to_all_sinks(delay=delay):
            logging.warning("Cannot update delay on all sinks")
        exit()

    if args.cmd == "no_otap":
        logging.info("Set OTAP action to <no otap> on all sinks")
        if not otapHelper.set_no_otap_to_all_sinks():
            logging.warning("Cannot set OTAP action to <no otap> on all sinks")
        exit()

    # Optional: find a "good" sequence
    current_target_seq_set = otapHelper.get_target_scratchpad_seq_list()

    logging.info(f"Sequences already in use: {current_target_seq_set}")
    # Take a sequence from 1-254 that is not in the current set
    seq = choice([i for i in range(1,254) if i not in current_target_seq_set])

    logging.info("Sequence chosen: %d " % seq)

    if not otapHelper.load_scratchpad_to_all_sinks(args.file, seq):
        logging.warning("Cannot load scratchpad to all sinks")

    if args.cmd == "sink_only":
        logging.info("Processing scratchpad on all sinks")
        if not otapHelper.process_scratchpad_on_all_sinks():
            logging.warning("Cannot process scratchpad on all sinks")
    elif args.cmd == "propagate_only":
        logging.info("Set OTAP action to <propagate only> on all sinks")
        if not otapHelper.set_propagate_scratchpad_to_all_sinks():
            logging.warning("Cannot set OTAP action <propagate only> on all sinks")
    elif args.cmd == "immediately":
        logging.info("Set OTAP action <propagate and process> on all sinks")
        if not otapHelper.set_propagate_and_process_scratchpad_to_all_sinks():
            logging.warning("Cannot set OTAP action <propagate and process> on all sinks")
    elif args.cmd == "delayed":
        logging.info("Set OTAP action <propagate and process with %s delay> on all sinks" % delay_str)
        if not otapHelper.set_propagate_and_process_scratchpad_to_all_sinks(delay=delay):
            logging.warning("Cannot set OTAP action <propagate and process only with delay> on all sinks")
