# Copyright 2021 Wirepas Ltd. All Rights Reserved.
#
# See file LICENSE.txt for full license details.
#
from threading import Event
from time import sleep
import wirepas_mesh_messaging as wmm
import logging
from struct import pack, unpack
from random import randint


class WirepasOtapHelper:
    """Class to ease Otap operation

    This class contains methods allowing the execution of simple otap operations
    on all the sinks belonging to the same network in a single call.

    It allows to write more complex script by chaining those operations.

    """
    def __init__(self, wni, network, gateway=None):
        """Constructor

        :param wni: Wirepas network interface
        :type wni: :obj:`~wirepas_mqtt_library.wirepas_network_interface.WirepasNetworkInterface`
        :param network: Network address concerned by the otap
        :param gateway: Gateway (or list) concerned by the otap
        """
        if wni is None:
            raise RuntimeError("No Wirepas Network Interface provided")
        self.wni = wni

        self.network = network

        # Get all the sinks associated with the given network
        self._sinks = self.wni.get_sinks(self.network, gateway)

        # Get initial scratchpad status from sinks
        self._status = []
        self._update_scratchpad_status_from_all_sinks()

        # Dictionary of nodes in the network and their otap status
        # discovered since this api object is created
        self._nodes = {}

        # Register for remote API data from given network
        wni.register_data_cb(self._on_remote_api_response_received,
                             network=self.network,
                             src_ep=240,
                             dst_ep=255)

    def _update_scratchpad_status_from_all_sinks(self):
        logging.debug("Getting scratchpad status from all sinks for network %d" % self.network)
        self._status = []
        for gw, sink, config in self._sinks:
            logging.debug("Getting scratchpad status for [%s][%s] " % (gw, sink))
            try:
                res, status = self.wni.get_scratchpad_status(gw, sink)
                if res != wmm.GatewayResultCode.GW_RES_OK:
                    logging.error("Scratchpad Status error from [%s][%s]: [%s] " %  (gw, sink, res))
                    return
                logging.debug("Scratchpad status read correctly")
                self._status.append(status)
            except TimeoutError:
                logging.error("Cannot read scratchpad status from [%s][%s] => Timeout " % (gw, sink))

    def _on_remote_api_response_received(self, data):
        logging.debug("RX from %s:%s:%s => %s" % (data.gw_id, data.sink_id, data.source_address, data.data_payload))

        # We are only interested by scratchpad remote status
        response_type = unpack('<B', data.data_payload[0:1])[0]
        if response_type != 0x99:
            logging.debug("Receiving remote api that is not scratchpad status (0x: %x)" % response_type)
            return

        # Check if current status is newer
        try:
            curr = self._nodes[data.source_address]
            if curr["ts"] > data.rx_time_ms_epoch:
                logging.debug("Remote status is older than current one")
                return
        except KeyError:
            # No status yet for this node
            pass

        # Define wich version of status we received based on size
        size = unpack('<B', data.data_payload[1:2])[0]
        if size == 39:
            version = 2
        elif size == 24:
            version = 1
        elif size == 47:
            version = 3
        else:
            logging.error("Error, wrong size (%d) for Remote api response status" % size)
            return

        # Parse common part for all versions
        # Get Scratchpad present
        length, crc, seq, type, status = unpack('<IHBBB', data.data_payload[2:11])

        # Get Scratchpad that produce firmware
        scr_firm_length, scr_firm_crc, scr_firm_seq = unpack('<IHB', data.data_payload[11:18])

        # Get firmware area id
        firm_area_id = unpack('<I', data.data_payload[18:22])[0]

        stack_version = unpack('<BBBB', data.data_payload[22:26])

        new_status = {
            "ts": data.rx_time_ms_epoch,
            "seq": seq,
            "crc": crc,
            "length": length,
            "type": type,
            "status": status,
            "stack_version": stack_version,
            "stack_area_id": firm_area_id}

        # Load info included after version 1
        if version >= 2:
            new_status["app_version"] = unpack('<BBBB', data.data_payload[37:41])
            # Get Scratchpad that produce app
            scr_app_length, scr_app_crc, scr_app_seq = unpack('<IHB', data.data_payload[26:33])

            # Get firmware area id
            new_status["app_area_id"] = unpack('<I', data.data_payload[33:37])[0]

        # Load info included after version 2
        if version >= 3:
            action, target_seq, target_crc, target_delay, remaining_delay = unpack('<BBHHH', data.data_payload[41:49])
            new_status["action"] = action
            new_status["target_crc"] = target_crc
            new_status["target_seq"] = target_seq
            new_status["target_delay_m"] = target_delay
            new_status["remaining_delay_m"] = remaining_delay

        self._nodes[data.source_address] = new_status

    def load_scratchpad_to_all_sinks(self, scratchpad_file, seq=None, timeout=60, delay_between_sinks_s=1):
        """Load the specified scratchpad file on all sinks of the network

        :param scratchpad_file: scratchpad file with .otap extension
        :type scratchpad_file: str
        :param seq: optional seq for the scratchpad. If not given a random one is chosen
        :type seq: int
        :param timeout: maximum time to wait for each scratchpad upload
        :type timeout: int
        :param delay_between_sinks_s: time to wait between the sending of a load scratchpad between sinks.
                                      On large network, it may help to avoid loading the broker too much.
        :type delay_between_sinks_s: int
        :return: True if scratchpad is correctly loaded on all sinks, false otherwise
        """
        all_expected_answers = set()
        all_received = Event()
        final_result = True

        # Read the scratchpad file as binary
        try:
            with open(scratchpad_file, "rb") as f:
                scratchpad = f.read()
        except Exception as e:
            logging.error("Cannot read scratchpad file: [%s] " % str(e))
            return False

        if seq is None:
            seq = randint(1, 254)

        def _on_scratchpad_loaded_cb(gw_error_code, param):
            nonlocal final_result
            if gw_error_code != wmm.GatewayResultCode.GW_RES_OK:
                logging.error("Scratchpad loaded error on [%s][%s]: [%s] " % (param[0], param[1], gw_error_code))
                final_result = False
            else:
                logging.debug("Scratchpad loaded successfully on [%s][%s]" % (param[0], param[1]))

            try:
                all_expected_answers.remove((param[0], param[1]))
            except KeyError:
                logging.error("Answer already received: duplicated for [%s][%s]" % (param[0], param[1]))

            # Check if everybody has answered
            if all_expected_answers.__len__() == 0:
                all_received.set()

        logging.info("Loading scratchpad with seq = %d to all sinks for network %d" %(seq, self.network))
        for gw, sink, _ in self._sinks:
            logging.debug("Loading scratchpad to [%s][%s] " % (gw, sink))
            self.wni.upload_scratchpad(gw, sink, seq, scratchpad, cb=_on_scratchpad_loaded_cb, param=(gw, sink), timeout=timeout)
            all_expected_answers.add((gw, sink))
            sleep(delay_between_sinks_s)

        if not all_received.wait(timeout):
            logging.error("Some sinks do not answered within timeout %s" % all_expected_answers)
            final_result = False

        return final_result

    def process_scratchpad_on_all_sinks(self, timeout=120):
        """Process the scratchpad localy on all sinks of the network

        :param timeout: maximum time to wait for processing of scratchpad
        :type timeout: int
        :return: True if scratchpad is correctly set to be processed on all sinks, false otherwise
        """
        logging.info("Processing scratchpad on all sinks for network %d" % self.network)
        for gw, sink, config in self._sinks:
            logging.debug("Processing scratchpad to [%s][%s] " % (gw, sink))
            try:
                res = self.wni.process_scratchpad(gw, sink, timeout=timeout)
                if res != wmm.GatewayResultCode.GW_RES_OK:
                    logging.error("Scratchpad processing error: [%s] " % res)
                    return False
                logging.debug("Scratchpad processed correctly")
            except TimeoutError:
                logging.error("Cannot process scratchpad to [%s][%s] => Timeout " % (gw, sink))
                return False
        return True

    def get_stored_scratchpad_seq_list(self):
        """Retrieve a set of stored scratchpad sequence in sinks for this network

        :return: Set of stored scratchpad sequence in use in sinks
            or None if cannot be determined
        """
        # Update the list
        self._update_scratchpad_status_from_all_sinks()

        try:
            return set([i["stored_scratchpad"]['seq'] for i in self._status])
        except KeyError:
            # Shouldn't happen as stored scratchpad must be present
            logging.error("Cannot determine stored scratchpad sequences")
            return None

    def get_target_scratchpad_seq_list(self):
        """Retrieve a set of target scratchpad sequence in sinks for this network

        :return: Set of target scratchpad sequence in use in sinks
            or None if cannot be determined
        """
        self._update_scratchpad_status_from_all_sinks()

        try:
            return set([i["target_scratchpad_and_action"]['target_sequence'] for i in self._status])
        except (KeyError, TypeError):
            # Can happen if gateway or sink doesn't support target scratchpad
            logging.error("Cannot determine target scratchpad sequences (are sinks >= 5.1 and gateways >= 1.4.0 ?)")
            return None

    def _set_target_to_all_sinks(self, target):
        ret = True
        for gw, sink, config in self._sinks:
            logging.debug("Set target scratchpad to [%s][%s] " % (gw, sink))
            try:
                res = self.wni.set_target_scratchpad(gw, sink, target)
                if res != wmm.GatewayResultCode.GW_RES_OK:
                    logging.error("Set propagate scratchpad error: [%s] " % res)
                    ret = False
            except TimeoutError:
                logging.error("Cannot set target to [%s][%s] => Timeout " % (gw, sink))
                ret = False
        return ret

    def set_legacy_otap_to_all_sinks(self):
        """Set legacy action on all sinks

        :return: True if action is correctly set on all sinks, false otherwise
        """
        logging.info("Set legacy otap to all sinks for network %d" % self.network)
        target = {'action': wmm.ScratchpadAction.ACTION_LEGACY_OTAP}
        return self._set_target_to_all_sinks(target)

    def set_no_otap_to_all_sinks(self):
        """Set no otap action on all sinks

        :return: True if action is correctly set on all sinks, false otherwise
        """
        logging.info("Set no otap to all sinks for network %d" % self.network)
        target = {'action': wmm.ScratchpadAction.ACTION_NO_OTAP}
        return self._set_target_to_all_sinks(target)

    def set_propagate_scratchpad_to_all_sinks(self, seq=None, crc=None):
        """Set propagate only action on all sinks

        :param seq: Seq to set as target (If not set, seq of stored scratchpad is used)
        :param crc: Crc to set as target ((If not set, crc of stored scratchpad is used)
        :return: True if action is correctly set on all sinks, false otherwise
        """
        logging.info("Set propagate scratchpad to all sinks for network %d" % self.network)

        target = {'action': wmm.ScratchpadAction.ACTION_PROPAGATE_ONLY}
        if seq is not None:
            target['target_sequence'] = seq
        if crc is not None:
            target['target_crc'] = crc

        return self._set_target_to_all_sinks(target)

    def set_propagate_and_process_scratchpad_to_all_sinks(self, seq=None, crc=None, delay=None):
        """Set propagate and process action on all sinks

        :param seq: Seq to set as target (If not set, seq of stored scratchpad is used)
        :param crc: Crc to set as target ((If not set, crc of stored scratchpad is used)
        :param delay: delay for the processing (wmm.ProcessingDelay)
        :return: True if action is correctly set on all sinks, false otherwise
        """
        logging.info("Set propagate and process scratchpad to all sinks for network %d" % self.network)

        if delay is None:
            target = {'action': wmm.ScratchpadAction.ACTION_PROPAGATE_AND_PROCESS}
        elif isinstance(delay, wmm.ProcessingDelay):
            target = {'action': wmm.ScratchpadAction.ACTION_PROPAGATE_AND_PROCESS_WITH_DELAY,
                      'delay': delay}
        else:
            # This is a custom delay (raw format)
            target = {'action': wmm.ScratchpadAction.ACTION_PROPAGATE_AND_PROCESS_WITH_DELAY,
                      'param': delay}

        if seq is not None:
            target['target_sequence'] = seq
        if crc is not None:
            target['target_crc'] = crc

        return self._set_target_to_all_sinks(target)

    def send_remote_scratchpad_status(self, address=0xFFFFFFFF):
        """Send a remote api scratchpad status cmd to the network to trigger
        a response from nodes

        :param address: address to send the remote api command
        :return: True if command is correctly sent from all sinks, false otherwise
        """
        ret = True
        for gw, sink, config in self._sinks:
            try:
                res = self.wni.send_message(gw, sink, address, 255, 240, pack('<BB', 25, 0))
                if res != wmm.GatewayResultCode.GW_RES_OK:
                    logging.error("Cannot send data to %s:%s res=%s" % (gw, sink, res))
                    ret = False
            except TimeoutError:
                logging.error("Cannot send data to %s:%s", gw, sink)
                ret = False
        return ret

    def get_current_nodes_status(self):
        """Return dictionary of all nodes status from the network discovered since this
        helper was created

        :return: dictionary of nodes. Keys are node ids and values are dictionary with following keys:

            ===================== =============
            Key                   Value
            --------------------- -------------
            ts                    (int) timestamp when this info was received
            seq                   (int) stored scratchpad sequence
            crc                   (int) stored scratchpad crc
            length                (int) length of stored scratchpad
            type                  (int) type of stored scratchpad
            status                (int) status of stored scratchpad
            stack_version         (tuple) version of stack
            stack_area_id         (int) area id for stack
            app_version           (tuple) version of app
            app_area_id           (int) area id for app
            action                (int) current otap action
            target_crc            (int) current otap target crc
            target_seq            (int) current otap target sequence
            target_delay_m        (int) current otap delay in minutes
            remaining_delay_m     (int) current otap remaining delay in minutes
            ===================== =============

            .. note:: All keys are not necessarily present in dictionary depending on the available info
        """
        return self._nodes
