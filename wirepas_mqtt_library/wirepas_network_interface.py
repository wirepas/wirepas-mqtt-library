# Copyright 2021 Wirepas Ltd. All Rights Reserved.
#
# See file LICENSE.txt for full license details.
#
import ssl
import logging

import paho.mqtt.client as mqtt
from paho.mqtt.client import connack_string
import wirepas_mesh_messaging as wmm
from .topic_helper import TopicGenerator, TopicParser
from threading import Event, Thread
from time import sleep, time
from queue import Queue


class WirepasNetworkInterface:
    """Class to interact with a Wirepas network through an MQTT broker.

    This class wraps the MQTT connection and the messages creation / parsing in protobuf
    format.

    Most of the calls can be called in a blocking mode (ie, blocking until gateway response is received)
    or in an asynchronous way by specifying a callback to receive the response.

    """
    # Timeout in s to wait for status message after subscription to
    # topic and connection is established
    _TIMEOUT_GW_STATUS_S = 1

    # Timeout in s to receive config from gw
    _TIMEOUT_GW_CONFIG_S = 2

    # Inner class definition to define a gateway
    class _Gateway:
        def __init__(self, id, online=False, sinks=[]):
            self.id = id
            self.online = online
            self.sinks = sinks
            self.config_received_event = Event()

    def __init__(self, host, port, username, password, insecure=False, num_worker_thread=1, strict_mode=True):
        """Constructor

        :param host: MQTT broker host address
        :param port: MQTT broker port
        :param username: MQTT username
        :param password: MQTT password
        :param insecure: if set to true, TLS handshake with broker is skipped
        :param num_worker_thread: number of thread to use to handle asynchronous event (like data reception)
        :param strict_mode: if strict_mode is set to false, some errors do not generate exceptions but only an error message
        """
        # Create an MQTT client
        self._mqtt_client = mqtt.Client()

        if not insecure:
            self._mqtt_client.tls_set(ca_certs=None, certfile=None, keyfile=None,
                                      cert_reqs=ssl.CERT_REQUIRED,
                                      tls_version=ssl.PROTOCOL_TLSv1_2, ciphers=None)

        self._mqtt_client.username_pw_set(username,
                                          password)

        self._mqtt_client.on_connect = self._on_connect_callback
        self._mqtt_client.on_disconnect = self._on_disconnect_callback

        # Reduce a bit keepalive for faster connection break detection
        self._mqtt_client.connect(host, int(port), keepalive=20)
        self._mqtt_client.loop_start()

        # Variable to store connection time
        self._connected = Event()
        self._connection_time = None

        # Flag to determine if we waited at least one time
        # for the full config
        self._initial_discovery_done = False

        self._gateways = {}
        # Dictionary to store request id, associated cb
        self._ongoing_requests = {}

        self._data_filters = {}
        self._on_config_changed_cb = None

        # Create rx queue and start dispatch thread
        self._task_queue = _TaskQueue(num_worker_thread)

        self._strict_mode = strict_mode

    def _on_connect_callback(self, client, userdata, flags, rc):
        if rc != 0:
            logging.error("MQTT cannot connect: %s (%s)", connack_string(rc), rc)
            return

        # Register for Gateway status topic
        all_gateway_status_topic = TopicGenerator.make_status_topic()
        self._mqtt_client.subscribe(all_gateway_status_topic, qos=1)
        self._mqtt_client.message_callback_add(all_gateway_status_topic,
                                               self._on_status_gateway_received)

        # Register for Data topic
        all_data_topic = TopicGenerator.make_received_data_topic()
        self._mqtt_client.subscribe(all_data_topic, qos=1)
        self._mqtt_client.message_callback_add(all_data_topic,
                                               self._on_data_received)

        # Register for all responses
        all_responses_topic = "gw-response/#"
        self._mqtt_client.subscribe(all_responses_topic, 1)
        self._mqtt_client.message_callback_add(all_responses_topic,
                                               self._on_response_received)

        self._connection_time = time()

        logging.info("..Connected to MQTT")
        self._connected.set()

    def _on_disconnect_callback(self, client, userdata, rc):
        logging.info("..MQTT disconnected rc={}".format(rc))
        self._connected.clear()

    def _on_status_gateway_received(self, client, userdata, message):
        try:
            status = wmm.StatusEvent.from_payload(message.payload)

            if status.state == wmm.GatewayState.ONLINE:
                self._gateways[status.gw_id] = self._Gateway(status.gw_id, True)
                request = wmm.GetConfigsRequest()
                self._publish(TopicGenerator.make_get_configs_request_topic(status.gw_id),
                              request.payload,
                              1)

                # Call update gateway config when receiving it
                self._wait_for_response(self._update_gateway_configs, request.req_id)
            else:
                # Gateway is offline, remove config (Override any old config)
                self._gateways[status.gw_id] = self._Gateway(status.gw_id, False)
                # Config has changed, notify any subscriber
                if self._on_config_changed_cb is not None:
                    self._task_queue.add_task(self._on_config_changed_cb)

        except wmm.GatewayAPIParsingException as e:
            logging.error(str(e))
            return

    def _update_gateway_configs(self, config):
        try:
            gw = self._gateways[config.gw_id]
            gw.sinks = config.configs
            gw.config_received_event.set()
        except KeyError:
            logging.error("Receiving gateway config that is unknown %s", config.gw_id)

        # Config has changed, notify any subscriber
        if self._on_config_changed_cb is not None:
            self._task_queue.add_task(self._on_config_changed_cb)

    def _on_data_received(self, client, userdata, message):
        try:
            data = wmm.ReceivedDataEvent.from_payload(message.payload)
            if data.network_address is None:
                # Network id is from topic
                _, _, network_address, _, _ = TopicParser.parse_received_data_topic(message.topic)
                data.network_address = network_address
            self._task_queue.add_task(self._dispatch_data, data)
        except wmm.GatewayAPIParsingException as e:
            logging.error(str(e))

    def _dispatch_data(self, data):
        for f in self._data_filters.values():
            f.filter_and_dispatch(data)

    def _publish(self, topic, payload, qos=1):
        try:
            self._mqtt_client.publish(topic,
                                      payload,
                                      qos=qos)
        except Exception as e:
            logging.error(str(e))

    def _call_cb(self, response, *args):
        try:
            cb, param = self._ongoing_requests[response.req_id]
            # Add caller param after response error code and add any additional param at the end
            cb(response.res, param, *args)
            # Cb called, remove key
            del self._ongoing_requests[response.req_id]
        except KeyError:
            # No cb set, just pass (could be timeout that expired)
            logging.debug("Response but no associated cb: %s" % response.req_id)
            pass

    def _on_response_received(self, client, userdata, message):
        # Topic are as followed: gw-response/cmd/...
        cmd = message.topic.split("/")[1]
        logging.debug("Response for: %s" % cmd)
        handler = self._call_cb
        additional_params = None
        try:
            if cmd == "send_data":
                response = wmm.SendDataResponse.from_payload(message.payload)
            elif cmd == "get_configs":
                response = wmm.GetConfigsResponse.from_payload(message.payload)
                # Get config are consumed internally, no external cb to call
                handler = self._update_gateway_configs
            elif cmd == "set_config":
                response = wmm.SetConfigResponse.from_payload(message.payload)
            elif cmd == "otap_load_scratchpad":
                response = wmm.UploadScratchpadResponse.from_payload(message.payload)
            elif cmd == "otap_set_target_scratchpad":
                response = wmm.SetScratchpadTargetAndActionResponse.from_payload(message.payload)
            elif cmd == "otap_process_scratchpad":
                response = wmm.ProcessScratchpadResponse.from_payload(message.payload)
            elif cmd == "otap_status":
                response = wmm.GetScratchpadStatusResponse.from_payload(message.payload)
                status = None
                if response.res == wmm.GatewayResultCode.GW_RES_OK:
                    status = {
                        'stored_scratchpad': response.stored_scratchpad,
                        'stored_status': response.stored_status,
                        'stored_type': response.stored_type,
                        'processed_scratchpad': response.processed_scratchpad,
                        'target_scratchpad_and_action': response.target_scratchpad_and_action
                    }
                additional_params = status
            else:
                logging.debug("Untracked response type %s" % cmd)
                return

            if additional_params is None:
                self._task_queue.add_task(handler, response)
            else:
                self._task_queue.add_task(handler, response, additional_params)

        except wmm.GatewayAPIParsingException as e:
            logging.error(str(e))

    def _wait_for_configs(fn):
        # Decorator to handle waiting for enough time for the setup
        # of the network interface
        def wrapper(*args, **kwargs):
            # args[0] is self
            if not args[0]._initial_discovery_done:
                # Wait for connection
                args[0]._connected.wait(1)
                if not args[0]._connected.is_set():
                    raise TimeoutError("Cannot connect to broker")

                # Check if enough time was elapsed since we connect to receive all status
                delay = (args[0]._connection_time + args[0]._TIMEOUT_GW_STATUS_S) - time()
                if delay > 0:
                    sleep(delay)

                # Check if we are still waiting for configs
                for gw in args[0]._gateways.values():
                    if gw.online:
                        gw.config_received_event.wait(args[0]._TIMEOUT_GW_CONFIG_S)
                        if not gw.config_received_event.is_set():
                            logging.error("Config timeout for gw %s" % gw.id)
                            # Mark the config as received to avoid waiting for it next time
                            # It may still come later
                            gw.config_received_event.set()
                            if args[0]._strict_mode:
                                raise TimeoutError("Cannot get config from online GW %s" % gw.id)

                # Discovery done, no need to do it again
                args[0]._initial_discovery_done = True

            return fn(*args, **kwargs)
        wrapper.__doc__ = fn.__doc__
        return wrapper

    @_wait_for_configs
    def get_sinks(self, network_address=None, gateway=None):
        """
        get_sinks(self, network_address=None, gateway=None)
        Get list of current sink connected to this broker

        :param network_address: If specified, only sinks with matching network addresses are returned
        :param gateway: gateway or list of gateways to get sinks
        :return: List of sinks as tuple (gateway_id, sink_id, config)

            Config is dictionary with following keys

            Keys are:

            ===================== =============
            Key                   Value
            --------------------- -------------
            sink_id               (str) Unique id of sink on the gateway
            node_role             (int) Role of node
            node_address          (int) Address of node
            network_address       (int) Network address
            network_channel       (int) Network channel
            app_config_diag       (int) Diag interval
            app_config_seq        (int) Diag sequence
            app_config_data       (bytearray) App config data
            are_keys_set          (bool) Tells if network keys are configured
            started               (bool) Is stack started
            current_ac_range_min  (int) Current minimum access cycle
            current_ac_range_max  (int) Current maximum access cycle
            min_ac                (int) Minimum access cycle value allowed
            max_ac                (int) Maximum access cycle value allowed
            max_mtu               (int) Maximum packet size
            min_ch                (int) Minimum channel
            max_ch                (int) Maximum channel
            hw_magic              (int) Hardware magic
            stack_profile         (int) Stack profile
            app_config_max_size   (int) Maximum size for app config
            firmware_version      (tuple) Firmware version of node
            ===================== =============

            .. note:: node_role encoding is same as in dualmcu protocol

        :raises TimeoutError: If a gateway doesn't send its initial config fast enough

        """
        sinks = list()
        for gw in self._gateways.values():
            if not gw.online:
                continue

            if gateway is not None and gw.id not in gateway:
                continue

            for sink in gw.sinks:
                if network_address is not None and sink['network_address'] != network_address:
                    continue
                sinks.append((gw.id, sink['sink_id'], sink))

        return sinks

    @_wait_for_configs
    def get_gateways(self, only_online=True):
        """
        get_gateways(self, only_online=True)
        Get list of current gateways connected to this broker

        :param only_online: if True (default) only online gateways are returned
        :return: List of gateway name

        """
        if only_online:
            gateways = list(filter(lambda x: self._gateways[x].online, self._gateways))
        else:
            gateways = self._gateways.keys()

        return gateways

    def _wait_for_response(self, cb, req_id, timeout=2, param=None):
        if cb is not None:
            # Unblocking call, cb will be called later
            self._ongoing_requests[req_id] = cb, param
            return None

        # No cb specified so blocking call
        res = None
        response_event = Event()

        def unlock(response, param, *args):
            nonlocal res

            if args.__len__() == 0:
                res = response
            else:
                res = response, *args
            response_event.set()

        self._ongoing_requests[req_id] = unlock, None
        if not response_event.wait(timeout):
            # Timeout
            del self._ongoing_requests[req_id]
            raise TimeoutError("Cannot get response for request")

        return res

    def send_message(self, gw_id, sink_id, dest, src_ep, dst_ep, payload, qos=0, csma_ca_only=False, cb=None, param=None):
        """
        send_message(self, gw_id, sink_id, dest, src_ep, dst_ep, payload, qos=0, csma_ca_only=False, cb=None, param=None)
        Send a message from a sink

        :param gw_id: Id of gateway the sink is attached
        :type gw_id: str
        :param sink_id: Id of sink
        :type sink_id: str
        :param dest: destination address
        :param src_ep: source endpoint
        :param dst_ep: destination endpoint
        :param qos:  Quality of service to use (0 or 1) (default is 0)
        :param payload: payload to send
        :type payload: bytearray
        :param csma_ca_only: Is packet only for csma-ca nodes
        :type csma_ca_only: bool
        :param cb: If set, callback to be asynchronously called when gateway answer.
            If None, call is blocking.

            .. note:: Expected signature:

                on_message_sent_cb(gw_error_code, param)

                with type:

                - gw_error_code: :obj:`~wirepas_mesh_messaging.gateway_result_code.GatewayResultCode`
                - param: param given when doing this call

        :type cb: function
        :param param: Optional parameter that will be passed to callback
        :type param: object
        :return: None if cb is set or error code from gateway is synchronous call
        :rtype: :obj:`~wirepas_mesh_messaging.gateway_result_code.GatewayResultCode`

        :raises TimeoutError: Raised if cb is None and response is not received within 2 sec
        """
        request = wmm.SendDataRequest(dest, src_ep, dst_ep, qos, payload, is_unack_csma_ca=csma_ca_only)

        self._publish(TopicGenerator.make_send_data_request_topic(gw_id, sink_id),
                      request.payload,
                      1)

        return self._wait_for_response(cb, request.req_id, param=param)

    def upload_scratchpad(self, gw_id, sink_id, seq, scratchpad=None, cb=None, param=None):
        """
        upload_scratchpad(self, gw_id, sink_id, seq, scratchpad=None, cb=None, param=None)
        Upload a scratchpad on a given sink

        :param gw_id: Id of gateway the sink is attached
        :type gw_id: str
        :param sink_id: Id of sink
        :type sink_id: str
        :param seq: Sequence to use for this scratchpad
        :type seq: int
        :param scratchpad: Scratchpad to upload, None to clear the current stored one
        :type scratchpad: str
        :param cb: If set, callback to be asynchronously called when gateway answers

            **Expected signature**:

            .. code-block:: python

                 on_scratchpad_uploaded_cb(gw_error_code, param)

            - gw_error_code: :obj:`~wirepas_mesh_messaging.gateway_result_code.GatewayResultCode`
            - param: param given when doing this call

            .. warning:: If unset, call is synchronous and caller can be blocked for up to 60 seconds

        :type cb: function
        :param param: Optional parameter that will be passed to callback
        :type param: object
        :return: None if cb is set or error code from gateway is synchronous call
        :rtype: :obj:`~wirepas_mesh_messaging.gateway_result_code.GatewayResultCode`

        :raises TimeoutError: Raised if cb is None and response is not received within 60 sec
        """
        request = wmm.UploadScratchpadRequest(seq, sink_id, scratchpad=scratchpad)

        self._publish(TopicGenerator.make_otap_load_scratchpad_request_topic(gw_id, sink_id),
                      request.payload,
                      1)
        return self._wait_for_response(cb, request.req_id, timeout=60, param=param)

    @_wait_for_configs
    def process_scratchpad(self, gw_id, sink_id, cb=None, param=None):
        """
        process_scratchpad(self, gw_id, sink_id, cb=None, param=None)
        Process scratchpad on a given sink

        :param gw_id: Id of gateway the sink is attached
        :type gw_id: str
        :param sink_id: Id of sink
        :type sink_id: str
        :param cb: If set, callback to be asynchronously called when gateway answers

            **Expected signature**:

            .. code-block:: python

                on_scratchpad_processed_cb(gw_error_code, param)

            - gw_error_code: :obj:`~wirepas_mesh_messaging.gateway_result_code.GatewayResultCode`
            - param: param given when doing this call

            .. warning:: If unset, call is synchronous and caller can be blocked for up to 60 seconds

        :type cb: function
        :param param: Optional parameter that will be passed to callback
        :type param: object
        :return: None if cb is set or error code from gateway is synchronous call
        :rtype: :obj:`~wirepas_mesh_messaging.gateway_result_code.GatewayResultCode`

        :raises TimeoutError: Raised if cb is None and response is not received within 60 sec
        """
        request = wmm.ProcessScratchpadRequest(sink_id)

        self._publish(TopicGenerator.make_otap_process_scratchpad_request_topic(gw_id, sink_id),
                      request.payload,
                      1)
        return self._wait_for_response(cb, request.req_id, timeout=60, param=param)

    @_wait_for_configs
    def get_scratchpad_status(self, gw_id, sink_id, cb=None, param=None):
        """
        get_scratchpad_status(self, gw_id, sink_id, cb=None, param=None)
        Get scratchpad status of a given sink

        :param gw_id: Id of gateway the sink is attached
        :type gw_id: str
        :param sink_id: Id of sink
        :type sink_id: str
        :param cb: If set, callback to be asynchronously called when gateway answers

            **Expected signature**:

            .. code-block:: python

                on_status_received_cb(gw_error_code, param, status)

            - gw_error_code: :obj:`~wirepas_mesh_messaging.gateway_result_code.GatewayResultCode`
            - param: param given when doing this call
            - status: see note

            .. warning:: If unset, call is synchronous and caller can be blocked for up to 2 seconds

        :type cb: function
        :param param: Optional parameter that will be passed to callback
        :type param: object
        :return: Tuple (:obj:`~wirepas_mesh_messaging.gateway_result_code.GatewayResultCode`, status) None if cb is set. See note.
        :rtype: Tuple

        :raises TimeoutError: Raised if cb is None and response is not received within 2 sec

        .. note:: Status is a dictionary with following keys:

                ============================= =============
                Key                           Value
                ----------------------------- -------------
                stored_scratchpad             (dic) keys: len, crc, seq
                stored_status                 (int)
                stored_type                   (int)
                processed_scratchpad          (dic) keys: len, crc, seq
                target_scratchpad_and_action  (dic) keys: action, target_sequence, target_crc, param
                ============================= =============

        """
        request = wmm.GetScratchpadStatusRequest(sink_id)

        self._publish(TopicGenerator.make_otap_status_request_topic(gw_id, sink_id),
                      request.payload,
                      1)
        return self._wait_for_response(cb, request.req_id, param=param)

    @_wait_for_configs
    def set_target_scratchpad(self, gw_id, sink_id, action, cb=None, param=None):
        """
        set_target_scratchpad(self, gw_id, sink_id, action, cb=None, param=None)
        Set target scratchpad on a given sink

        :param gw_id: Id of gateway the sink is attached
        :type gw_id: str
        :param sink_id: Id of sink
        :type sink_id: str
        :param action: Action to set
        :type action: :class:`wirepas_mesh_messaging.otap_helper.ScratchpadAction`
        :param cb: If set, callback to be asynchronously called when gateway answers

            **Expected signature**:

            .. code-block:: python

                 on_target_set_cb(gw_error_code, param)

            - gw_error_code: :obj:`~wirepas_mesh_messaging.gateway_result_code.GatewayResultCode`
            - param: param given when doing this call

            .. warning:: If unset, call is synchronous and caller can be blocked for up to 2 seconds

        :type cb: function
        :param param: Optional parameter that will be passed to callback
        :type param: object
        :return: None if cb is set or error code from gateway is synchronous call
        :rtype: :obj:`~wirepas_mesh_messaging.gateway_result_code.GatewayResultCode`

        :raises TimeoutError: Raised if cb is None and response is not received within 2 sec
        """
        request = wmm.SetScratchpadTargetAndActionRequest(sink_id,
                                                          action)

        self._publish(TopicGenerator.make_otap_set_target_scratchpad_request_topic(gw_id, sink_id),
                      request.payload,
                      1)
        return self._wait_for_response(cb, request.req_id, param=param)

    def register_data_cb(self, cb, gateway=None, sink=None, network=None, src_ep=None, dst_ep=None):
        """
        Register a data filter to received filtered data

        :param cb: Callback to be called when a matching packet is received
        :param gateway: Filter on a given gateway (None for all)
        :param sink: Filter on a given sink (None for all)
        :param network: Filter on a given network (None for all)
        :param src_ep: Filter on a given source endpoint (None for all)
        :param dst_ep: Filter on a given destination endpoint (None for all)
        :return: The id of this filter, to be used when removing it with
            :meth:`~wirepas_mqtt_library.wirepas_network_interface.WirepasNetworkInterface.unregister_data_cb`
        """
        new_filter = _DataFilter(cb, gateway, sink, network, src_ep, dst_ep)
        self._data_filters[id(new_filter)] = new_filter

        return id(new_filter)

    def unregister_data_cb(self, id):
        """Unregister data callback previously registered
        with :meth:`~wirepas_mqtt_library.wirepas_network_interface.WirepasNetworkInterface.register_data_cb`

        :param id: id returned when adding the filter
        :raises KeyError: if id doesn't exist
        """
        del self._data_filters[id]

    @_wait_for_configs
    def set_sink_config(self, gw_id, sink_id, new_config, cb=None, param=None):
        """
        set_sink_config(self, gw_id, sink_id, new_config, cb=None, param=None)
        Set the config of a sink

        Configuration is sent as dictionary. Call can be clocking or unblocking
        depending on ``cb`` parameter.

        :param gw_id: gateway id the sink is attached to
        :param sink_id: id of the sink to configure
        :param new_config: the new config.
            Keys are:

            ===================== =============
            Key                   Value
            --------------------- -------------
            node_role             (int)
            node_address          (int)
            network_address       (int)
            network_channel       (int)
            app_config_diag       (int)
            app_config_seq        (int)
            app_config_data       (bytearray)
            cipher_key            (bytearray)
            authentication_key    (bytearray)
            started               (bool)
            ===================== =============

        :type new_config: dict
        :param cb: If set, callback to be asynchronously called when gateway answers

            **Expected signature**:

            .. code-block:: python

                on_scratchpad_processed_cb(gw_error_code, param)

            - gw_error_code: :obj:`~wirepas_mesh_messaging.gateway_result_code.GatewayResultCode`
            - param: param given when doing this call

            .. warning:: If unset, call is synchronous and caller can be blocked for up to 5 seconds

        :type cb: function
        :param param: Optional parameter that will be passed to callback
        :type param: object
        :return: None if cb is set or error code from gateway is synchronous call
        :rtype: :obj:`~wirepas_mesh_messaging.gateway_result_code.GatewayResultCode`

        :raises TimeoutError: Raised if cb is None and response is not received within 5 sec

        .. note:: node_role encoding is same as in dualmcu protocol

        """
        request = wmm.SetConfigRequest(sink_id,
                                       new_config)

        self._publish(TopicGenerator.make_set_config_request_topic(gw_id, sink_id),
                      request.payload,
                      1)
        # Extend default delay as it may require a reboot of sink
        return self._wait_for_response(cb, request.req_id, timeout=5, param=param)

    @_wait_for_configs
    def set_config_changed_cb(self, cb):
        """
        set_config_changed_cb(self, cb)
        Set a callback to be called when a config has changed on any of
        the attached gateway

        :param cb: Callback to be called when something has changed.
            :meth:`~wirepas_mqtt_library.wirepas_network_interface.WirepasNetworkInterface.get_gateways`
            and :meth:`~wirepas_mqtt_library.wirepas_network_interface.WirepasNetworkInterface.get_sinks`
            can be used to discover what has changed
        """
        self._on_config_changed_cb = cb

        # Call the cb a first time as an initial info
        self._on_config_changed_cb()

    def __str__(self):
        return str(self._gateways)


class _DataFilter:
    def __init__(self, cb, gateway=None, sink=None, network=None, src_ep=None, dst_ep=None):
        # Convert parameter to list if required
        if gateway is not None and isinstance(gateway, str):
            self.gateway = [gateway]
        else:
            self.gateway = gateway

        if sink is not None and isinstance(sink, str):
            self.sink = [sink]
        else:
            self.sink = sink

        if network is not None and isinstance(network, int):
            self.network = [network]
        else:
            self.network = network

        if src_ep is not None and isinstance(src_ep, int):
            self.src_ep = [src_ep]
        else:
            self.src_ep = src_ep

        if dst_ep is not None and isinstance(dst_ep, int):
            self.dst_ep = [dst_ep]
        else:
            self.dst_ep = dst_ep

        if cb is None:
            raise ValueError("Callback must be specified")
        self.callback = cb

    def filter_and_dispatch(self, message):
        if self.gateway is not None and message.gw_id not in self.gateway:
            return

        if self.sink is not None and message.sink_id not in self.sink:
            return

        if self.network is not None and message.network_address not in self.network:
            return

        if self.src_ep is not None and message.source_endpoint not in self.src_ep:
            return

        if self.dst_ep is not None and message.destination_endpoint not in self.dst_ep:
            return

        # Message is not filtered and can be dispatched
        self.callback(message)


class _TaskQueue(Queue):
    def __init__(self, num_worker=1):
        Queue.__init__(self)
        for i in range(num_worker):
            thread = Thread(target=self.worker)
            thread.daemon = True
            thread.start()

    def add_task(self, task, *args, **kwargs):
        args = args or ()
        kwargs = kwargs or {}
        self.put((task, args, kwargs))

    def worker(self):
        while True:
            task, args, kwargs = self.get()
            task(*args, **kwargs)
