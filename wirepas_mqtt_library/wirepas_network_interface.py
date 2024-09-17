# Copyright 2021 Wirepas Ltd. All Rights Reserved.
#
# See file LICENSE.txt for full license details.
#
import ssl
import logging
import socket

import paho.mqtt.client as mqtt
from paho.mqtt.client import connack_string
import wirepas_mesh_messaging as wmm
from .topic_helper import TopicGenerator, TopicParser
from threading import Event, Thread, Lock
from time import sleep, time
from queue import Queue


class WirepasNetworkInterface:
    """Class to interact with a Wirepas network through an MQTT broker.

    This class wraps the MQTT connection and the messages creation / parsing in protobuf
    format.

    Most of the calls can be called in a blocking mode (ie, blocking until gateway response is received)
    or in an asynchronous way by specifying a callback to receive the response.

    """
    # Timeout in s to wait for connection
    _TIMEOUT_NETWORK_CONNECTION_S = 4

    # Timeout in s to wait for status message after subscription to
    # topic and connection is established
    _TIMEOUT_GW_STATUS_S = 2

    # Inner class definition to define a gateway
    class _Gateway:
        def __init__(self, id, online=False, sinks=None, model=None, version=None, max_scratchpad_size=None):
            self.id = id
            self.online = online
            self.sinks = sinks
            self.model = model
            self.version = version
            self.max_scratchpad_size = max_scratchpad_size
            self.config_received_event = Event()
            if sinks is not None:
                self.config_received_event.set()

            self._config_lock = Lock()

        def update_all_sink_configs(self, configs):
            with self._config_lock:
                updated = False

                if self.sinks is None or configs is None:
                    self.sinks = configs
                    updated = True
                else:
                    if sorted(configs, key = lambda ele: sorted(ele.items())) != sorted(
                            self.sinks, key = lambda ele: sorted(ele.items())):
                        self.sinks = configs
                        updated = True

                # Initial config has been received
                self.config_received_event.set()

                return updated


        def update_sink_config(self, config):
            with self._config_lock:
                updated = False
                if self.config_received_event.is_set():
                    # We have receive full config so we can update
                    if self.sinks is None:
                        self.sinks = [config]
                        updated = True
                    else:
                        for idx, sink in enumerate(self.sinks):
                            if sink["sink_id"] == config["sink_id"]:
                                # Update the config
                                if self.sinks[idx] != config:
                                    self.sinks[idx] = config
                                    updated = True

                return updated

        def __repr__(self):
            return self.__str__()

        def __str__(self):
            if not self.online:
                return "GW {} => OFFLINE".format(self.id)
            elif self.config_received_event.is_set():
                return "GW {} => ONLINE with sinks: {}".format(self.id, self.sinks)
            else:
                return "GW {} => ONLINE (config unknown yet)".format(self.id)

    class ConnectionErrorCode:
        UNKNOWN_ERROR = 255
        OK = 0
        UNKNOWN_HOST = 1
        SOCKET_TIMEOUT = 2
        CONNECTION_REFUSED = 3
        BROKER_REFUSED_PROTOCOL_VERSION = 4
        BROKER_REFUSED_IDENTIFIER_REJECTED = 5
        BROKER_REFUSED_SERVER_UNAVAILABLE = 6
        BROKER_REFUSED_BAD_USERNAME_PASSWORD = 7
        BROKER_REFUSED_NOT_AUTHORIZED = 8

        @classmethod
        def from_broker_connack(cls, rc):
            error_code = cls.UNKNOWN_ERROR
            if rc == 0:
                error_code = cls.OK
            elif rc == 1:
                error_code = cls.BROKER_REFUSED_PROTOCOL_VERSION
            elif rc == 2:
                error_code = cls.BROKER_REFUSED_IDENTIFIER_REJECTED
            elif rc == 3:
                error_code = cls.BROKER_REFUSED_SERVER_UNAVAILABLE
            elif rc == 4:
                error_code = cls.BROKER_REFUSED_BAD_USERNAME_PASSWORD
            elif rc == 5:
                error_code = cls.BROKER_REFUSED_NOT_AUTHORIZED
            return error_code

    def __init__(self, host, port, username, password,
                 insecure=False, num_worker_thread=1, strict_mode=False,
                 connection_cb=None, client_id="", clean_session=None,
                 transport="tcp", gw_timeout_s=2):
        """Constructor

        :param host: MQTT broker host address
        :param port: MQTT broker port
        :param username: MQTT username
        :param password: MQTT password
        :param insecure: if set to true, TLS handshake with broker is skipped
        :param num_worker_thread: number of thread to use to handle asynchronous event (like data reception)
        :param strict_mode: if strict_mode is set to false, some errors do not generate exceptions but only an error message
        :param connection_cb: If set, callback to be called when connection to mqtt broker changes

            **Expected signature**:

            .. code-block:: python

                on_mqtt_connection_change_cb(connected, error_code)

            - connected: if True, connection is established false otherwise
            - error_code: :class:`~wirepas_mqtt_library.wirepas_network_interface.ConnectionErrorCode`

        :type connection_cb: function
        :param client_id: the unique client id string used when connecting to the broker. If client_id is zero length or None,
               then one will be randomly generated. In this case the clean_session parameter must be True.
        :param clean_session: a boolean that determines the client type. If True, the broker will remove all information about this client when it disconnects.
               If False, the client is a durable client and subscription information and queued messages will be retained when the client disconnects.
        :param transport: set to "websockets" to send MQTT over WebSockets. Leave at the default of "tcp" to use raw TCP.
        :param gw_timeout_s: Timeout in s to receive a response from a gw
        """
        # Create an MQTT client (can generate Exception if clean session is False and no client id set,
        # but no need to catch it here)
        self._mqtt_client = mqtt.Client(client_id=client_id, clean_session=clean_session, transport=transport)

        if not insecure:
            self._mqtt_client.tls_set(ca_certs=None, certfile=None, keyfile=None,
                                      cert_reqs=ssl.CERT_REQUIRED,
                                      tls_version=ssl.PROTOCOL_TLSv1_2, ciphers=None)

        self._mqtt_client.username_pw_set(username,
                                          password)

        self._mqtt_client.on_connect = self._on_connect_callback
        self._mqtt_client.on_disconnect = self._on_disconnect_callback

        # Variable to store connection time
        self._connected = Event()
        self._connection_time = None

        # Flag to determine if we waited at least one time
        # for the full config
        self._initial_discovery_done = False

        self._gateways = {}
        # Dictionary to store request id, associated cb
        self._ongoing_requests = {}


        self._data_uplink_filters_lock = Lock()
        self._data_uplink_filters = {}
        self._data_downlink_filters_lock = Lock()
        self._data_downlink_filters = {}
        self._on_config_changed_cb = None

        # Create rx queue and start dispatch thread
        self._task_queue = _TaskQueue(num_worker_thread)

        # Save user option parameters
        self._strict_mode = strict_mode
        self._connection_cb = connection_cb
        self._gw_timeout_s = gw_timeout_s

        # Make the connection in a dedicated thread to use the
        # synchronous call and be able to catch network connections exceptions
        self._task_queue.add_task(self._execute_connection, host, int(port))

        # Start the mqtt own thread
        self._mqtt_client.loop_start()

    def _execute_connection(self, host, port):
        error = None
        try:
            # Reduce a bit keepalive for faster connection break detection
            self._mqtt_client.connect(host, port, keepalive=20)
        except socket.gaierror:
            error = WirepasNetworkInterface.ConnectionErrorCode.UNKNOWN_HOST
        except socket.timeout:
            error = WirepasNetworkInterface.ConnectionErrorCode.SOCKET_TIMEOUT
        except ConnectionRefusedError:
            error = WirepasNetworkInterface.ConnectionErrorCode.CONNECTION_REFUSED
        except Exception as e:
            error = WirepasNetworkInterface.ConnectionErrorCode.UNKNOWN_ERROR
            logging.error("Unknown exception in client connection %s", e)

        if self._connection_cb is not None and error is not None:
            self._connection_cb(False, error)

    def _on_connect_callback(self, client, userdata, flags, rc):
        if rc != 0:
            logging.error("MQTT cannot connect: %s (%s)", connack_string(rc), rc)
            if self._connection_cb is not None:
                self._task_queue.add_task(self._connection_cb, False, self.ConnectionErrorCode.from_broker_connack(rc))
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
        # TODO must be part of TopicGenerator
        all_responses_topic = "gw-response/#"
        self._mqtt_client.subscribe(all_responses_topic, 1)
        self._mqtt_client.message_callback_add(all_responses_topic,
                                               self._on_response_received)


        # Register for all sent data
        all_send_data_topic = TopicGenerator.make_send_data_request_topic()
        self._mqtt_client.subscribe(all_send_data_topic, 1)
        self._mqtt_client.message_callback_add(all_send_data_topic,
                                               self._on_downlink_data_received)

        self._connection_time = time()

        logging.info("..Connected to MQTT")
        self._connected.set()
        if self._connection_cb is not None:
            self._task_queue.add_task(self._connection_cb, True, self.ConnectionErrorCode.OK)

    def _on_disconnect_callback(self, client, userdata, rc):
        logging.info("..MQTT disconnected rc={}".format(rc))
        self._connected.clear()
        if self._connection_cb is not None:
            self._task_queue.add_task(self._connection_cb, False, self.ConnectionErrorCode.from_broker_connack(rc))

    def _on_status_gateway_received(self, client, userdata, message):
        if message.payload.__len__() == 0:
            # Remove gateway if in our list
            try:
                del self._gateways[TopicParser.parse_status_topic(message.topic)]
                # Config has changed, notify any subscriber
                if self._on_config_changed_cb is not None:
                    self._task_queue.add_task(self._on_config_changed_cb)
            except KeyError:
                pass

            return

        try:
            status = wmm.StatusEvent.from_payload(message.payload)

            # Get gateway if it already exist
            try:
                gw = self._gateways[status.gw_id]
                gw.online = (status.state == wmm.GatewayState.ONLINE)
                gw.model = status.gateway_model
                gw.version = status.gateway_version
                gw.max_scratchpad_size=status.max_scratchpad_size

                if gw.update_all_sink_configs(status.sink_configs):
                    # Config has changed, notify any subscriber
                    if self._on_config_changed_cb is not None:
                        self._task_queue.add_task(self._on_config_changed_cb)

            except KeyError:
                # Create Gateway
                self._gateways[status.gw_id] = self._Gateway(status.gw_id,
                                                             status.state == wmm.GatewayState.ONLINE,
                                                             status.sink_configs,
                                                             model=status.gateway_model,
                                                             version=status.gateway_version,
                                                             max_scratchpad_size=status.max_scratchpad_size)



        except wmm.GatewayAPIParsingException as e:
            logging.error(str(e))
            gw = TopicParser.parse_status_topic(message.topic)
            logging.error("It probably means that one of the gateway sent a malformed status."
            " It can be cleared by calling clear_gateway_status(\"%s\")", gw)
            return

    def _update_sink_config(self, gw_id, config):
        try:
            gw = self._gateways[gw_id]
            if gw.update_sink_config(config):
                # Config has changed, notify any subscriber
                if self._on_config_changed_cb is not None:
                    self._task_queue.add_task(self._on_config_changed_cb)

        except KeyError:
            logging.error("Receiving sink config that is unknown %s/%s", gw_id, config["sink_id"])

    def _update_gateway_configs(self, config):
        try:
            gw = self._gateways[config.gw_id]
            if gw.update_all_sink_configs(config.configs):
                # Config has changed, notify any subscriber
                if self._on_config_changed_cb is not None:
                    self._task_queue.add_task(self._on_config_changed_cb)
        except KeyError:
            logging.error("Receiving gateway config that is unknown %s", config.gw_id)

    def _on_data_received(self, client, userdata, message):
        try:
            data = wmm.ReceivedDataEvent.from_payload(message.payload)
            if data.network_address is None:
                # Network id is from topic
                try:
                    _, _, network_address, _, _ = TopicParser.parse_received_data_topic(message.topic)
                    data.network_address = network_address
                except ValueError:
                    logging.error("Cannot determine network address from topic: %s",message.topic)
                    # Address is unknown but still dispatch data
                    data.network_address = None

            self._task_queue.add_task(self._dispatch_uplink_data, data)
        except wmm.GatewayAPIParsingException as e:
            logging.error(str(e))

    def _dispatch_uplink_data(self, data):
        with self._data_uplink_filters_lock:
            filters_copy = list(self._data_uplink_filters.values())

        for f in filters_copy:
            f.filter_and_dispatch(data)

    def _publish(self, topic, payload, qos=1, retain=False):
        try:
            self._mqtt_client.publish(topic,
                                      payload,
                                      qos=qos,
                                      retain=retain)
        except Exception as e:
            logging.error(str(e))

    def _call_cb(self, response, *args):
        try:
            for cb, param in self._ongoing_requests[response.req_id]:
            # Add caller param after response error code and add any additional param at the end
                cb(response.res, param, *args)
                # Cb called, remove key

            del self._ongoing_requests[response.req_id]
        except KeyError:
            # No cb set, just pass (could be timeout that expired)
            logging.debug("Response but no associated cb: %s" % response.req_id)
            pass

    def _on_downlink_data_received(self, client, userdata, message):
        try:
            data = wmm.SendDataRequest.from_payload(message.payload)

            # retrieve missing info from message by reading topic
            gw_id, sink_id = TopicParser.parse_send_data_topic(message.topic)
            data.__dict__.update({'gw_id': gw_id})
            data.__dict__.update({'sink_id': sink_id})

            logging.debug("Received message with id: %s", data.req_id)
            self._wait_for_response(self._dispatch_downlink_data, data.req_id, param=data)

        except wmm.GatewayAPIParsingException as e:
            logging.error(str(e))

    def _dispatch_downlink_data(self, response, data):
        with self._data_downlink_filters_lock:
            filters_copy = list(self._data_downlink_filters.values())

        for f in filters_copy:
            f.filter_and_dispatch(data, response)

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
                # Set config answer contains the config set, update our cache value
                self._update_sink_config(response.gw_id, response.config)
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

    def _wait_for_connection(fn):
        # Decorator to handle waiting for connection to mqtt
        def wrapper(*args, **kwargs):
            args[0]._connected.wait(args[0]._TIMEOUT_NETWORK_CONNECTION_S)
            if not args[0]._connected.is_set():
                raise TimeoutError("Cannot connect to broker")

            # Check if enough time was elapsed since we connect to receive all status
            delay = (args[0]._connection_time + args[0]._TIMEOUT_GW_STATUS_S) - time()
            if delay > 0:
                sleep(delay)

            return fn(*args, **kwargs)
        wrapper.__doc__ = fn.__doc__
        return wrapper

    def _ask_gateway_config(self, gw_id):
        request = wmm.GetConfigsRequest()
        self._publish(TopicGenerator.make_get_configs_request_topic(gw_id),
                        request.payload,
                        1)

        # Call update gateway config when receiving it
        self._wait_for_response(self._update_gateway_configs, request.req_id)


    def _wait_for_configs(self, gateways=None):
        gateways_to_wait_config = []
        for gw in self._gateways.copy().values():
            if gateways is not None and gw.id not in gateways:
                # Not interested by this gateway, so no need for the config
                continue

            if gw.online:
                if gw.config_received_event.is_set():
                    # We have already received the config
                    continue

                # Time to ask the gateway config
                self._ask_gateway_config(gw.id)
                gateways_to_wait_config.append(gw)

        timeout_ts = time() + self._gw_timeout_s

        # We have asked config for gateway we never received it
        # Check if we received it for all gateways we asked before timeout
        for gw in gateways_to_wait_config:
            timeout = timeout_ts - time()
            if timeout > 0:
                # We can still wait
                gw.config_received_event.wait(timeout)

            if not gw.config_received_event.is_set():
                logging.error("Config timeout for gw %s" % gw.id)
                logging.error("Is the gateway really online? If not, its status can be cleared by "
                                "calling clear_gateway_status(\"%s\")", gw.id)
                # Mark the initial config as empty list to avoid waiting for it next time
                # It may still come later
                gw.update_all_sink_configs([])

                if self._strict_mode:
                    logging.error("This Timeout will generate an exception but you can "
                                    "avoid it by starting WirepasNetworkInteface with strict_mode=False")
                    raise TimeoutError("Cannot get config from online GW %s", gw.id)

    def close(self):
        """Explicitly close this network interface as well as the worker threads

        This closes disconnects the MQTT client from the broker which automatically closes the network thread.
        In addition, it creates empty tasks that force the worker daemon threads to exit cleanly.
        Given the connection to broker is closed first, callbacks are not executed anymore and no additional tasks
        are therefore added in the queue. Thus by design, no valid task can be added to the queue whilst the worker threads
        are being closed.

        If you do not require to close the connection in a deterministic way by invoking this method, the resources on the
        host and broker are anyway cleaned up by way of garbage collection once the main program utilising this class is
        stopped and the corresponding python process is exited.

        .. warning:: the instance of WNI must not be used anymore after this call. Any other method call requiring the broker
            connection after this one will end up in TimeoutError exception
        """
        self._mqtt_client.disconnect()
        self._task_queue.terminate()


    @_wait_for_connection
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
        # Wait for subset of gateways
        self._wait_for_configs(gateways=gateway)
        sinks = list()
        for gw in self._gateways.copy().values():
            if not gw.online:
                continue

            if gateway is not None and gw.id not in gateway:
                continue

            if gw.sinks is None:
                # No sinks, nothing to iterate
                continue

            for sink in gw.sinks:
                if network_address is not None:
                    # Sinks are filtered on network address
                    try:
                        if sink['network_address'] != network_address:
                            continue
                    except KeyError:
                        # Network address is unset so doesn't match
                        continue

                sinks.append((gw.id, sink['sink_id'], sink))

        return sinks

    @_wait_for_connection
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

    @_wait_for_connection
    def clear_gateway_status(self, gw_id):
        """
        Clear a gateway status

        :param gw_id: Id of gateway the sink is attached
        :type gw_id: str

        .. note:: Gateway status is sent by gateway as a retain message on the broker.
            It may be need to explicitly remove it in some situation:
            - An offline gateway that will never be back online (removed from network)
            - A sticky gateway online status that is not here anymore (bug from gateway)
            - A malformed gateway status (bug from gateway)
        """
        topic = TopicGenerator.make_status_topic(gw_id)
        self._publish(topic, None, qos=1, retain=True)

    def _add_to_ongoing_request(self, req_id, cb, param=None):
        try:
            self._ongoing_requests[req_id].append((cb, param))
        except KeyError:
            self._ongoing_requests[req_id]= [(cb, param)]

    def _wait_for_response(self, cb, req_id, extra_timeout=0, param=None):
        if cb is not None:
            # Unblocking call, cb will be called later
            self._add_to_ongoing_request(req_id, cb, param)
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

        self._add_to_ongoing_request(req_id, unlock)

        if not response_event.wait(self._gw_timeout_s + extra_timeout):
            # Timeout
            del self._ongoing_requests[req_id]
            raise TimeoutError("Cannot get response for request")

        return res

    @_wait_for_connection
    def send_message(self, gw_id, sink_id, dest, src_ep, dst_ep, payload, qos=0, csma_ca_only=False, hop_limit=0, cb=None, param=None):
        """
        send_message(self, gw_id, sink_id, dest, src_ep, dst_ep, payload, qos=0, csma_ca_only=False, cb=None, param=None)
        Send a message to wirepas network from a given sink

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
        :param hop_limit(int): maximum number of hops this message can do to reach its destination (<16)
        :type hop_limit: int
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
        request = wmm.SendDataRequest(dest, src_ep, dst_ep, qos, payload, is_unack_csma_ca=csma_ca_only, hop_limit=hop_limit)

        self._publish(TopicGenerator.make_send_data_request_topic(gw_id, sink_id),
                      request.payload,
                      1)

        return self._wait_for_response(cb, request.req_id, param=param)

    def _upload_scratchpad_as_chunks(self, topic, sink_id, seq, scratchpad, max_chunk_size, cb, param=None, timeout=60):
        end_event = Event()
        final_res = None

        # Definition of the intermediate callback
        # to publish next block if previous one was successfully sent
        def next_chunk(response, param):
            total_size = scratchpad.__len__()
            # Parse the param we set in last call
            full_scratchpad, sent_bytes, user_cb, user_param = param

            # Is it end of transfer? (last chunk or error)
            if (response is not None and response != wmm.GatewayResultCode.GW_RES_OK) \
                or sent_bytes == total_size:
                # Time to call initial callback of unlock our initial requester
                if user_cb is not None:
                    user_cb(response, user_param)
                else:
                    nonlocal final_res
                    final_res = response
                    end_event.set()
                return

            chunk_size = min(total_size - sent_bytes, max_chunk_size)
            request = wmm.UploadScratchpadRequest(seq,
                                                    sink_id,
                                                    scratchpad=scratchpad[sent_bytes:sent_bytes+chunk_size],
                                                    chunk_info={"total_size": total_size,
                                                                "offset": sent_bytes})

            logging.info("Sending chunk from %d -> %d (%d)", sent_bytes, sent_bytes + chunk_size, total_size)

            self._publish(topic, request.payload, 1)
            sent_bytes += chunk_size
            self._wait_for_response(next_chunk,
                                    request.req_id,
                                    extra_timeout=timeout,
                                    param=(full_scratchpad, sent_bytes, user_cb, user_param))

        # Initiate the transfer by calling the next_chunk cb a fisrt time
        next_chunk(None, (scratchpad, 0, cb, param))

        if cb is None:
            # There is no user callback so lock caller until end of transfer
            end_event.wait(timeout)

        return final_res

    @_wait_for_connection
    def upload_scratchpad(self, gw_id, sink_id, seq, scratchpad=None, cb=None, param=None, timeout=60):
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

            .. warning:: If unset, call is synchronous and caller can be blocked for up to specified timeout

        :type cb: function
        :param param: Optional parameter that will be passed to callback
        :type param: object
        :param timeout: Timeout in second to wait for the upload (default 60s)
        :type timeout: int
        :return: None if cb is set or error code from gateway is synchronous call
        :rtype: :obj:`~wirepas_mesh_messaging.gateway_result_code.GatewayResultCode`

        :raises TimeoutError: Raised if cb is None and response is not received within the specified timeout
        """
        try:
            # Check what is the max transfert size supported by the gateway
            max_size = self._gateways[gw_id].max_scratchpad_size
            if max_size is not None:
                logging.info("Max scratchpad size is %d for %s" % (scratchpad.__len__(), gw_id))
        except KeyError:
            logging.error("Unknown gateway in upload_scratchpad %s", gw_id)
            return wmm.GatewayResultCode.GW_RES_INVALID_PARAM

        topic = TopicGenerator.make_otap_load_scratchpad_request_topic(gw_id, sink_id)

        # Check if scratchpad must be sent as chunk
        if scratchpad is not None and max_size is not None and scratchpad.__len__() > max_size:
            # Scratchpad must be devided in chunk
            logging.info("Loading scratchpad of size: %d in chunk of %d bytes" % (scratchpad.__len__(), max_size))
            return self._upload_scratchpad_as_chunks(
                            topic,
                            sink_id,
                            seq,
                            scratchpad,
                            max_size,
                            cb,
                            param,
                            timeout)
        else:
            # A single request is enough to upload (or clear) scratchpad
            request = wmm.UploadScratchpadRequest(seq, sink_id, scratchpad=scratchpad)

            self._publish(topic,
                          request.payload,
                          1)
            return self._wait_for_response(cb, request.req_id, extra_timeout=timeout, param=param)

    @_wait_for_connection
    def process_scratchpad(self, gw_id, sink_id, cb=None, param=None, timeout=120):
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

            .. warning:: If unset, call is synchronous and caller can be blocked for up to specified timeout

        :type cb: function
        :param param: Optional parameter that will be passed to callback
        :type param: object
        :param timeout: Timeout in second to wait for the processing of scratchpad (default 120s)
        :type timeout: int
        :return: None if cb is set or error code from gateway is synchronous call
        :rtype: :obj:`~wirepas_mesh_messaging.gateway_result_code.GatewayResultCode`

        :raises TimeoutError: Raised if cb is None and response is not received within specified timeout
        """
        request = wmm.ProcessScratchpadRequest(sink_id)

        self._publish(TopicGenerator.make_otap_process_scratchpad_request_topic(gw_id, sink_id),
                      request.payload,
                      1)
        return self._wait_for_response(cb, request.req_id, extra_timeout=timeout, param=param)

    @_wait_for_connection
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

    @_wait_for_connection
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
        """Deprecated
        Replaced by :meth:`~wirepas_mqtt_library.wirepas_network_interface.WirepasNetworkInterface.register_uplink_traffic_cb`
        Only name has changed
        """
        return self.register_uplink_traffic_cb(cb, gateway, sink, network, src_ep, dst_ep)

    def unregister_data_cb(self, id):
        """Deprecated
        Replaced by :meth:`~wirepas_mqtt_library.wirepas_network_interface.WirepasNetworkInterface.unregister_uplink_traffic_cb`
        Only name has changed
        """
        return self.unregister_uplink_traffic_cb(id)

    def register_uplink_traffic_cb(self, cb, gateway=None, sink=None, network=None, src_ep=None, dst_ep=None):
        """
        Register a data filter to received uplink filtered data

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
        with self._data_uplink_filters_lock:
            self._data_uplink_filters[id(new_filter)] = new_filter

        return id(new_filter)

    def unregister_uplink_traffic_cb(self, id):
        """Unregister uplink data callback previously registered
        with :meth:`~wirepas_mqtt_library.wirepas_network_interface.WirepasNetworkInterface.register_uplink_traffic_cb`

        :param id: id returned when adding the filter
        :raises KeyError: if id doesn't exist
        """
        with self._data_uplink_filters_lock:
            del self._data_uplink_filters[id]

    def register_downlink_traffic_cb(self, cb, gateway=None, sink=None, src_ep=None, dst_ep=None):
        """
        Register a data filter to received downlink filtered data

        :param cb: Callback to be called when a matching packet is received
        :param gateway: Filter on a given gateway (None for all)
        :param sink: Filter on a given sink (None for all)
        :param src_ep: Filter on a given source endpoint (None for all)
        :param dst_ep: Filter on a given destination endpoint (None for all)
        :return: The id of this filter, to be used when removing it with
            :meth:`~wirepas_mqtt_library.wirepas_network_interface.WirepasNetworkInterface.unregister_downlink_traffic_cb`
        """
        # Downlink traffic do not have network address field. It could be determined based on gateway/sink config
        new_filter = _DataFilter(cb, gateway, sink, None, src_ep, dst_ep)
        with self._data_downlink_filters_lock:
            self._data_downlink_filters[id(new_filter)] = new_filter

        return id(new_filter)

    def unregister_downlink_traffic_cb(self, id):
        """Unregister data callback previously registered
        with :meth:`~wirepas_mqtt_library.wirepas_network_interface.WirepasNetworkInterface.register_downlink_traffic_cb`

        :param id: id returned when adding the filter
        :raises KeyError: if id doesn't exist
        """
        with self._data_downlink_filters_lock:
            del self._data_downlink_filters[id]

    @_wait_for_connection
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

                on_config_set_cb(gw_error_code, param)

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
        # Add extra timeout as it may require a reboot of sink
        return self._wait_for_response(cb, request.req_id, extra_timeout=3, param=param)

    @_wait_for_connection
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
        self._wait_for_configs()
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

    def filter_and_dispatch(self, message, *args):
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
        self.callback(message, *args)


class _TaskQueue(Queue):
    def __init__(self, num_worker=1):
        self._num_worker = num_worker

        Queue.__init__(self)
        for i in range(self._num_worker):
            thread = Thread(target=self.worker)
            thread.daemon = True
            thread.start()

    def add_task(self, task, *args, **kwargs):
        args = args or ()
        kwargs = kwargs or {}
        self.put((task, args, kwargs))

    def worker(self):
        while True:
            try:
                task, args, kwargs = self.get()
                task(*args, **kwargs)
            except TypeError as e:
                # When a task is None in the queue and the task is invoked
                # a type error is raised. This condition is used to terminate the Thread
                break

    def terminate(self):
        for worker_id in range(0,self._num_worker):
            logging.debug("Adding empty task to force worker %d thread to exit" %worker_id)
            self.add_task(None)