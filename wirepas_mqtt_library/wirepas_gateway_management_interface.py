# Copyright 2025 Wirepas Ltd. All Rights Reserved.
#
# See file LICENSE.txt for full license details.
#
from enum import auto, Enum
from queue import Queue, Empty
import json
import logging
import math
import random
import ssl
from threading import Event, Lock, Timer
from time import sleep, time
from typing import List

import paho.mqtt.client as mqtt

from .wirepas_network_interface import _TaskQueue


class GwMgmtResponseErrorCodeEnum(Enum):
    SUCCESS = 0
    GW_ACK_TIMEOUT = auto()
    GW_ACK_ERROR_RESPONSE = auto()
    GW_ACK_OTHER_ERROR = auto()
    GW_CONFIG_RESP_TIMEOUT = auto()
    GW_CONFIG_OTHER_ERROR = auto()


class GwMgmtResponse:
    def __init__(self, error_code: GwMgmtResponseErrorCodeEnum, req_id: int,
                 gateway: str, request_type: str, gw_ack_response: dict, value=None):
        """
        Class to store the information of a gateway management response and
        the related configuration response.
        """
        self.error_code = error_code
        self.req_id = req_id
        self.gateway = gateway
        self.request_type = request_type
        self.gw_ack_response = gw_ack_response
        self.value = value

    def __str__(self) -> str:
        return (
            f"GwMgmtResponse(\n"
            f"  error_code   = {self.error_code},\n"
            f"  req_id       = {self.req_id},\n"
            f"  gateway      = '{self.gateway}',\n"
            f"  request_type = '{self.request_type}',\n"
            f"  gw_ack_resp  = {self.gw_ack_response},\n"
            f"  value        = {self.value}\n"
            f")"
        )

    def __repr__(self) -> str:
        return self.__str__()


class MgmtRequestInfo:
    def __init__(self, req_id, gateway, request_type, sub_cmd = None):
        """ Class to store the information of a gateway management request.

        :param req_id: Request id of the message.
        :param gateway: Gateway identifier to where the request is sent to.
        :param request_type: Request type of the message.
        :param sub_cmd: Sub-command type of the message.
        """
        self.req_id = req_id
        self.gateway = gateway
        self.request_type = request_type
        self.sub_cmd = sub_cmd

    def is_gw_ack(self, req_id, gateway, cmd, sub_cmd = None) -> bool:
        """Return whether the gateway acknowledge response correspond to this request.

        :param req_id: request id acknowledged.
        :param gateway: Gateway acknowledging a request.
        :param cmd: command type of the message acknowledged.
        :param sub_cmd: Sub-command type of the message acknowledged.
        """
        return int(self.req_id) == int(req_id) and str(self.gateway) == str(gateway) \
               and str(self.request_type) == str(cmd) and str(self.sub_cmd) == str(sub_cmd)

    def is_config_response(self, gateway, sub_cmd, request_type="set_config") -> bool:
        """Return whether the gateway configuration response correspond to this request.

        :param gateway: Gateway acknowledging a request.
        :param sub_cmd: Sub configuration type (or sub command) of the message acknowledged.
        :param request_type: Configuration request type of the message acknowledged.
        """
        return str(self.gateway) == str(gateway) and str(self.request_type) == str(request_type) \
               and str(self.sub_cmd) == str(sub_cmd)


class WirepasGatewayMgmtInterface:
    # Timeout in s to wait for connection
    _TIMEOUT_MQTT_BROKER_CONNECTION_S = 4

    # Timeout in s to wait for all retained messages after subscription to
    # topic and connection is established
    _TIMEOUT_RETAINED_MSG_S = 5

    def __init__(self, host, port, username, password, insecure=False,
                 num_worker_thread=1, client_id="", clean_session=None,
                 transport="tcp", gw_timeout_s=5):
        """Synchronous MQTT broker client interface for gateway management.

        :param host: MQTT broker host address
        :param port: MQTT broker port
        :param username: MQTT username
        :param password: MQTT password
        :param insecure: if set to true, TLS handshake with broker is skipped
        :param num_worker_thread: number of thread to use to handle asynchronous event (like data reception)
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
        self._mqtt_client.username_pw_set(username, password)

        if not insecure:
            self._mqtt_client.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLSv1_2)

        self._mqtt_client.on_connect = self._on_connect_callback
        self._mqtt_client.on_disconnect = self._on_disconnect_callback

        # Message reception variables
        self._messages_queue = Queue()
        self._current_request_info = None
        self._current_gw_ack_response = Queue()
        self._current_response_config = Queue()

        self._img_images_info = {}
        self._img_config_filters_lock = Lock()
        self._img_config_filters = {}
        self._img_event_filters_lock = Lock()
        self._img_event_filters = {}

        # Save user option parameters
        self._gw_timeout_s = gw_timeout_s
        self._is_connected = Event()

        # Create rx queue and start dispatch thread
        self._task_queue = _TaskQueue(num_worker_thread)
        self._task_queue.add_task(self._execute_connection, host, int(port))

        # Start the mqtt own thread
        self._mqtt_client.loop_start()

    def _execute_connection(self, host, port):
        try:
            self._mqtt_client.connect(host, port, keepalive=20)
        except Exception as e:
            logging.error("An exception occured in client connection %s", e)

    def _on_connect_callback(self, client, userdata, flags, rc):
        if rc != 0:
            logging.error("MQTT cannot connect: %s", rc)
            return

        # Register for all img responses
        all_img_responses_topic = "gw-mgmt-response/#"
        self._mqtt_client.subscribe(all_img_responses_topic, 1)
        self._mqtt_client.message_callback_add(all_img_responses_topic,
                                               self._on_img_response_received)

        # Register for all img events
        all_img_events_topic = "gw-mgmt-event/#"
        self._mqtt_client.subscribe(all_img_events_topic, 1)
        self._mqtt_client.message_callback_add(all_img_events_topic,
                                               self._on_img_event_received)

        # Register for all img configs
        all_img_configs_topic = "gw-mgmt-config/#"
        self._mqtt_client.subscribe(all_img_configs_topic, 1)
        self._mqtt_client.message_callback_add(all_img_configs_topic,
                                               self._on_img_config_received)

        # Register for all img image infos
        img_images_info_topic = "gw-mgmt-image-bin/+/info"
        self._mqtt_client.subscribe(img_images_info_topic, 1)
        self._mqtt_client.message_callback_add(img_images_info_topic,
                                               self._on_img_image_info_received)

        # Wait for all retained message
        Timer(self._TIMEOUT_RETAINED_MSG_S, lambda: self._is_connected.set()).start()

    def _on_disconnect_callback(self, client, userdata, rc):
        logging.info("..MQTT disconnected rc={}".format(rc))
        self._is_connected.clear()
        try:
            sleep(2)
            self._mqtt_client.reconnect()
        except Exception as e:
            logging.error("An exception occured in client reconnection %s", e)

    def _dispatch_img_config(self, data, gateway, sub_config_name):
        with self._img_config_filters_lock:
            filters_copy = list(self._img_config_filters.values())

        for f in filters_copy:
            if not f.gateway or gateway in f.gateway:
                f.callback(data, gateway, sub_config_name)

    def _dispatch_img_event(self, data, gateway, event):
        with self._img_event_filters_lock:
            filters_copy = list(self._img_event_filters.values())

        for f in filters_copy:
            f.filter_and_dispatch_img_message(data, gateway, event)

    def _publish(self, topic, payload, qos=1, retain=False):
        try:
            self._mqtt_client.publish(topic,
                                      payload,
                                      qos=qos,
                                      retain=retain)
        except Exception as e:
            logging.error(str(e))

    def _reset_response(self):
        self._current_request_info = None
        self._current_gw_ack_response = Queue()
        self._current_response_config = Queue()

    def _wait_for_gw_ack(self):
        gw_ack_response = None
        error_code = GwMgmtResponseErrorCodeEnum.GW_ACK_OTHER_ERROR
        if not self._current_request_info:
            raise ValueError("No response are expected!")

        try:
            gw_ack_response = self._current_gw_ack_response.get(timeout=self._gw_timeout_s)
        except Empty:
            pass

        if not gw_ack_response:
            error_code = GwMgmtResponseErrorCodeEnum.GW_ACK_TIMEOUT
            logging.warning("A timeout occured while waiting for a gateway response!")
        elif not gw_ack_response or not gw_ack_response.get("success", False):
            # Gateway sent an error.
            logging.warning("An unsuccessful response has been received from the gateway: %s", gw_ack_response)
            error_code = GwMgmtResponseErrorCodeEnum.GW_ACK_ERROR_RESPONSE
        else:
            logging.debug("A response has been received from the gateway: %s", gw_ack_response)
            error_code = GwMgmtResponseErrorCodeEnum.SUCCESS

        return GwMgmtResponse(error_code=error_code,
                              req_id=self._current_request_info.req_id,
                              gateway=self._current_request_info.gateway,
                              request_type=self._current_request_info.request_type,
                              gw_ack_response=gw_ack_response)

    def _wait_for_gw_config(self, mgmt_response):
        response_config = None
        try:
            response_config = self._current_response_config.get(timeout=self._gw_timeout_s)
        except Empty:
            pass

        if not response_config:
            logging.warning("A timeout occured while waiting for a gateway configuration!")
            if mgmt_response.error_code == GwMgmtResponseErrorCodeEnum.SUCCESS:  # Update error code if gw ack went well
                mgmt_response.error_code = GwMgmtResponseErrorCodeEnum.GW_CONFIG_RESP_TIMEOUT
        else:
            logging.debug("A response to a configuration has been received from the gateway: %s", response_config)
            mgmt_response.value = response_config

        return mgmt_response

    def _wait_for_response(self, wait_config=False):
        mgmt_response = self._wait_for_gw_ack()

        if wait_config:
            self._wait_for_gw_config(mgmt_response)

        self._reset_response()
        return mgmt_response

    def _on_img_response_received(self, client, userdata, message):
        if not self._current_request_info:
            return  # Not waiting for any response.

        try:
            response = json.loads(message.payload)
            split_topic = message.topic.split("/")
            cmd = split_topic[1]
            gateway = split_topic[2]
            sub_cmd = split_topic[3] if len(split_topic) >= 4 else None

            if self._current_request_info.is_gw_ack(response.get("req_id"), gateway, cmd, sub_cmd):
                self._current_gw_ack_response.put(response)
        except Exception as e:
            logging.error(str(e))

    def _on_img_config_received(self, client, userdata, message):
        # Topic are as followed: gw-mgmt-config/gw-id/sub_config/...
        _, gateway, sub_config_name, *_ = message.topic.split("/")
        payload = message.payload
        try:
            payload = json.loads(message.payload)
        except:
            pass

        # Check if the response is generated from a request.
        if self._current_request_info and self._current_request_info.is_config_response(gateway, sub_config_name):
            self._current_response_config.put(payload)

        self._task_queue.add_task(self._dispatch_img_config, payload, gateway, sub_config_name)

    def _on_img_event_received(self, client, userdata, message):
        # Topic are as followed: gw-mgmt-event/event/gw-id/...
        _, event, gateway, *_ = message.topic.split("/")
        payload = message.payload
        try:
            payload = json.loads(message.payload)
        except:
            pass

        self._task_queue.add_task(self._dispatch_img_event, payload, gateway, event)

    def _on_img_image_info_received(self, client, userdata, message):
        # Topic are as followed: gw-mgmt-image-bin/image_id/info
        try:
            image_id = message.topic.split("/")[1]
            image_info = message.payload
            if not image_info:
                del self._img_images_info[image_id]
                return

            self._img_images_info[image_id] = json.loads(image_info)
        except Exception as e:
            logging.error(str(e))

    def _wait_for_connection(fn):
        # Decorator to handle waiting for connection to mqtt
        def wrapper(self, *args, **kwargs):
            self._is_connected.wait(self._TIMEOUT_MQTT_BROKER_CONNECTION_S)
            if not self._mqtt_client.is_connected():
                raise TimeoutError("Cannot connect to MQTT broker")

            return fn(self, *args, **kwargs)
        wrapper.__doc__ = fn.__doc__
        return wrapper

    def register_img_config_cb(self, cb, in_meter_gateway: list = None):
        """
        Register a data filter to received img uplink config filtered data
        :param cb: Callback to be called when a matching packet is received
        :param in_meter_gateway: Filter on a given gateway (None for all)
        :return: The id of this filter, to be used when removing it with
            :meth:`~wirepas_mqtt_library.wirepas_gateway_management_interface.WirepasGatewayMgmtInterface.unregister_img_config_cb`
        """
        new_filter = _DataFilter(cb, gateway=in_meter_gateway)
        with self._img_config_filters_lock:
            self._img_config_filters[id(new_filter)] = new_filter

        return id(new_filter)

    def unregister_img_config_cb(self, id):
        """Unregister data callback previously registered
        with :meth:`~wirepas_mqtt_library.wirepas_gateway_management_interface.WirepasGatewayMgmtInterface.register_img_config_cb`
        :param id: id returned when adding the filter
        :raises KeyError: if id doesn't exist
        """
        with self._img_config_filters_lock:
            del self._img_config_filters[id]

    def register_img_event_cb(self, cb, in_meter_gateway: list = None):
        """
        Register a data filter to received img uplink event filtered data
        :param cb: Callback to be called when a matching packet is received
        :param in_meter_gateway: Filter on a given gateway (None for all)
        :return: The id of this filter, to be used when removing it with
            :meth:`~wirepas_mqtt_library.wirepas_gateway_management_interface.WirepasGatewayMgmtInterface.unregister_img_event_cb`
        """
        new_filter = _DataFilter(cb, gateway=in_meter_gateway)
        with self._img_event_filters_lock:
            self._img_event_filters[id(new_filter)] = new_filter

        return id(new_filter)

    def unregister_img_event_cb(self, id):
        """Unregister data callback previously registered
        with :meth:`~wirepas_mqtt_library.wirepas_gateway_management_interface.WirepasGatewayMgmtInterface.register_img_event_cb`
        :param id: id returned when adding the filter
        :raises KeyError: if id doesn't exist
        """
        with self._img_event_filters_lock:
            del self._img_event_filters[id]

    @_wait_for_connection
    def get_img_images_info(self):
        """
        get_img_images_info(self)
        Get a dictionary of current image infos in this broker, such as:
        {'image_id': {'full_size': 300000, 'chunk_size': 1000, 'chunk_number': 300, 'image_details': {'differential_image': False, 'app_version': 'vx.y.z'}}}
        :return: Dictionary of image infos
        """
        return self._img_images_info

    @_wait_for_connection
    def upload_img_image(self, image_id, image_binary, chunk_size, app_version, differential_image=False):
        """
        upload_img_image(self, image_id, image_binary, chunk_size, app_version, differential_image=False)
        Upload a gateway image by chunks
        :param image_id: Id of the image to be uploaded
        :type image_id: str
        :param image_binary: Binary of the image to be uploaded
        :type image_binary: bytearray
        :param chunk_size: Size of each chunk
        :type chunk_size: int
        :param app_version: Version of the img application once the image is uploaded
        :type app_version: str
        :param differential_image: A boolean that determines if the image is a differential image.
        :type differential_image: bool
        """
        full_size = len(image_binary)
        image_info = {
            "full_size": full_size,
            "chunk_size": chunk_size,
            "chunk_number": math.ceil(full_size/chunk_size),
            "image_details": {
                "differential_image": differential_image,
                "app_version": app_version
            }
        }

        logging.info("Upload img image in the MQTT broker with id %s", image_id)
        self.img_set_image_bin_info(image_id, image_info)
        for chunk_id, image_offset in enumerate(range(0, full_size, chunk_size)):
            chunk_binary = image_binary[image_offset:image_offset + chunk_size]
            self.img_set_image_chunk(image_id, chunk_id, chunk_binary)
            sleep(0.05)  # Avoid spamming too much the MQTT

        sleep(1)  # Make sure the image has been received by the MQTT broker.

    @_wait_for_connection
    def delete_img_image(self, image_id, chunk_number):
        """
        delete_img_image(self, image_id, chunk_number)
        Delete a gateway image from the MQTT broker
        :param image_id: Id of the image to delete
        :type image_id: str
        :param chunk_number: Number of chunks the img image to be deleted.
        :type chunk_number: int
        """
        for chunk_id in range(chunk_number):
            self.img_set_image_chunk(image_id, chunk_id, "")
            sleep(0.05)  # Avoid spamming too much the MQTT

        self.img_set_image_bin_info(image_id, "")

        # Wait for the confirmation that the image is delete.
        start = time()
        while time() - start < self._gw_timeout_s and self.get_img_images_info().get(image_id, None):
            sleep(0.1)

    @_wait_for_connection
    def img_send_config_request(self, gw_id, sub_config_name, request) -> GwMgmtResponse:
        """
        img_send_config_request(self, gw_id, sub_config_name, request)
        Send an img config request.
        :param gw_id: Id of the gateway
        :type gw_id: str
        :param sub_config_name: Topic name of the configuration
        :type sub_config_name: str
        :param request: img configuration request in dictionary format
        :type request: dict
        """
        if "req_id" not in request:  # Add a request id to the request if it is not provided
            request["req_id"] = random.getrandbits(32)

        req_id = request["req_id"]

        # Prepare response reception
        self._reset_response()
        self._current_request_info = MgmtRequestInfo(req_id, gw_id, "set_config", sub_config_name)

        # Send the request
        topic = f"gw-mgmt-request/set_config/{str(gw_id)}/{sub_config_name}"
        logging.info("Send a configuration request to %s on topic %s with the content: %s", gw_id, topic, request)
        self._publish(topic, json.dumps(request), 1)
        return self._wait_for_response(wait_config=True)

    @_wait_for_connection
    def img_update_image_request(self, gw_id, image_id, req_id=None) -> GwMgmtResponse:
        """
        img_update_image_request(self, gw_id, image_id, req_id=None)
        Send an update image request to a gateway.
        :param gw_id: Id of the gateway
        :type gw_id: str
        :param image_id: Id of the image to be updated in the gateway
        :type image_id: str
        :param req_id: Id of the request to be sent to the gateway. If not set, a random value in 32 bits will be used
        :type req_id: int
        """
        if req_id is None:  # Add a request id to the request
            req_id = random.getrandbits(32)

        # Prepare response reception
        self._reset_response()
        self._current_request_info = MgmtRequestInfo(req_id, gw_id, "update_image")

        # Send the request
        topic = f"gw-mgmt-request/update_image/{str(gw_id)}"
        request = json.dumps({"req_id" : req_id, "image_id": image_id})
        logging.info("Send an update image request to %s on topic %s with the content: %s", gw_id, topic, request)
        self._publish(topic, request, 1)
        return self._wait_for_response(wait_config=False)

    @_wait_for_connection
    def img_reset_request(self, gw_id, req_id=None) -> GwMgmtResponse:
        """
        img_reset_request(self, gw_id, req_id=None)
        Send a reset request to a gateway.
        :param gw_id: Id of the gateway
        :type gw_id: str
        :param req_id: Id of the request to be sent to the gateway. If not set, a random value in 32 bits will be used
        :type req_id: int
        """
        if req_id is None:  # Add a request id to the request
            req_id = random.getrandbits(32)

        # Prepare response reception
        self._reset_response()
        self._current_request_info = MgmtRequestInfo(req_id, gw_id, "reset")

        # Send the request
        topic = f"gw-mgmt-request/reset/{str(gw_id)}"
        request = json.dumps({"req_id" : req_id})
        logging.info("Send a reset request to %s on topic %s with the content: %s", gw_id, topic, request)
        self._publish(topic, request, 1)
        return self._wait_for_response(wait_config=False)

    @_wait_for_connection
    def img_console_request(self, gw_id, cmd, req_id=None) -> GwMgmtResponse:
        """
        img_console_request(self, gw_id, cmd, req_id=None)
        Send a console request to a gateway.
        :param gw_id: Id of the gateway
        :type gw_id: str
        :param cmd: Console command to be sent
        :type cmd: str
        :param req_id: Id of the request to be sent to the gateway. If not set, a random value in 32 bits will be used
        :type req_id: int
        """
        if req_id is None:  # Add a request id to the request
            req_id = random.getrandbits(32)

        # Prepare response reception
        self._reset_response()
        self._current_request_info = MgmtRequestInfo(req_id, gw_id, "console")

        # Send the request
        topic = f"gw-mgmt-request/console/{str(gw_id)}"
        request = json.dumps({"req_id" : req_id, "cmd": cmd})
        logging.info("Send an img console request to %s on topic %s with the content: %s", gw_id, topic, request)
        self._publish(topic, request, 1)
        return self._wait_for_response(wait_config=False)

    @_wait_for_connection
    def img_set_image_bin_info(self, image_id, image_bin_info=None):
        """
        img_set_image_bin_info(self, image_id, image_bin_info)
        Set image binary information in the MQTT broker as a retained message
        :param image_id: Id of the concerned gateway image
        :type image_id: str
        :param image_bin_info: Image binary info in dictionary format, None to clear the current stored one
        :type image_bin_info: dict
        """
        request = ""
        if image_bin_info:
            request = json.dumps(image_bin_info)

        # Send the request
        topic = f"gw-mgmt-image-bin/{image_id}/info"
        logging.info("Send image binary info on topic %s", topic)
        self._publish(topic, request, 1, retain=True)

    @_wait_for_connection
    def img_set_image_chunk(self, image_id, chunk_id, chunk_binary=""):
        """
        img_set_image_chunk(self, image_id, image_bin_info, chunk_binary)
        Set an image binary chunk in the MQTT broker as a retained message
        :param image_id: Id of the concerned gateway image
        :type image_id: str
        :param chunk_id: Id of the chunk to be pushed
        :type chunk_id: int
        :param chunk_binary: Empty string to clear the current stored one
        :type chunk_binary: bytearray
        """
        topic = f"gw-mgmt-image-bin/{image_id}/chunk/{str(chunk_id)}"
        self._publish(topic, chunk_binary, 1, retain=True)

    @_wait_for_connection
    def get_allowed_configurations(self, gw_id, req_id=None) -> List[str]:
        """
        get_allowed_configurations(self, gw_id, req_id)
        Returns a list of sub configuration strings allowed by the gateway. For example,
        ['meter_init', 'cellular', 'time', 'reset', 'mqtt']
        :param gw_id: Id of the gateway
        :type gw_id: str
        :param req_id: Id of the request to be sent to the gateway. If not set, a random value in 32 bits will be used
        :type req_id: int
        """
        request = {"req_id": req_id} if req_id else {}
        mgmt_response = self.img_send_config_request(gw_id, "main", request)
        try:
            return mgmt_response.value.get("ro", {}).get('config')
        except TypeError:
            error_message = f"An error occured when querying the configuration: {mgmt_response.error_code} " \
                f"- ack: {mgmt_response.gw_ack_response}"
        except KeyError:
            error_message = f"Invalid response has been received: {mgmt_response.value}"

        raise ValueError(error_message)


class _DataFilter:
    def __init__(self, cb, gateway=None):
        if cb is None:
            raise ValueError("Callback must be specified")

        # Convert parameter to list if required
        if gateway is not None and isinstance(gateway, str):
            self.gateway = [gateway]
        else:
            self.gateway = gateway

        self.callback = cb

    def filter_and_dispatch_img_message(self, message, gateway, *args):
        if self.gateway is not None and gateway not in self.gateway:
            return

        # Message is not filtered and can be dispatched
        self.callback(message, gateway, *args)
