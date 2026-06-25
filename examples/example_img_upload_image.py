# Copyright 2025 Wirepas Ltd. All Rights Reserved.
#
# See file LICENSE.txt for full license details.
#
import argparse
import logging

from wirepas_mqtt_library import WirepasGatewayMgmtInterface


if __name__ == "__main__":
    parser = argparse.ArgumentParser(fromfile_prefix_chars='@')
    parser.add_argument('--host',
                        help="MQTT broker address")
    parser.add_argument('--port', default=8883,
                        type=int,
                        help="MQTT broker port")
    parser.add_argument('--username', default='mqttmasteruser',
                        help="MQTT broker username")
    parser.add_argument('--password',
                        help="MQTT broker password")
    parser.add_argument('--insecure',
                        dest='insecure',
                        action='store_true',
                        help="MQTT use unsecured connection")
    parser.add_argument('--image_id',
                        type=str,
                        help="Img image identifier")
    parser.add_argument('--image_binary',
                        type=str,
                        help="Path of the image binary to be pushed to the mqtt broker for img")
    parser.add_argument('--chunk_size',
                        type=int,
                        help="Size of each chunk splitting the image in the MQTT broker")
    parser.add_argument('--app_version',
                        type=str,
                        help="Version of the img application once the image is uploaded")

    args = parser.parse_args()
    logging.basicConfig(format='%(levelname)s %(asctime)s %(message)s', level=logging.INFO)

    gw_mgmt = WirepasGatewayMgmtInterface(args.host,
                                          args.port,
                                          args.username,
                                          args.password,
                                          insecure=args.insecure)

    with open(args.image_binary, "rb") as image_binary:
        gw_mgmt.upload_img_image(args.image_id, image_binary.read(), chunk_size=args.chunk_size, app_version=args.app_version)
