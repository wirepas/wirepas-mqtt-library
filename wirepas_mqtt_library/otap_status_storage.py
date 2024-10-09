# Copyright 2024 Wirepas Ltd. All Rights Reserved.
#
# See file LICENSE.txt for full license details.
#

import logging
import sys

try:
    from peewee import *
except ModuleNotFoundError:
    logging.error("Required *peewee* module not found! Please run `python -m pip install peewee`")

# Constants
## Internal
### Database metadata
_MAX_LEN_FIELD_DB_CREATOR= 128
_DB_CREATOR = 'com.wirepas.otaphelper'
_DB_SCHEMA_VERSION = 1
_DB_METADATA_TABLE_NAME = 'otapstatusdbmetadata'

### Database otap status
_DB_DATA_TABLE_NAME = 'otapstatusdb'
_MAX_LEN_FIELD_SCRATCHPAD_ST_TYPE = 16
_MAX_LEN_FIELD_SCRATCHPAD_ST_STATUS = 20
_MAX_LEN_FIELD_WIREPAS_STACK_VERSION = 12
_MAX_LEN_FIELD_APP_VERSION = _MAX_LEN_FIELD_WIREPAS_STACK_VERSION
_MAX_LEN_FIELD_OTAP_ACTION = 33

### Database connection. Init deferred when final filename known.
_DB_CONN = SqliteDatabase(None, autoconnect=False)

## Public
# None

class OtapStatusStorage:
    """Class to handle OTAP status persistence
    """

    def __init__(self, storage_file=None):
        """Constructor.
        :param storage_file: File where nodes' OTAP status will be stored
        """
        logging.debug("Initialise storage")

        _DB_CONN.init(storage_file)

        if storage_file is not None:
            # Create databases if does not exist
            self._create_storage()

            # Check if storage is properly initialised and compatible with this code
            self._check_storage_format()

        else:
            logging.error("No storage file provided!")
            raise ValueError

    def _create_storage(self):
        # Check if database already initialised
        logging.debug("Creating storage...")

        with _DB_CONN:
            if not _DB_CONN.table_exists(_DB_METADATA_TABLE_NAME):
                logging.info(f"Metadata storage does not exist. Creating it...")
                _DB_CONN.create_tables([OtapstatusDBMetadata])
                # Populate metadata
                OtapstatusDBMetadata(creator=_DB_CREATOR, schema_version=_DB_SCHEMA_VERSION).save()
            else:
                logging.info(f"Existing metadata storage found.")

            if not _DB_CONN.table_exists(_DB_DATA_TABLE_NAME):
                logging.info(f"Main storage does not exist. Creating it...")
                _DB_CONN.create_tables([OtapStatusDB])
            else:
                logging.info(f"Existing main storage found.")

    def _check_storage_format(self):
        logging.info("Checking storage format...")
        with _DB_CONN:
            metadata = OtapstatusDBMetadata.get()

            if metadata.creator != _DB_CREATOR:
                logging.error("Wrong storage creator!")
                raise ValueError

            if metadata.schema_version != _DB_SCHEMA_VERSION:
                logging.error(f"Unsupported storage version ({metadata.schema_version}). Version {_DB_SCHEMA_VERSION} required.")
                raise ValueError
        logging.info("Format OK.")

    def write_node_status(self, node_status, network_address, node_address, travel_time_ms):
        """Write node OTAP status to persisted storage
        :param node_status: node's OTAP status to persist
        :param network_address: network address the node belong to
        Used to identify if some storage entry with same node address where overwritten between execution
        :param node_address: OTAP status originator
        :param travel_time_ms: OTAP status packet travel time in milliseconds
        """

        # Create data to write to storage
        storage_entry = {
                            OtapStatusDB.node_address: node_address,
                            OtapStatusDB.network_address: network_address,
                            OtapStatusDB.rx_timestamp_epoch_ms: node_status["ts"],
                            OtapStatusDB.tx_timestamp_epoch_ms: node_status["ts"] - travel_time_ms,
                            OtapStatusDB.scratchpad_stored_seq: node_status["seq"],
                            OtapStatusDB.scratchpad_stored_crc: node_status["crc"],
                            OtapStatusDB.scratchpad_stored_len: node_status["length"],
                            OtapStatusDB.scratchpad_stored_type: node_status["type"],
                            OtapStatusDB.scratchpad_stored_type_str: OtapStatusStorageFormatter().otap_type_to_str(node_status["type"]),
                            OtapStatusDB.scratchpad_stored_status: node_status["status"],
                            OtapStatusDB.scratchpad_stored_status_str: OtapStatusStorageFormatter().otap_status_to_str(node_status["status"]),
                            OtapStatusDB.wirepas_stack_version: OtapStatusStorageFormatter().component_version_to_str(node_status["stack_version"]),
                            OtapStatusDB.wirepas_stack_area_id: node_status["stack_area_id"]
                        }

        try:
            storage_entry[OtapStatusDB.app_version] = OtapStatusStorageFormatter().component_version_to_str(node_status["app_version"])
            storage_entry[OtapStatusDB.app_area_id] = node_status["app_area_id"]
        except KeyError:
            # Fields not present in OTAP status
            storage_entry[OtapStatusDB.app_version] = None
            storage_entry[OtapStatusDB.app_area_id] = None

        try:
            storage_entry[OtapStatusDB.otap_action] = node_status["action"]
            storage_entry[OtapStatusDB.otap_action_str] = OtapStatusStorageFormatter().otap_action_to_str(node_status["action"])
            storage_entry[OtapStatusDB.otap_target_crc] = node_status["target_crc"]
            storage_entry[OtapStatusDB.otap_target_seq] = node_status["target_seq"]
            storage_entry[OtapStatusDB.otap_target_delay_m] = node_status["target_delay_m"]
            storage_entry[OtapStatusDB.otap_remaining_delay_m] = node_status["remaining_delay_m"]
        except KeyError:
            # Fields not present in OTAP status
            storage_entry[OtapStatusDB.otap_action] = None
            storage_entry[OtapStatusDB.otap_action_str] = None
            storage_entry[OtapStatusDB.otap_target_crc] = None
            storage_entry[OtapStatusDB.otap_target_seq] = None
            storage_entry[OtapStatusDB.otap_target_delay_m] = None
            storage_entry[OtapStatusDB.otap_remaining_delay_m] = None

        with OtapStatusDB._meta.database:

            logging.debug(f"Writing node's {node_address} status: {storage_entry} for nw {network_address} and travel_time {travel_time_ms}")
            try:
                OtapStatusDB.insert(storage_entry).on_conflict(
                                    conflict_target=[OtapStatusDB.node_address],
                                    preserve=[OtapStatusDB.node_address],
                                    update=storage_entry
                                    ).execute()

                logging.debug(f"Done writing entry.")
            except Exception as e:
                logging.error(f"Could not write entry. Reason: {e}")


class OtapStatusStorageFormatter:
    """ Class to format data to proper storage type
    """
    def otap_type_to_str(self, type_int):
        map_to_str = ("Blank", "Present", "Process")
        try:
            return map_to_str[type_int]
        except IndexError:
            logging.error(f"Invalid scratchpad type value received: {type_int}")
            return "Unknown"

    def otap_status_to_str(self, status):
        map_to_str = {
                        "0": "Success",
                        "1": "Flash error",
                        "2": "Invalid header",
                        "3": "Invalid CRC",
                        "4": "Auth error",
                        "5": "Decompression error",
                        "6": "No space",
                        "7": "Invalid file header",
                        "8": "Flash driver error",
                        "255": "New"
        }

        try:
            return map_to_str[str(status)]
        except (KeyError, TypeError):
            logging.error(f"Invalid scratchpad status value received: {status}")
            return "Unknown"

    def component_version_to_str(self, version):
        if version is not None:
            try:
                return f"{version[0]}.{version[1]}.{version[2]}.{version[3]}"
            except IndexError:
                logging.error(f"Invalid component version received: {version}")
                return "x.x.x.x"
        return version

    def otap_action_to_str(self, otap_action):
        map_to_str = {
                        "0": "no_otap",
                        "1": "propagate_only",
                        "2": "propagate_and_process",
                        "3": "propagate_and_process_with_delay",
                        "4": "legacy"
        }

        try:
            return map_to_str[str(otap_action)]
        except (KeyError, TypeError):
            logging.error(f"Invalid scratchpad action value received: {otap_action}")
            return "Unknown"


# 'peewee' documentation recommends to create this class to that
# any other database model definition will use the same storage
class _BaseModel(Model):
    class Meta:
        database = _DB_CONN

class OtapStatusDB(_BaseModel):
    """OTAP status database model definition: will store the nodes' status info
    Note: some fields allowed to be NULL as dependent on remote API status packet version.
    """

    # Mandatory fields
    node_address = BigIntegerField(primary_key=True)
    network_address = IntegerField()
    rx_timestamp_epoch_ms = IntegerField()
    tx_timestamp_epoch_ms = IntegerField()
    scratchpad_stored_seq = IntegerField()
    scratchpad_stored_crc = IntegerField()
    scratchpad_stored_len = IntegerField()
    scratchpad_stored_type = IntegerField()
    scratchpad_stored_type_str = FixedCharField(_MAX_LEN_FIELD_SCRATCHPAD_ST_TYPE)
    scratchpad_stored_status = IntegerField()
    scratchpad_stored_status_str = FixedCharField(_MAX_LEN_FIELD_SCRATCHPAD_ST_STATUS)
    wirepas_stack_version = FixedCharField(_MAX_LEN_FIELD_WIREPAS_STACK_VERSION)
    wirepas_stack_area_id = BigIntegerField()
    # Optional fields
    app_version = FixedCharField(_MAX_LEN_FIELD_APP_VERSION, null=True)
    app_area_id = BigIntegerField(null=True)
    otap_action = IntegerField(null=True)
    otap_action_str = FixedCharField(_MAX_LEN_FIELD_OTAP_ACTION, null=True)
    otap_target_crc = IntegerField(null=True)
    otap_target_seq = IntegerField(null=True)
    otap_target_delay_m = IntegerField(null=True)
    otap_remaining_delay_m = IntegerField(null=True)

    class Meta:
        table_name = _DB_DATA_TABLE_NAME

class OtapstatusDBMetadata(_BaseModel):
    """OTAP status metadata model definition
    """
    creator = FixedCharField(_MAX_LEN_FIELD_DB_CREATOR)
    schema_version = IntegerField()

    class Meta:
        table_name = _DB_METADATA_TABLE_NAME

