# Copyright 2021 Wirepas Ltd. All Rights Reserved.
#
# See file LICENSE.txt for full license details.
#
import wirepas_mesh_messaging as wmm
import logging
from wirepas_mqtt_library import WirepasNetworkInterface 


class WirepasTLVAppConfigHelper:
    """Class to ease operation management of app_config with Wirepas TLV format 

    This class contains methods allowing the modification (addition, removal) 
    of app_config data inside a Wirepas network compliant with the TLV format.
    It offers an abstraction for wirepas supported feature at application level
    like the local provisioning.

    """

    max_wirepas_app_config_size = 80

    def __init__(self, wni, network=None, gateway_sink_subset=None):
        """Constructor

        :param wni: Wirepas network interface
        :type wni: :obj:`~wirepas_mqtt_library.wirepas_network_interface.WirepasNetworkInterface`
        :param network: Network address concerned by the otap
        :param gateway_sink_subset: list of (gateway, sink) tupple concerned by the change
        
        :note: Either network or gateway_sink_subset must be specified
        """
        if wni is None:
            raise RuntimeError("No Wirepas Network Interface provided")
        self.wni = wni

        if gateway_sink_subset is None and network is None:
            raise RuntimeError("No gw/sink list provided nor network")
        
        if gateway_sink_subset is not None and network is not None:
            raise RuntimeError("gw/sink list and network cannot be specified together")

        self.network = network
        self.gateway_sink_subset = gateway_sink_subset

        self._removed_entries = list()
        self._new_entries = dict()

    def _get_list_target_sinks(self):
        sink_list = list()
        for gw, sink, config in self.wni.get_sinks(network_address=self.network):
            if self.gateway_sink_subset is not None and (gw, sink) not in self.gateway_sink_subset:
                continue
            # Get current app_config sequence even if not used anymore after 5.2
            sink_list.append((gw, sink, config["app_config_data"], config["app_config_seq"], config["app_config_diag"]))
        return sink_list

    def add_raw_entry(self, entry, value):
        """Add a new entry to the local cache

        Nothing is done on the network yet. It will be updated only when
        calling :meth:`~wirepas_mqtt_library.wirepas_tlv_app_config_helper.network_interface.update_entries`

        :param entry: new entry to add
        :type entry: int (between 0 and 0xffff)
        :param value: value for the entry
        :type value: bytearray
        """
        if entry < 0 or entry > 0xffff:
            raise ValueError("TLV type must be between 0 and 0xffff")
        self._new_entries[entry] = value
        return self

    def remove_raw_entry(self, entry):
        """Remove an entry from the local cache

        Nothing is done on the network yet. It will be updated only when
        calling :meth:`~wirepas_mqtt_library.wirepas_tlv_app_config_helper.network_interface.update_entries`

        :param entry: entry to remove
        :type entry: int (between 0 and 0xffff)
        """
        if entry < 0 or entry > 0xffff:
            raise ValueError("TLV type must be between 0 and 0xffff")
        
        self._removed_entries.append(entry)
        return self

    def setup_local_provisioning(self, enabled, psk_id=None, psk=None):
        """Set a new entry to enable or disable local provisioning

        :param enabled: True to enable local provisioning, False to disable it
        :type enabled: bool
        :param psk_id: Id of the psk on 4 bytes (only valid when enabled)
        :type psk_id: bytearray
        :param psk: psk to use for lacal provisioning on 32 bytes (only valid when enabled)
        :type psk: bytearray
        :raises ValueError: if psk and psk_id are not both set or unset or with a wrong size
        """
        # check parameters
        if psk_id is not None and psk_id.__len__() != 4:
            raise ValueError("psk_id must be a 4 bytes bytearray")

        if psk is not None and psk.__len__() != 32:
            raise ValueError("psk must be a 32 bytes bytearray")

        if psk_id is not None and psk is None or \
            psk_id is None and psk is not None:
            raise ValueError("psk and psk_id must be both unset or set")

        if enabled:
            if psk:
                return self.add_raw_entry(0xc4, b'\x02' + psk_id + psk)
            else:
                return self.add_raw_entry(0xc4, b'\x01')
        else:
            return self.add_raw_entry(0xc4, b'\x00')

    def update_entries(self, override=False):
        """Set a new entry to the NDP for the selected network or sink

        :param override: if True, if an app config is already present that doesn't
             follow the TLV format, full app config is overriden with TLV format with this entry
        :return: True if all specified sinks are updated, False otherwise.

        .. warning:: If returned value is False, only a subset of sinks may be updated
        """

        if self._new_entries.__len__() == 0 and \
           self._removed_entries.__len__() == 0:
            logging.info("Nothing to update, no entry added or removed")
            return

        logging.info("Updating entries of selected sinks")

        proceed = True

        current_configs = self._get_list_target_sinks()
        new_configs = list()
        # First check that it would feat in any specified sink
        for gw, sink, app_config, seq, diag in current_configs:
            logging.info("Checking app config final size for [%s][%s]" % (gw, sink))
            try:
                app_conf_obj = WirepasTLVAppConfig.from_value(app_config)
                previous_size = app_conf_obj.value.__len__()
            except ValueError:
                # Not a TLV format
                if override:
                    previous_size = app_config.__len__()
                    app_conf_obj = WirepasTLVAppConfig(None)
                else:
                    logging.error(
                        "Current app config from [%s][%s] doesn't follow TLV format and override is false"
                         % (gw, sink))
                    proceed = False
                    continue      

            # First remove entries
            removed = False
            for entry in self._removed_entries:
                removed |= app_conf_obj.remove_entry(entry)

            # Then add new ones
            added = False
            for entry, value in self._new_entries.items():
                added |= app_conf_obj.add_entry(entry, value)

            new_app_config = app_conf_obj.value
            new_size = new_app_config.__len__()
            logging.info("From %d -> %d" % (previous_size, new_size))

            if not added and not removed:
                logging.info("No change, skip update")
                # Nothing was changed, no need to update
                continue

            # Check size
            if new_size > WirepasTLVAppConfigHelper.max_wirepas_app_config_size:
                logging.error(
                    "Final app config for [%s][%s] will overflow by (%d)"
                    % (gw, sink, new_size - WirepasTLVAppConfigHelper.max_wirepas_app_config_size))
                proceed = False
                continue

            new_configs.append((gw, sink, new_app_config, seq + 1, diag))

        if not proceed:
            logging.error("Cannot add new app_config TLV entry")
            return False
        
        logging.info("Check done, doing the change")
        logging.debug("New configs are: %s" % new_configs)

        res = True
        for gw, sink, app_config, seq, diag in new_configs:
            new_config = {}
            new_config["app_config_data"] = app_config
            new_config["app_config_seq"] = seq
            new_config["app_config_diag"] = diag

            try:
                self.wni.set_sink_config(gw, sink, new_config)
            except TimeoutError:
                logging.error("Issue when setting new app config to [%s][%s]" % (gw, sink))
                res = False
                continue

            logging.info("New app config set for [%s][%s]" % (gw, sink))

            # Reset cache
            self._removed_entries = list()
            self._new_entries = dict()

        return res

    def __str__(self):
        str = ""
        for gw, sink, app_config, _, _ in self._get_list_target_sinks():
            try:
                app_conf_obj = WirepasTLVAppConfig.from_value(app_config)
                str += "[{}][{}] => {}\n".format(gw, sink, app_conf_obj)
            except ValueError:
                str += "[{}][{}] => No TLV format\n".format(gw, sink)
        return str     

class WirepasTLVAppConfig:
    wirepas_tlv_header = b'\xF6\x7E'

    def __init__(self, entries):
        if entries == None:
            entries = dict()
        self.entries = entries

    def add_entry(self, entry_type, entry_value):
        # Adding an entry for a type already existing, will remove
        # previous one
        try:
            if self.entries[entry_type] == entry_value:
                # Same value already
                return False
        except KeyError:
            # Key doesn't exist
            pass
        
        self.entries[entry_type] = entry_value
        return True

    def remove_entry(self, entry_type):
        try:
            del self.entries[entry_type]
            return True
        except KeyError:
            logging.warning("Trying to remove a key that doesn't exist")
            return False

    def _generate_app_config(entries):
        """ Generate app config from a dic of type -> value
        """
        # Create the app_config holder with correct header
        app_config = bytearray(WirepasTLVAppConfig.wirepas_tlv_header)

        # Add number of TLV entries
        app_config.append(entries.__len__())
        
        # Add one by one the entries
        for t, v in entries.items():
            l = v.__len__()
            logging.debug("Adding: t = 0x%x, l = %d" % (t, l))
            
            if (t > 0xffff):
                raise ValueError("Type must be feat on 2 bytes: 0x%x", t)

            # Add Type LSByte
            app_config.append(t & 0xff)
            if (t >= 256):
                # Long type, set MSBit of length to 1
                app_config.append(l | 0x80)
                app_config.append(t >> 8 & 0xff)
            else:
                app_config.append(l)

            # Add the value
            app_config += v

        logging.debug("Gen app_config is %s" % app_config.hex())
        return app_config

    def _parse_app_config(app_config):
        # Check that it starts with right key
        if app_config[0:2] != WirepasTLVAppConfig.wirepas_tlv_header:
            # It is not an app config following Wirepas TLV format
            logging.debug("Not a Wirepas TLV app config")
            return None

        # Check number of TLV entries
        tlv_entries = app_config[2]
        logging.debug("Number of tlv entries: %d" % tlv_entries)

        app_config = app_config[3:]

        entries={} 
        # Iterate the different entries
        while (tlv_entries > 0):
            value_offset = 2
            # Check type first
            t = app_config[0]
            if app_config[1] >= 0x80:
                # We have a long type
                t += app_config[2] * 256
                value_offset += 1
            
            l = app_config[1] & ~0x80
            logging.debug("t = 0x%x, l = %d," % (t, l))
            entries[t] = app_config[value_offset:value_offset+l]
            
            app_config = app_config[value_offset+l:]
            tlv_entries-=1

        return entries

    @classmethod
    def from_value(cls, app_config):
        d = WirepasTLVAppConfig._parse_app_config(app_config)
        if d == None:
            raise ValueError("Not a Wirepas TLV app_config format")
        return cls(d)

    @property
    def value(self):
        return WirepasTLVAppConfig._generate_app_config(self.entries)

    def __str__(self):
        str = "{"
        for t, v in self.entries.items():
            if str.__len__() > 2:
                str+=", "
            str+= "0x%x:%s" % (t, v.hex())

        str += "}"
        return str

