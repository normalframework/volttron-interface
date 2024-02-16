# -*- coding: utf-8 -*-
# vim: set fenc=utf-8 ft=python sw=4 ts=4 sts=4 et:
#


import time

from platform_driver.interfaces import BaseInterface, BaseRegister, BasicRevert

import logging
import json
import requests

_log = logging.getLogger(__name__)
type_mapping = {"string": str,
                "int": int,
                "integer": int,
                "float": float,
                "bool": bool,
                "boolean": bool}
GRPC_TIMEOUT = 30

DEFAULT_NAME_FORMAT_STRING = "{uuid}/device_id:{device_id}/device_name:{device_prop_object_name}/object_name:{prop_object_name}"

class NormalRegister(BaseRegister):
    def __init__(self, point, nameFormat):
        #     register_type, read_only, pointName, units, description = ''):
        read_only = True
        point['attrs']['uuid'] = point['uuid']
        try: 
            pointName = nameFormat.format(**point['attrs'])
        except Exception as e:
            print (e)
            pointName = DEFAULT_NAME_FORMAT_STRING.format(**point['attrs'])
        units = point.get('displayUnits', "")

        # parse the HPL data for writing
        self.uuid = point['uuid']
        
        # clear the HPL field since we might not have the right types
        # loaded to serialize it.
        description = json.dumps(point)
        self.point = point

        print(pointName, units)
        super(NormalRegister, self).__init__("byte", read_only, pointName, units,
                                             description=description)


class Interface(BaseInterface):
    def __init__(self, **kwargs):
        super(Interface, self).__init__(**kwargs)

    def _test_loop(self):
        while True:
            time.sleep(1)
            try:
                val = self.get_point("ANALOG OUTPUT 0")
            except Exception as e:
                print (e)
            else:
                print (repr(val))

    def configure(self, config_dict, registry_config_str):
        self.nfurl = config_dict.get("nfurl", "localhost:8080")
        self.scrape_window = int(config_dict.get("scrape_window", 300))
        self.default_priority = int(config_dict.get("priority", 14))
        self.query = config_dict.get("structured_query", {
            "field": {
                "property": "period",
                "numeric": {
                    "minValue": 1,
                    "maxInfinity": True,
                }
            }
        })
        self.layer = config_dict.get("layer", "")
        self.nameFormat = config_dict.get("topic_name_format", DEFAULT_NAME_FORMAT_STRING)
        self.written_points = set([])

        offset, stride, total= 0, 100, 1
        try:
            while offset < total:
                # TODO: rename hpl -> layer when we update the normalgw proto files
                res = requests.post(self.nfurl + "/api/v1/point/query", json={
                    "layer": self.layer,
                    "query": "SimpleServer",
                    "page_size": stride,
                    "page_offset": offset,
                    "stride": stride,
                })
                points = res.json()
                offset += len(points['points'])
                total = int(points['totalCount'])
                print ("Got points batch {}; total is {}".format(len(points['points']), total))
                for p in points['points']:
                    reg = NormalRegister(p, self.nameFormat)
                    self.insert_register(reg)
        except Exception as e:
            print ("Error loading points: ", e)
            import traceback
            traceback.print_exc()


    def get_point(self, point_name):
        """Get Point performs a BACnet ReadProperty in the underlying system

        Note, that NF may combine get_point requests made concurrently
        to the same device into a ReadPropertyMultiple request.

        You could set request.options.unmergable = True to prevent this.
        """
        register = self.get_register_by_name(point_name)
        if not register:
            raise RuntimeError(
                "Point not found: " + point_name)

        # the command service now provides type conversions and in the
        # future will let this work with multiple HPLs.
        res = requests.post(self.nfurl + "/api/v2/command/read", json={
            "reads": [
                {
                    "point": {
                        "uuid": register.uuid,
                        "layer": "hpl:bacnet:1",
                    }
                }
            ]
        })
        if res.status_code == 200:
            return float(res.json()["results"][0]["scalar"])
        else:
            raise RuntimeError("Could not write point")

    def set_point(self, point_name, value, priority=None):
        """Write to a BACnet point using NF

        This uses the BACnet addresses loaded during configuration to
        address the point.

        """
        register = self.get_register_by_name(point_name)
        if not register:
            raise RuntimeError(
                "Point not found: " + point_name)

        # the command service now provides type conversions and in the
        # future will let this work with multiple HPLs.
        res = requests.post(self.nfurl + "/api/v2/command/write", json={
            "writes": [
                {
                    "bacnetOptions": {
                        # always write default property = PROP_PRESENT_VALUE now
                        "priority": priority or self.default_priority,
                    },
                    "point": {
                        "uuid": register.uuid,
                        "layer": "hpl:bacnet:1",
                    },
                    "value": {
                        "double": value
                    }
                }
            ]
        })
        if res.status_code == 200:
            self.written_points.add(point_name)
        else:
            raise RuntimeError("Could not write point")


    def revert_point(self, point_name, priority=None):
        return self.set_point(point_name, None, priority)

    def revert_all(self):
        for p in self.written_points:
            self.revert_point(self, p, priority)
    
    def scrape_all(self):
        """Read current values for all of the points.  Right now this doesn't
        initiate a real BACnet write since that is managed by NF, but
        only reads back the latest cached value.
        """
        uuids = [p.uuid for p in self.point_map.values()]
        uuid_names = {p.uuid: name for (name, p) in self.point_map.items()}
        rv = {}

        from_ = int(time.time() - self.scrape_window)
        to_ = int(time.time())

        for i in range(0, len(uuid_names), 100):
            try:
                data = requests.get(self.nfurl + "/api/v1/point/data", params={
                    "layer": "hpl:bacnet:1",
                    "uuids": uuids[i:i+100],
                    "from.seconds": from_,
                    "to.seconds": to_,
                    "window.seconds": self.scrape_window,
                    "method": "LAST"}).json()
            except Exception as e:
                print (e)
                continue

            for v in data['data']:
                if len(v['values']) == 0:
                    continue
                rv[uuid_names[v['uuid']]] = v['values'][-1]['double']
            print ("Offset: %d count: %d" % (i, len(data['data'])))

        print ("returning %i readings" % len(rv))
        return rv

