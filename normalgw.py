# -*- coding: utf-8 -*-
# vim: set fenc=utf-8 ft=python sw=4 ts=4 sts=4 et:
#


import random

from platform_driver.interfaces import BaseInterface, BaseRegister, BasicRevert
from csv import DictReader
from io import StringIO
import logging
import json

import grpc
from google.protobuf import timestamp_pb2
from google.protobuf import duration_pb2
from google.protobuf.json_format import MessageToDict
import json

import normalgw.hpl.point_pb2
import normalgw.hpl.point_pb2_grpc
import normalgw.bacnet.scan_pb2
import normalgw.bacnet.bacnet_pb2
import normalgw.bacnet.bacnet_pb2_grpc

_log = logging.getLogger(__name__)
type_mapping = {"string": str,
                "int": int,
                "integer": int,
                "float": float,
                "bool": bool,
                "boolean": bool}


class NormalRegister(BaseRegister):
    def __init__(self, point):
        #     register_type, read_only, pointName, units, description = ''):
        read_only = True
        pointName = (point.uuid + '/device_id:' +
                     point.attrs.get('device_id', '') + '/device_name:' + 
                     point.attrs.get('device_prop_object_name', '') + '/object_name:' +
                     point.attrs.get('prop_object_name', ''))
        #point.attrs.get('de')
        units = point.attrs.get("prop_units", "")

        # parse the HPL data for writing
        self.bacnet = normalgw.bacnet.scan_pb2.BACnetPoint()
        point.hpldata.Unpack(self.bacnet)
        
        # clear the HPL field since we might not have the right types
        # loaded to serialize it.
        point.ClearField("hpldata")
        description = json.dumps(MessageToDict(point))
        self.point = point

        print(pointName)
        super(NormalRegister, self).__init__("byte", read_only, pointName, units,
                                             description=description)


class Interface(BaseInterface):
    def __init__(self, **kwargs):
        super(Interface, self).__init__(**kwargs)

    def configure(self, config_dict, registry_config_str):
        self.point_service = config_dict.get("point_service", "localhost:8080")
        self.bacnet_service = config_dict.get("bacnet_service", "localhost:8080")
        self.scrape_window = int(config_dict.get("scrape_window", 300))
        self.default_priority = int(config_dict.get("priority", 14))
        self.written_points = set([])

        channel = grpc.insecure_channel(self.point_service)
        service = normalgw.hpl.point_pb2_grpc.PointManagerStub(channel)

        offset, stride, total= 0, 100, 1
        try:
            while offset < total:
                req = normalgw.hpl.point_pb2.GetPointsRequest(hpl="hpl:bacnet:1",
                                                              query="@period:[1, +inf]",
                                                              page_size=stride,
                                                              page_offset=offset)
                points = service.GetPoints(req)
                offset += len(points.points)
                total = points.total_count
                print ("Got points batch {}; total is {}".format(len(points.points), total))
                for p in points.points:
                    reg = NormalRegister(p)
                    self.insert_register(reg)
        except Exception as e:
            print ("Error loading points: ", e)
        finally:
            channel.close()

    def get_point(self, point_name):
        register = self.get_register_by_name(point_name)
        return register.value

    def set_point(self, point_name, value, priority=None):
        """Write to a BACnet point using NF

        This uses the BACnet addresses loaded during configuration to
        address the point.

        """
        register = self.get_register_by_name(point_name)
        if not register:
            raise RuntimeError(
                "Point not found: " + point_name)

        val = normalgw.bacnet.bacnet_pb2.ApplicationDataValue()
        point_type = register.bacnet.example_value.WhichOneof("value")
        if value == None:
            val.null = True
        if point_type == "boolean":
            val.boolean = bool(value)
        elif point_type == "unsigned":
            val.unsigned = int(value)
        elif point_type == "signed":
            val.signed = int(value)
        elif point_type == "real":
            val.real = float(value)
        elif point_type == "double":
            val.double = double(value)
        elif point_type == "character_string":
            val.character_string = str(value)
        
        request = normalgw.bacnet.bacnet_pb2.WritePropertyRequest(**{
            "device_address": register.bacnet.device_address,
            "property": register.bacnet.property,
            "priority":(priority or self.default_priority),
            "value": val})

        channel = grpc.insecure_channel(self.bacnet_service)
        service = normalgw.bacnet.bacnet_pb2_grpc.BacnetStub(channel)
        try:
            resp = service.WriteProperty(request)
            if resp.error.WhichOneof("error_type") is not None:
                raise RuntimeError(
                    "Error writing to register: " + str(resp.error))
        except:
            raise
        else:
            self.written_points.add(point_name)
        finally:
            channel.close()



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
        channel = grpc.insecure_channel(self.point_service)
        service = normalgw.hpl.point_pb2_grpc.PointManagerStub(channel)

        from_ = timestamp_pb2.Timestamp()
        from_.GetCurrentTime()
        from_.seconds -= self.scrape_window
        to_ = timestamp_pb2.Timestamp()
        to_.GetCurrentTime()
        dur = duration_pb2.Duration()
        dur.FromSeconds(self.scrape_window)

        uuid_names = {x[:36]: x for x in self.get_register_names_view()}
        uuids = list(uuid_names.keys())
        rv = {}

        for i in range(0, len(uuid_names), 100):
            try:
                req = normalgw.hpl.point_pb2.GetDataRequest(**{"layer": "hpl:bacnet:1",
                                                               "uuids": uuids[i:i+100],
                                                               "from": from_,
                                                               "to": to_,
                                                               "window": dur,
                                                               "method": "LAST"})
                data = service.GetData(req)
            except Exception as e:
                print (e)
                continue

            for v in data.data:
                if len(v.values) == 0:
                    continue
                rv[uuid_names[v.uuid]] = v.values[-1].double
            print ("Offset: %d count: %d" % (i, len(data.data)))

        print ("returning %i readings" % len(rv))
        channel.close()
        return rv

