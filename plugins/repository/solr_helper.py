import os
import configparser
from pycsw import wsgi
from pycsw.core import util

def get_config():
    pycsw_root = wsgi.get_pycsw_root_path(os.environ, os.environ)
    configuration_path = wsgi.get_configuration_path(os.environ, os.environ, pycsw_root)

    return util.parse_ini_config(configuration_path)

def get_config_parser(section, entry):
    return get_config().get(section, entry)

def get_collection_filter():
    pycsw_root = wsgi.get_pycsw_root_path(os.environ, os.environ)
    configuration_path = wsgi.get_configuration_path(os.environ, os.environ, pycsw_root)

    config = configparser.ConfigParser(interpolation=util.EnvInterpolation())

    with open(configuration_path, encoding='utf-8') as scp:
        config.read_file(scp)
        collection_filter = config.get("repository", "adc_collection")
    return collection_filter.replace(',',' ')

def get_bbox(query, right_hand_envelope=False):
        # first check if there is a key: "ogc:Filter"
    if "ogc:BBOX" in query["_dict"]["ogc:Filter"]:

        print("Got bbox filter query")
        lc = (
            query["_dict"]["ogc:Filter"]["ogc:BBOX"]["gml:Envelope"]["gml:lowerCorner"]
            .strip()
            .split()
        )
        uc = (
            query["_dict"]["ogc:Filter"]["ogc:BBOX"]["gml:Envelope"]["gml:upperCorner"]
            .strip()
            .split()
        )
        lc = [float(i) for i in lc]
        uc = [float(i) for i in uc]
        # Right hand polygon
        if right_hand_envelope:
            envelope = (",").join(
                [
                    f"POLYGON(({lc[0]} {lc[1]}",
                    f"{uc[0]} {lc[1]}",
                    f"{uc[0]} {uc[1]}",
                    f"{lc[0]} {uc[1]}",
                    f"{lc[0]} {lc[1]}))",
                ]
            )
            return envelope
        else:
            min_x, max_x, min_y, max_y = lc[1], uc[1], lc[0], uc[0]
            envelope = f"ENVELOPE({min_y},{max_y},{max_x},{min_x})"
            #envelope = f"ENVELOPE({min_x},{max_x},{max_y},{min_y})"
            print(envelope)
            return  envelope
    if "ogc:And" in query["_dict"]["ogc:Filter"]:
        if "ogc:BBOX" in query["_dict"]["ogc:Filter"]["ogc:And"]:
            print("Got bbox AND filter query")
            lc = (
                query["_dict"]["ogc:Filter"]["ogc:And"]["ogc:BBOX"]["gml:Envelope"]["gml:lowerCorner"]
                .strip()
                .split()
            )
            uc = (
                query["_dict"]["ogc:Filter"]["ogc:And"]["ogc:BBOX"]["gml:Envelope"]["gml:upperCorner"]
                .strip()
                .split()
            )
            lc = [float(i) for i in lc]
            uc = [float(i) for i in uc]
            # Right hand polygon
            if right_hand_envelope:
                envelope = (",").join(
                    [
                        f"POLYGON(({lc[0]} {lc[1]}",
                        f"{uc[0]} {lc[1]}",
                        f"{uc[0]} {uc[1]}",
                        f"{lc[0]} {uc[1]}",
                        f"{lc[0]} {lc[1]}))",
                        ]
                    )
                return envelope
            else:
                min_x, max_x, min_y, max_y = lc[1], uc[1], lc[0], uc[0]
                envelope = f"ENVELOPE({min_y},{max_y},{max_x},{min_x})"
                #envelope = f"ENVELOPE({min_x},{max_x},{max_y},{min_y})"
                print(envelope)
                return  envelope

        else:
            print(f"ogc:BBOX not found in {query}")
            return False
    else:
        return False
