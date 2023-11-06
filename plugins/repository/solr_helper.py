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


def parse_time_query(constraint, params, and_flag=False):
    dateformat="%Y-%m-%dT%H:%M:%SZ"
    if not and_flag:
        qstring = constraint["_dict"]["ogc:Filter"]["ogc:And"]
    else:
        qstring = constraint["_dict"]["ogc:Filter"]
    if "ogc:PropertyIsGreaterThanOrEqualTo" in qstring:
        tempBegin = qstring.get("ogc:PropertyIsGreaterThanOrEqualTo", False)
        # print("tempBegin", type(tempBegin), tempBegin)
        if tempBegin:
            begin = qstring["ogc:PropertyIsGreaterThanOrEqualTo"]["ogc:Literal"]
            # print('begin:', begin)
            datestring = dparser.parse(begin)
            # print(datestring)
            params["fq"].append("temporal_extent_start_date:[%s TO *]" % datestring.strftime(dateformat))
            # print(params)
    if "ogc:PropertyIsLessThanOrEqualTo" in qstring:
        tempEnd = qstring.get("ogc:PropertyIsLessThanOrEqualTo", False)
        # print(tempEnd)
        # print("tempEnd", type(tempEnd), tempEnd)
        if tempEnd:
            end = qstring["ogc:PropertyIsLessThanOrEqualTo"]["ogc:Literal"]
            # print('end:', end)
            datestring = dparser.parse(end)
            # print(datestring)
            params["fq"].append("temporal_extent_end_date:[%s TO *]" % datestring.strftime(dateformat))
            # print(params)
    return params

def parse_field_query(constraint, params, and_flag=False):
    
    if not and_flag:
        property_name = list(constraint["_dict"]["ogc:Filter"].keys())[0]
        print('property_name: ', property_name)
        qstring = constraint["_dict"]["ogc:Filter"][property_name]["ogc:Literal"]
        name = constraint["_dict"]["ogc:Filter"][property_name]["ogc:PropertyName"]
    if and_flag:
        property_name = list(constraint["_dict"]["ogc:Filter"]["ogc:And"].keys())[0]
        print('property_name: ', property_name)
        qstring = constraint["_dict"]["ogc:Filter"]["ogc:And"][property_name]["ogc:Literal"]
        name = constraint["_dict"]["ogc:Filter"]["ogc:And"][property_name]["ogc:PropertyName"]
    qstring = qstring.replace('%','*')
    if 'title' in name.lower():
        params["fq"].append("title:(%s)" % qstring)
    elif 'abstract' in name.lower():
        params["fq"].append("abstract:(%s)" % qstring)
    elif 'subject' in name.lower():
        params["fq"].append("keywords_keyword:(%s)" % qstring)
    elif 'creator' in name.lower():
        params["fq"].append("personnel_investigator_name:(%s)" % qstring)
    elif 'contributor' in name.lower():
        params["fq"].append("personnel_technical_name:(%s) OR personnel_metadata_author_name:(%s)" % (qstring, qstring))
    elif 'dc:source' in name:
        params["fq"].append("related_url_landing_page:(%s)" % qstring)
    elif 'format' in name.lower():
        params["fq"].append("storage_information_file_format:(%s)" % qstring)
    elif 'language' in name.lower():
        params["fq"].append("dataset_language:(%s)" % qstring)
    elif 'publisher' in name.lower():
        params["fq"].append("dataset_citation_publisher:(%s)" % qstring)
    elif 'rights' in name.lower():
        params["fq"].append("use_constraint_identifier:(%s) OR use_constraint_license_text:(%s)" % (qstring, qstring))
    else:
        params["q"] = "full_text:(%s)" % qstring
    return params