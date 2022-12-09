# -*- coding: iso-8859-15 -*-
# =================================================================
#
# Authors: Tom Kralidis <tomkralidis@gmail.com>
#
# Copyright (c) 2022 Tom Kralidis
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

import base64
import configparser
from datetime import datetime, timezone
import dateutil.parser as dparser
import logging
from urllib.parse import urlencode

import requests

from pycsw.core import util
from pycsw.core.etree import etree
import os

from http.client import HTTPConnection  # py3

import json

LOGGER = logging.getLogger(__name__)
#HTTPConnection.debuglevel = 1

from pycsw.plugins.repository.solr_helper import get_bbox
from pycsw.plugins.repository.solr_helper import get_collection_filter
from pycsw.plugins.repository.solr_helper import get_config



class SOLRMETNORepository(object):
    """
    Class to interact with underlying METNO SOLR backend repository
    """
    def __init__(self, context, repo_filter=None):
        """
        Initialize repository
        """
        #print('SOLRMETNORepository __init__')
        self.context = context
        self.filter = repo_filter
        self.fts = False
        self.label = 'MetNO/SOLR'
        self.local_ingest = True
        self.solr_select_url = '%s/select' % self.filter
        self.dbtype = 'SOLR'

        #self.config_obj = get_config()
        #print(
        self.adc_collection_filter = get_collection_filter()
        #print(self.adc_collection_filter)

        # generate core queryables db and obj bindings
        self.queryables = {}

        for tname in self.context.model['typenames']:
            for qname in self.context.model['typenames'][tname]['queryables']:
                self.queryables[qname] = {}
                items = self.context.model['typenames'][tname]['queryables'][qname].items()

                for qkey, qvalue in items:
                    self.queryables[qname][qkey] = qvalue

        # flatten all queryables
        self.queryables['_all'] = {}
        for qbl in self.queryables:
            self.queryables['_all'].update(self.queryables[qbl])
        self.queryables['_all'].update(self.context.md_core_model['mappings'])

        #self.dataset = type('dataset', (object,), {})

    def dataset(self, record):
        """
        Stub to mock a pycsw dataset object for Transactions
        """
        #print('dataset stub')
        return type('dataset', (object,), record)

    def query_ids(self, ids):
        """
        Query by list of identifiers
        """

        results = []

        params = {
            'fq': ['metadata_identifier:("%s")' % '" OR "'.join(ids)],
            'q.op': 'OR',
            'q': '*:*'
        }

        if self.adc_collection_filter != '' or self.adc_collection_filter !=None:
            params['fq'].append('collection:(%s)' % self.adc_collection_filter)

        print(params)
        response = requests.get(self.solr_select_url, params=params)

        response = response.json()

        for doc in response['response']['docs']:
            results.append(self._doc2record(doc))
        #print("query by ID \n")
        return results


    def query_domain(self, domain, typenames, domainquerytype='list', count=False):
        """
        Query by property domain values
        """
        #print('Query domain')
        results = []

        params = {
            'q': '*:*',
            'rows': 0,
            'facet': 'true',
            'facet.query': 'distinct',
            'facet.type': 'terms',
            'facet.field': domain,
            'fq': [],
        }
        if self.adc_collection_filter != '' or self.adc_collection_filter !=None:
            params['fq'].append('collection:(%s)' % self.adc_collection_filter)

        print(params)
        response = requests.get('%s/select' % self.filter, params=params).json()

        counts = response['facet_counts']['facet_fields'][domain]

        for term in zip(*([iter(counts)] * 2)):
            LOGGER.debug('Term: %s', term)
            results.append(term)

        return results

    def query_insert(self, direction='max'):
        """
        Query to get latest (default) or earliest update to repository
        """
        #print('query_insert')
        if direction == 'min':
            sort_order = 'asc'
        else:
            sort_order = 'desc'

        params = {
            'q': '*:*',
            'q.op': 'OR',
            'fl': 'timestamp',
            'sort': 'timestamp %s' % sort_order,
            'fq': []
        }
        if self.adc_collection_filter != '' or self.adc_collection_filter !=None:
            params['fq'].append('collection:(%s)' % self.adc_collection_filter)


        response = requests.get('%s/select' % self.filter, params=params).json()

        timestamp = datetime.strptime(response['response']['docs'][0]['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')

        return timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')

    def query_source(self, source):
        """
        Query by source
        """
        #print('Query_source')
        return NotImplementedError()

    def query(self, constraint, sortby=None, typenames=None, maxrecords=10, startposition=0):
        """
        Query records from underlying repository
        """
        #print("Query records\n")
        #print('###################################################',
        #      '\n',
        #      constraint)
        #print(dir(constraint), type(constraint))

        print(json.dumps(constraint,indent=2, default=str))
        results = []

        dateformat="%Y-%m-%dT%H:%M:%SZ"
        # Default search params
        params = {
            'q': '*:*',
            'q.op': 'OR',
            'start': startposition,
            'rows': maxrecords,
            'fq': [],
        }

        #print(len(constraint))
        #Only add query constraint if we have some, else return all records

        # if 'where' in constraint:
        #     if 'anytext' in constraint['where']:
        #         qstring = constraint['values'][0]
        #         qstring = qstring.replace('%','*')
        #         params["q"] = "full_text:(%s)" % qstring
        #     if 'query_spatial' in constraint['where']:
        #         envelope = get_bbox(constraint)
        #         if envelope != False:
        #             solr_bbox_query = "{!field f=bbox score=overlapRatio}"+f"Within({envelope})"
        #             params['fq'] = solr_bbox_query
        #     print(params)
        if len(constraint) != 0:
            #print('parsing constraints')
            #Do/check for  spatial search
            envelope = get_bbox(constraint)
            if envelope != False:
                solr_bbox_query = "{!field f=bbox score=overlapRatio}"+f"Within({envelope})"
                params['fq'].append(solr_bbox_query)
            # if constraint is none, return all the recordsogc:PropertyName'
            # otherwise catch the filter syntax and translate it
            #
            #print('current constraint\n')
            #print(json.dumps(constraint["_dict"]["ogc:Filter"],indent=2, default=str))

            #Do/check for  text search
            qstring = "*:*"
            if "ogc:PropertyIsLike" in constraint["_dict"]["ogc:Filter"]:
                qstring = constraint["_dict"]["ogc:Filter"]["ogc:PropertyIsLike"]["ogc:Literal"]
                qstring = qstring.replace('%','*')
                params["q"] = "full_text:(%s)" % qstring
                #print('no and isLike ' ,qstring)
            if "ogc:PropertyIsEqualTo" in constraint["_dict"]["ogc:Filter"]:
                qstring = constraint["_dict"]["ogc:Filter"]["ogc:PropertyIsEqualTo"]["ogc:Literal"]
                params["q"] = "full_text:(%s)" % qstring
                #print('no and isEqualto' ,qstring)
            #print('Check and')
            if "ogc:And" in constraint["_dict"]["ogc:Filter"]:
                #print('Got AND')
                if "ogc:And" in constraint["_dict"]["ogc:Filter"]["ogc:And"]:
                    #print('Got AND _ AND ')
                    anyText = constraint["_dict"]["ogc:Filter"]["ogc:And"].get("ogc:PropertyIsLike",False)
                    #print('AnyText: %s' % anyText)
                    #if "csw:AnyText" in constraint["_dict"]["ogc:Filter"]["ogc:And"]["ogc:PropertyIsLike"]["ogc:PropertyName"]:
                    if anyText:
                        qstring = constraint["_dict"]["ogc:Filter"]["ogc:And"]["ogc:PropertyIsLike"]["ogc:Literal"]
                        qstring = qstring.replace('%','*')
                        params["q"] = "full_text:(%s)" % qstring
                        #print('Anytext qstring:' ,qstring)

                    if not anyText:
                        anyText = constraint["_dict"]["ogc:Filter"]["ogc:And"].get("ogc:PropertyIsEqualTo",False)
                        #print('AnyText: %s' % anyText)
                        #if "csw:AnyText" in constraint["_dict"]["ogc:Filter"]["ogc:And"]["ogc:PropertyIsLike"]["ogc:PropertyName"]:
                        if anyText:
                            qstring = constraint["_dict"]["ogc:Filter"]["ogc:And"]["ogc:PropertyIsEqualTo"]["ogc:Literal"]
                            qstring = qstring.replace('%','*')
                            params["q"] = "full_text:(%s)" % qstring
                            #print('Anytext qstring:' ,qstring)
                    #print('Test temoporal\n', constraint["_dict"]["ogc:Filter"])
                    tempBegin = constraint["_dict"]["ogc:Filter"]["ogc:And"]["ogc:And"].get("ogc:PropertyIsGreaterThanOrEqualTo",False)

                    #if "apiso:TempExtent_begin" in constraint["_dict"]["ogc:Filter"]["ogc:And"]["ogc:PropertyIsGreaterThanOrEqualTo"]["ogc:PropertyName"]:
                    #print("Begin string AND AND: %s" % tempBegin)
                    if tempBegin:
                        begin = constraint["_dict"]["ogc:Filter"]["ogc:And"]["ogc:And"]["ogc:PropertyIsGreaterThanOrEqualTo"]["ogc:Literal"]
                        datestring = dparser.parse(begin)
                        #print('Begin date: %s' %datestring.strftime(dateformat))
                        #print('Begin date: %s' % util.datetime2iso8601(begin))
                        params["fq"].append("temporal_extent_start_date:[%s TO *]" % datestring.strftime(dateformat))

                    tempEnd = constraint["_dict"]["ogc:Filter"]["ogc:And"]["ogc:And"].get("ogc:PropertyIsLessThanOrEqualTo",False)
                    #print("End string:  %s" % tempEnd)
                    #if "apiso:TempExtent_end" in constraint["_dict"]["ogc:Filter"]["ogc:And"]["ogc:PropertyIsLessThanOrEqualTo"]["ogc:PropertyName"]:
                    if tempEnd:
                        end = constraint["_dict"]["ogc:Filter"]["ogc:And"]["ogc:And"]["ogc:PropertyIsLessThanOrEqualTo"]["ogc:Literal"]
                        datestring = dparser.parse(end)
                        #print('End date: %s' %datestring.strftime(dateformat))
                        params["fq"].append("temporal_extent_end_date:[* TO %s]" % datestring.strftime(dateformat))
                else:
                    anyText = constraint["_dict"]["ogc:Filter"]["ogc:And"].get("ogc:PropertyIsLike",False)
                    #print('AnyText: %s' % anyText)
                    #if "csw:AnyText" in constraint["_dict"]["ogc:Filter"]["ogc:And"]["ogc:PropertyIsLike"]["ogc:PropertyName"]:
                    if anyText:
                        qstring = constraint["_dict"]["ogc:Filter"]["ogc:And"]["ogc:PropertyIsLike"]["ogc:Literal"]
                        qstring = qstring.replace('%','*')
                        params["q"] = "full_text:(%s)" % qstring
                        #print('Anytext qstring:' ,qstring)
                    if not anyText:
                        anyText = constraint["_dict"]["ogc:Filter"]["ogc:And"].get("ogc:PropertyIsEqualTo",False)
                        #print('AnyText: %s' % anyText)
                        #if "csw:AnyText" in constraint["_dict"]["ogc:Filter"]["ogc:And"]["ogc:PropertyIsLike"]["ogc:PropertyName"]:
                        if anyText:
                            qstring = constraint["_dict"]["ogc:Filter"]["ogc:And"]["ogc:And"]["ogc:PropertyIsLike"]["ogc:Literal"]
                            qstring = qstring.replace('%','*')
                            params["q"] = "full_text:(%s)" % qstring
                            #print('Anytext qstring:' ,qstring)

                    #print('Test temoporal', constraint["_dict"]["ogc:Filter"])
                    tempBegin = constraint["_dict"]["ogc:Filter"]["ogc:And"].get("ogc:PropertyIsGreaterThanOrEqualTo",False)

                    #if "apiso:TempExtent_begin" in constraint["_dict"]["ogc:Filter"]["ogc:And"]["ogc:PropertyIsGreaterThanOrEqualTo"]["ogc:PropertyName"]:
                    #print("Begin string: %s" % tempBegin)
                    if tempBegin:
                        begin = constraint["_dict"]["ogc:Filter"]["ogc:And"]["ogc:PropertyIsGreaterThanOrEqualTo"]["ogc:Literal"]
                        datestring = dparser.parse(begin)
                        #print('Begin date: %s' %datestring.strftime(dateformat))
                        #print('Begin date: %s' % util.datetime2iso8601(begin))
                        params["fq"].append("temporal_extent_start_date:[%s TO *]" % datestring.strftime(dateformat))

                    tempEnd = constraint["_dict"]["ogc:Filter"]["ogc:And"].get("ogc:PropertyIsLessThanOrEqualTo",False)
                    #print("End string:  %s" % tempEnd)
                    #if "apiso:TempExtent_end" in constraint["_dict"]["ogc:Filter"]["ogc:And"]["ogc:PropertyIsLessThanOrEqualTo"]["ogc:PropertyName"]:
                    if tempEnd:
                        end = constraint["_dict"]["ogc:Filter"]["ogc:And"]["ogc:PropertyIsLessThanOrEqualTo"]["ogc:Literal"]
                        datestring = dparser.parse(end)
                        #print('End date: %s' %datestring.strftime(dateformat))
                        params["fq"].append("temporal_extent_end_date:[* TO %s]" % datestring.strftime(dateformat))
                    #print(json.dumps(params, indent=2, default=str))
        #Solr query
        if self.adc_collection_filter != '' or self.adc_collection_filter !=None:
            params['fq'].append('collection:(%s)' % self.adc_collection_filter)

        print("#########################################################\n")
        print(json.dumps(params, indent=2, default=str))
        #print(('%s/select' % self.filter, params=params).json())
        response = requests.get('%s/select' % self.filter, params=params).json()
        #print(response)

        total = response['response']['numFound']
        # response = response.json()
        print('Found: %s' %total)
        for doc in response['response']['docs']:
            results.append(self._doc2record(doc))

        #print(total)

        # TODO
        # transform constraint['_dict'] into SOLR query syntax
        #  - set paging from maxrecords and startposition
        # transform each doc result into pycsw dataset object
        # return the total hits (int, and list of dataset objects)

        #DEBUG
        #if "_dict" in constraint:
        #    print("constraint: ", constraint['_dict'])
        return str(total), results

    def _doc2record(self, doc):
        """
        Transform a SOLR doc into a pycsw dataset object
        """

        record = {}

        record['identifier'] = doc['metadata_identifier']
        record['typename'] = 'gmd:MD_Metadata'
        record['schema'] = 'http://www.isotc211.org/2005/gmd'
        record['type'] = 'dataset'
        record['wkt_geometry'] = doc['bbox']
        record['title'] = doc['title'][0]
        record['abstract'] = doc['abstract'][0]
        record['topicategory'] = ','.join(doc['iso_topic_category'])
        record['keywords'] = ','.join(doc['keywords_keyword'])
        record['source'] = doc['related_url_landing_page'][0]
        record['language'] = doc['ss_language']

        #Transform the indexed time as insert_data
        insert = dparser.parse(doc['timestamp'][0])
        record['insert_date'] = insert.isoformat()

        # Transform the last metadata update datetime as modified
        modified = dparser.parse(doc['last_metadata_update_datetime'][0])
        record['date_modified'] = modified.isoformat()

        # Transform temporal extendt start and end dates
        if 'temporal_extent_start_date' in doc:
            time_begin = dparser.parse(doc['temporal_extent_start_date'][0])
            record['time_begin'] = time_begin.isoformat()
        if 'temporal_extent_end_date' in doc:
            time_end = dparser.parse(doc['temporal_extent_end_date'][0])
            record['time_end'] = time_end.isoformat()



        #Transform the first investigator as creator.
        if 'personnel_investigator_name' in doc:
            record['creator'] =doc['personnel_investigator_name'][0] +" (" + doc['personnel_investigator_email'][0] + "), " + doc['personnel_investigator_organisation'][0]
        if 'use_constraint_identifier' in doc:
            record['rights'] = doc['use_constraint_identifier']


        xslt = os.environ.get('MMD_TO_ISO')

        transform = etree.XSLT(etree.parse(xslt))
        xml_ = base64.b64decode(doc['mmd_xml_file'])

        doc_ = etree.fromstring(xml_, self.context.parser)
        result_tree = transform(doc_).getroot()
        record['xml'] = etree.tostring(result_tree)
        record['mmd_xml_file'] = doc['mmd_xml_file']

        params = {
            #'fq': doc['metadata_identifier'],
            'q.op': 'OR',
            'q': 'metadata_identifier:(%s)' % doc['metadata_identifier']
        }

        mdsource_url = self.solr_select_url + urlencode(params)
        record['mdsource'] = mdsource_url

        return self.dataset(record)
