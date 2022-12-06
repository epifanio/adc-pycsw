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
from datetime import datetime, timezone
import dateutil.parser as dparser
import logging
from urllib.parse import urlencode

import requests

from pycsw.core import util
from pycsw.core.etree import etree
import os

LOGGER = logging.getLogger(__name__)

from pycsw.plugins.repository.solr_helper import get_bbox

class SOLRMETNORepository:
    """
    Class to interact with underlying METNO SOLR backend repository
    """
    def __init__(self, context, repo_filter=None):
        """
        Initialize repository
        """

        self.context = context
        self.filter = repo_filter
        self.fts = False
        self.label = 'MetNO/SOLR'
        self.local_ingest = True
        self.solr_select_url = '%s/select' % self.filter
        self.dbtype = 'SOLR'

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

    def dataset(self, record):
        """
        Stub to mock a pycsw dataset object for Transactions
        """
        return type('', (object,), record)

    def query_ids(self, ids):
        """
        Query by list of identifiers
        """

        results = []

        params = {
            'fq': 'metadata_identifier:("%s")' % '" OR "'.join(ids),
            'q.op': 'OR',
            'q': '*:*'
        }

        response = requests.get(self.solr_select_url, params=params)

        response = response.json()

        for doc in response['response']['docs']:
            results.append(self._doc2record(doc))
        print("query by ID \n")
        return results


    def query_domain(self, domain, typenames, domainquerytype='list', count=False):
        """
        Query by property domain values
        """

        results = []

        params = {
            'q': '*:*',
            'rows': 0,
            'facet': 'true',
            'facet.query': 'distinct',
            'facet.type': 'terms',
            'facet.field': domain
        }

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

        if direction == 'min':
            sort_order = 'asc'
        else:
            sort_order = 'desc'

        params = {
            'q': '*:*',
            'q.op': 'OR',
            'fl': 'timestamp',
            'sort': 'timestamp %s' % sort_order
        }

        response = requests.get('%s/select' % self.filter, params=params).json()

        timestamp = datetime.strptime(response['response']['docs'][0]['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ')

        return timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')

    def query_source(self, source):
        """
        Query by source
        """

        return NotImplementedError()

    def query(self, constraint, sortby=None, typenames=None, maxrecords=10, startposition=0):
        """
        Query records from underlying repository
        """

        print('###################################################',
              '\n',
              constraint)
        print(dir(constraint), type(constraint))
        results = []


        # Default search params
        params = {
            'q': '*:*',
            'q.op': 'OR',
            'start': startposition,
            'rows': maxrecords,
        }

        print(len(constraint))
        #Only add query constraint if we have some, else return all records
        if len(constraint) != 0:

            #Do/check for  spatial search
            envelope = get_bbox(constraint)
            if envelope != False:
                solr_bbox_query = "{!field f=bbox score=overlapRatio}"+f"Within({envelope})"
                params['fq'] = solr_bbox_query
            # if constraint is none, return all the recordsogc:PropertyName'
            # otherwise catch the filter syntax and translate it
            #
            print('current constraint\n')
            print(constraint["_dict"]["ogc:Filter"])

            #Do/check for  text search
            qstring = "*:*"
            if "ogc:PropertyIsLike" in constraint["_dict"]["ogc:Filter"]:
                qstring = constraint["_dict"]["ogc:Filter"]["ogc:PropertyIsLike"]["ogc:Literal"]
                params["q"] = "full_text:"+qstring
                print(qstring)
            if "ogc:And" in constraint["_dict"]["ogc:Filter"]:
                if "csw:AnyText" in constraint["_dict"]["ogc:Filter"]["ogc:And"]["ogc:PropertyIsLike"]["ogc:PropertyName"]:
                    qstring = constraint["_dict"]["ogc:Filter"]["ogc:And"]["ogc:PropertyIsLike"]["ogc:Literal"]
                    params["q"] = "full_text:"+qstring
                    print(qstring)

        #Solr query

        print(params)
        response = requests.get('%s/select' % self.filter, params=params).json()
        print(response)

        total = response['response']['numFound']
        # response = response.json()

        for doc in response['response']['docs']:
            results.append(self._doc2record(doc))

        print(total)

        # TODO
        # transform constraint['_dict'] into SOLR query syntax
        #  - set paging from maxrecords and startposition
        # transform each doc result into pycsw dataset object
        # return the total hits (int, and list of dataset objects)

        #DEBUG
        if "_dict" in constraint:
            print("constraint: ", constraint['_dict'])
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
            'q': 'metadata_identifier:'+doc['metadata_identifier']
        }

        mdsource_url = self.solr_select_url + urlencode(params)
        record['mdsource'] = mdsource_url

        return self.dataset(record)
