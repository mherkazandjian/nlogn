import pprint
import time
import logging
import json
import requests
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch.exceptions import ConnectionTimeout
import numpy
import pandas as pd

from nlogn.loggers.logger import log


class Mapping:
    """
    Wrapper around the elasticsearch type mapping
    """
    ELASTIC2PANDAS_TYPES = {
        'keyword': numpy.str,
        'text': numpy.str,
        'float': numpy.float64,
        'long': numpy.int64,
        'date': numpy.datetime64
    }

    def __init__(self, mapping):
        """
        Constructor
        """
        self.mapping = mapping

    def to_numpy(self):
        """
        Return the mapping as numpy compatible mapping

        :return: dict
        """
        retval = {
            field: self.ELASTIC2PANDAS_TYPES[field_el_type['type']]
            for field, field_el_type in self.mapping.items()
        }
        return retval


class DatabaseBase:
    """
    Base class for interacting with elasticsearch
    """
    def __init__(self,
                 url=None,
                 host=None,
                 port=None,
                 verbose=True,
                 *args, **kwargs):
        """
        Constructor
        """

        self.url = url
        """The url of the database in the form {host}:port"""

        self.host = host
        """The host of the elasticsearch database"""

        self.port = port
        """The port of the elasticsearch database"""

        self.db = None
        """The instance of the elasticsearch object"""

        self.server_info = None
        """The version and compatibility strings of the server"""

        self.verbose = verbose

        if url is not None:
            self.host, self.port = self.url.split(':')

        if self.host is not None and self.port is not None:
            self.db, self.server_info = self.connect()

    def connect(self):
        """
        Connect to the elasticsearch database
        """
        es_logger = logging.getLogger('elasticsearch')
        es_logger.setLevel(logging.CRITICAL)
        logging.getLogger("urllib3").setLevel(logging.WARNING)

        # check if the server is running
        res = requests.get(f'http://{self.host}:{self.port}')

        server_info = json.loads(res.content.decode())
        if self.verbose:
            for line in pprint.pformat(server_info).splitlines():
                log.info(line)

        # create an instance of the elasticsearch connector object
        db = Elasticsearch([{'host': self.host, 'port': self.port}])

        return db, server_info

    def version(self):
        """
        Get the version components

        :return: string tuple
        """
        major, minor, patch = self.server_info['version']['number'].split('.')
        return major, minor, patch

    def indices(self):
        """
        Return a list of the indices
        """
        return list(self.db.indices.get('*').keys())

    def _query(self,
               query_body=None,
               index=None,
               verbose=False):
        """
        Perform the elasticsearch query and return the data as a dataframe

        :param query_body: The body of the elasticsearch query
        :param index: the name of the index
        :param verbose: if True the elasticsearch scrolling is done verbosely
        :return: the metadata as a dict and the matches as a dict
        """
        data = {}
        metadata = {
            '_index': [],
            '_type': [],
            '_id': [],
            '_score': []
        }

        page = self.db.search(
            index=index,
            body=query_body,
            scroll='1m', size=1000
        )

        sid = page['_scroll_id']
        scroll_size = len(page['hits']['hits'])

        def collect_hits_from_page(_page, collect_to=None):
            for hit in _page['hits']['hits']:
                source = hit['_source']
                # collect the data
                for key, value in source.items():
                    collect_to[key].append(value)
                # collect the metadata
                for key in metadata.keys():
                    metadata[key].append(hit[key])

        # get the names of the fields of the hits and create the field empty
        # lists in the dict where the data will be collected to
        if len(page['hits']['hits']) > 0:
            fields = list(page['hits']['hits'][0]['_source'].keys())
            for field in fields:
                data[field] = []

        collect_hits_from_page(page, collect_to=data)
        while scroll_size > 0:
            if verbose:
                log.info("Scrolling...")

            page = self.db.scroll(scroll_id=sid, scroll='1m')
            sid = page['_scroll_id']
            scroll_size = len(page['hits']['hits'])

            if verbose:
                log.info("scroll size: " + str(scroll_size))
            collect_hits_from_page(page, collect_to=data)

        return metadata, data

    def _query_by_date_range(self, date_range, index=None, **kwargs):
        """
        Query an index by filtering over a date-range

        :param date_range: a two element iterable with the format
         ["2019-04-22T07:00", "2019-04-29T08:00"]
        :param index: the name of the index to be queried e.g 'my_index'
        :param kwargs: (optional) doc_type: the type of the document
        :return: the metadata as a dict and the matches as a dict
        """
        body = {
            'query': {
                'bool': {
                    'must': [
                        # add specific queries here
                    ],
                }
            },
        }

        body['query']['bool']['must'].append(
            {
                'range': {
                    'timestamp': {
                        "gte": date_range[0],
                        "lt": date_range[1],
                    }
                }
            }
        )

        if 'doc_type' in kwargs and kwargs['doc_type'] is not None:
            body['query']['bool']['must'].append(
                {
                    "term": {
                        "_type": kwargs['doc_type']
                    }
                },
            )

        return self._query(query_body=body, index=index)

    def _query_most_recent(self, index=None, **kwargs):
        """
        Return as a dataframe the most recent item from the specified index

        :param index: the index to be quiried
        :param kwargs: (optional) doc_type: the type of the doc to be matched
        :return: the record metadata and the record data
        """
        body = {
            "query": {
                'bool': {
                    'must': [
                        {
                            "match_all": {},
                        }
                    ],
                },
            },
            "size": 1,
            "sort": [
                {
                    "timestamp": {
                        "order": "desc"
                    }
                }
            ]
        }

        if 'doc_type' in kwargs and kwargs['doc_type'] is not None:
            body['query']['bool']['must'].append(
                {
                    "term": {
                        "_type": kwargs['doc_type']
                    }
                },
            )

        hit = self.db.search(
            index=index,
            body=body,
            size=1
        )
        assert len(hit['hits']['hits']) == 1
        record = hit['hits']['hits'][0]['_source']
        record_metadata = {
            key: hit['hits']['hits'][0][key]
            for key in ['_index', '_type', '_id', '_score']
        }
        return record_metadata, record

    def query(self,
              index=None,
              doc_type=None,
              as_df=False,
              df_index='timestamp',
              df_sort_index=True,
              df_index_to_datetime=True,
              keep_columns=None,
              by=None,
              **kwargs):
        """
        Top level query function that can be used to query an index by different
        filters

        :param index: the name of the index to be queries
        :param doc_type: the type of the document to be queried
        :param as_df: return the data as a pandas dataframe
        :param df_index: the field to be used as a dataframe index, 'timestamp'
         by default
        :param df_sort_index: if True, the returned df is sorted by its index
        :param df_index_to_datetime: if True, the index is converted to a
         datetime obj
        :param keep_columns: The columns to be used in the dataframe, these
         must be a subset of the mapping of the elasticsearch index.
        :param by: a string specifying the type of the query, supported values
         are: 'date_range', 'most_recent'
        :param kwargs: keywords specific to the type of the query
        :return: tuple.
          - the mapping object
          - the metadata
          - the data: if as_df is set, this item is a pandas dataframe otherwise
            it is a dictionary that contains the data
        """
        mapping = self.get_mapping(index=index, doc_type=doc_type)

        if by == 'date_range':
            date_range = kwargs[by].split(',')
            metadata, data = self._query_by_date_range(
                date_range,
                index=index,
                doc_type=doc_type
            )
        elif by == 'most_recent':
            metadata, data = self._query_most_recent(
                index=index,
                doc_type=doc_type
            )
        else:
            raise ValueError(f'by={by} not supported')

        if as_df:

            if not data:
                # no data matched, return df as None
                df = pd.DataFrame(columns=mapping.mapping.keys())
                retval = mapping, metadata, df
            else:

                data_df = {}

                if keep_columns is None:
                    keep_columns = mapping.mapping.keys()

                # cast data to numpy arrays (change the values() of the dict)
                for field, field_type in mapping.to_numpy().items():
                    if field in keep_columns:
                        data_df[field] = numpy.array(data[field]).astype(field_type)

                # determine the column to be used as the index of the dataframe
                if df_index is None:
                    df = pd.DataFrame(data=data_df, index=None)
                else:
                    if df_index_to_datetime:
                        data_df[df_index] = pd.to_datetime(data_df[df_index])
                    df = pd.DataFrame(data=data_df)
                    df.drop_duplicates(inplace=True)
                    df.set_index(df_index, drop=True, inplace=True)
                    df.index.name = df_index

                    if df_sort_index:
                        df = df.sort_index()

                retval = mapping, metadata, df

        else:
            mapping = self.get_mapping(
                index=index, doc_type=doc_type, as_numpy=False
            )
            retval = mapping, metadata, data

        return retval

    def create_index(self, name=None, mapping=None, **kwargs):
        """
        Create an empty index that defines the mapping and the docment type

        By default a timestamp field is always define and the rest of the fields
        are passed as a mapping

        :param name: the name of the index e,g 'my_foo_index'
        :param mapping: the types of the fields
        :param kwargs: (optional) doc_type: the type of the content e.g 'doc'
        """
        raise NotImplementedError('must be implemented by subclass')

    def delete_index(self, name, verbose=True):
        """
        Delete an index by specifying its name if it exists

        :param name: the name of the index to be deleted
        :param verbose: do not raise an exception of the index does not exist
        """
        if name in self.indices():
            self.db.indices.delete(name)
            if verbose:
                log.info(f'index {name} deleted from {self.host}:{self.port}')
        else:
            if verbose:
                log.info(
                    f'index {name} does not exist in {self.host}:{self.port}'
                )
                log.info(f'\t\tskip attempt to delete index')

    def delete_by_query(self, index=None, date_range=None, **kwargs):
        """
        Delete items from an index based on a date range | doc_type or both

        .. code-block:: python

            # delete by date range only
            self.delete_by_query(
                index='my_foo_index',
                date_range='2018-05-04T23:00,2029-05-05T00:00'
            )

            # delete by document type only
            self.delete_by_query(
                index='my_foo_index',
                doc_type='my_doc_type',
            )

            # delete by date range and doc type
            self.delete_by_query(
                index='my_foo_index',
                doc_type='my_doc_type',
                date_range='2018-05-04T23:00,2029-05-05T00:00'
            )

        :param date_range: a list that specifies the start date (inclusive) and
         the end date (exclusive) of the matching query
        :param index: list of indices to be wiped. By defaila all self.INICES
         are used if this kwarg is not passed.
        :param kwargs: (optional) doc_type: the document type to be filterd by
        """
        start, end = date_range.split(',')
        body = {
            'query': {
                'bool': {
                    'must': [
                        # add queries here (see below)
                    ],
                }
            },
        }

        if date_range is not None:
            body['query']['bool']['must'].append(
                {
                    'range': {
                        'timestamp': {
                            "gte": start,
                            "lt": end,
                        }
                    }
                },
            )

        if 'doc_type' in kwargs and kwargs['doc_type'] is not None:
            body['query']['bool']['must'].append(
                {
                    "term": {
                        "_type": kwargs['doc_type']
                    }
                }
            )

        self.db.delete_by_query(index=index, body=body)

    def ingest_dataframe(self, df, index_name, **kwargs):
        """
        Ingest a dataframe in bulk

        :param df: The dataframe to be ingested
        :param index_name: the name of the index
        :param kwargs: (optional) the type of the document e.g 'doc'
        """
        raise NotImplementedError('should be implemented by subclass')

    def get_mapping(self, index=None, **kwargs):
        """
        Return the mapping of the fields for a certain document type

        :param index: the name of the index
        :param kwargs: (optional) doc_type: the type of the document
        :return: a dict of the the field names with their corresponding types
        """
        raise NotImplementedError('should be implemented by subclass')


class Database_v6(DatabaseBase):
    """
    Base class for interacting with elasticsearch
    """
    def create_index(self, name=None, doc_type=None, mapping=None):
        """
        Create an empty index for elasticsearch v6 (see self.create_index)
        """
        self.db.indices.create(
           index=name,
           body={
              'mappings': {
                 doc_type: {
                    "properties": mapping
                 }
              }
           }
        )

    def ingest_dataframe(self, df, index_name, doc_type=None):
        """
        Ingest a dataframe in bulk

        :param df: The dataframe to be ingested
        :param doc_type: the type of the document e.g 'doc'
        :param index_name: the name of the index
        """
        # ingest the dataframe in bulk
        records = [
            {
                "_index": index_name,
                "_type": doc_type,
                "_source": {
                    col_name: row[col_name] for col_name in df.columns
                }
            }
            for row_index, row in df.iterrows()
        ]

        while True:
            try:
                helpers.bulk(self.db, records)
                break
            except ConnectionTimeout:
                log.info(
                    f'connection timed out when ingesting data into {index_name}'
                )
                time.sleep(1)
                # .. todo:: add option to stop trying after N attempts

    def get_mapping(self, index=None, doc_type=None):
        """
        Return the mapping of the fields for a certain document type

        :param index: the name of the index
        :param doc_type: the type of the document
        :return: a dict of the the field names with their corresponding types
        """
        mapping = self.db.indices.get_mapping(index=index)

        doc_types = mapping[index]['mappings']
        if doc_type not in doc_types:
            msg = (
                f'doc_type {doc_type} is not in the mapping\n'
                f'the available document types are: {" ".join(doc_types)}'
            )
            raise KeyError(msg)
        _mapping = mapping[index]['mappings'][doc_type]['properties']
        return Mapping(_mapping)


class Database_v7(DatabaseBase):
    """
    Base class for interacting with elasticsearch
    """
    def create_index(self, name=None, mapping=None, **kwargs):
        """
        Create an empty index for elasticsearch v6 (see self.create_index)
        """
        self.db.indices.create(index=name)
        self.db.indices.put_mapping(
           index=name,
           body={
              'properties': mapping
           }
        )

    def ingest_dataframe(self, df, index_name, **kwargs):
        """
        Ingest a dataframe in bulk

        :param df: The dataframe to be ingested
        :param index_name: the name of the index
        """
        # ingest the dataframe in bulk
        records = [
            {
                "_index": index_name,
                "_source": {
                    col_name: row[col_name] for col_name in df.columns
                }
            }
            for row_index, row in df.iterrows()
        ]

        while True:
            try:
                helpers.bulk(self.db, records)
                break
            except ConnectionTimeout:
                log.info(
                    f'connection timed out when ingesting data into {index_name}'
                )
                time.sleep(1)
                # .. todo:: add option to stop trying after N attempts

    def get_mapping(self, index=None, **kwargs):
        """
        Return the mapping of the fields for a certain document type

        :param index: the name of the index
        :return: a dict of the the field names with their corresponding types
        """
        mapping = self.db.indices.get_mapping(index=index)
        _mapping = mapping[index]['mappings']['properties']
        return Mapping(_mapping)


class Database:
    """
    Factory class for the ElasticSearch implementations
    """
    IMPL = {
        '6': Database_v6,
        '7': Database_v7
    }

    def __new__(cls, *args, **kwargs):
        """
        Create an instance of the right version based on the info fetched
        from the url

        :param args:
        :param kwargs:
        :return: an instance of the elasticsearch class
        """
        db_tmp = DatabaseBase(verbose=False, *args, **kwargs)
        major_version, _, _ = db_tmp.version()
        db = Database.IMPL[major_version](*args, **kwargs)
        return db
