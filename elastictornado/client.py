from six.moves.urllib.parse import urlparse, urlencode
import certifi

from pyelasticsearch.client import JsonEncoder
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from tornado import gen

from elastictornado.utils import join_path, es_kwargs, concat


class ElasticTornado(object):
    def __init__(self,
                 url='http://localhost',
                 timeout=60,
                 max_retries=0,
                 port=9200,
                 username=None,
                 password=None,
                 ca_certs=certifi.where(),
                 client_cert=None):
        """
        :arg url: A URL of ES nodes, which can be a full URLs with port number,
            like ``http://elasticsearch.example.com:9200``, or you can pass the
            port separately using the ``port`` kwarg. To do HTTP basic
            authentication, you can use RFC-2617-style URLs like
            ``http://someuser:somepassword@example.com:9200`` or the separate
            ``username`` and ``password`` kwargs below.  Unlike
            ``pyelasticsearch``, we allow only one URL. This will likely change
            in a future version.
        :arg timeout: Number of seconds to wait for each request before raising
            Timeout
        :arg max_retries: How many other servers to try, in series, after a
            request times out or a connection fails
        :arg username: Authentication username to send via HTTP basic auth
        :arg password: Password to use in HTTP basic auth. If a username and
            password are embedded in a URL, those are favored.
        :arg port: The default port to connect on, for URLs that don't include
            an explicit port
        :arg ca_certs: A path to a bundle of CA certificates to trust. The
            default is to use Mozilla's bundle, the same one used by Firefox.
        :arg client_cert: A certificate to authenticate the client to the
            server
        """
        url = url.rstrip('/')
        parsed_url = urlparse(url)

        self.host = parsed_url.hostname
        self.port = parsed_url.port or port
        self.username = username or parsed_url.username
        self.password = password or parsed_url.password
        self.use_ssl = parsed_url.scheme == 'https'
        self.verify_certs = True
        self.ca_certs = ca_certs
        self.cert_file = client_cert
        self.max_retries = max_retries
        self.retry_on_timeout = True
        self.timeout = timeout

        self.http_client = AsyncHTTPClient()

    @gen.coroutine
    def send_request(self, method, path_components, body='',
                     query_params=None):
        if query_params is None:
            query_params = {}

        # TODO: Ignoring body provided. How to include body in addition to
        # query_params with HTTPRequest?
        body = urlencode(query_params)

        path = join_path(path_components)

        request_url = 'https://' if self.use_ssl else 'http://'
        request_url += self.host
        if self.port:
            request_url += ':' + self.port
        request_url = join_path([request_url, path])

        # TODO: There are a few member variables from pyelasticsearch not used.
        http_request = HTTPRequest(url=request_url,
                                   method=method,
                                   body=body,
                                   auth_username=self.username,
                                   auth_password=self.password,
                                   request_timeout=self.timeout,
                                   validate_cert=self.verify_certs,
                                   ca_certs=self.ca_certs)

        response = yield self.http_client.fetch(http_request)
        raise gen.Return(response)

    # REST API

    @es_kwargs('routing', 'parent', 'timestamp', 'ttl', 'percolate',
               'consistency', 'replication', 'refresh', 'timeout', 'fields')
    def index(self, index, doc_type, doc, id=None, overwrite_existing=True,
              query_params=None):
        if not overwrite_existing:
            query_params['op_type'] = 'create'

        return self.send_request('POST' if id is None else 'PUT',
                                 [index, doc_type, id],
                                 doc,
                                 query_params)

    def bulk(self):
        pass

    def delete_op(self):
        pass

    def update_op(self):
        pass

    def bulk_index(self):
        pass

    @es_kwargs('routing', 'parent', 'replication', 'consistency', 'refresh')
    def delete(self, index, doc_type, id, query_params=None):
        """
        Delete a typed JSON document from a specific index based on its ID.

        :arg index: The name of the index from which to delete
        :arg doc_type: The type of the document to delete
        :arg id: The (string or int) ID of the document to delete

        See `ES's delete API`_ for more detail.

        .. _`ES's delete API`:
            http://www.elastic.co/guide/en/elasticsearch/reference/current/docs-delete.html
        """
        # id should never be None, and it's not particular dangerous
        # (equivalent to deleting a doc with ID "None", but it's almost
        # certainly not what the caller meant:
        if id is None or id == '':
            raise ValueError('No ID specified. To delete all documents in '
                             'an index, use delete_all().')
        return self.send_request('DELETE', [index, doc_type, id],
                                 query_params=query_params)

    @es_kwargs('routing', 'parent', 'replication', 'consistency', 'refresh')
    def delete_all(self, index, doc_type, query_params=None):
        """
        Delete all documents of the given doc type from an index.

        :arg index: The name of the index from which to delete. ES does not
            support this being empty or "_all" or a comma-delimited list of
            index names (in 0.19.9).
        :arg doc_type: The name of a document type

        See `ES's delete API`_ for more detail.

        .. _`ES's delete API`:
            http://www.elastic.co/guide/en/elasticsearch/reference/current/docs-delete.html
        """
        return self.send_request('DELETE', [index, doc_type],
                                 query_params=query_params)

    def delete_by_query(self):
        pass

    @es_kwargs('realtime', 'fields', 'routing', 'preference', 'refresh')
    def get(self, index, doc_type, id, query_params=None):
        """
        Get a typed JSON document from an index by ID.

        :arg index: The name of the index from which to retrieve
        :arg doc_type: The type of document to get
        :arg id: The ID of the document to retrieve

        See `ES's get API`_ for more detail.

        .. _`ES's get API`:
            http://www.elastic.co/guide/en/elasticsearch/reference/current/docs-get.html
        """
        return self.send_request('GET', [index, doc_type, id],
                                 query_params=query_params)

    def multi_get(self):
        pass

    def update(self):
        pass

    def search(self):
        pass

    def count(self):
        pass

    def get_mapping(self):
        pass

    def put_mapping(self):
        pass

    def more_like_this(self):
        pass

    # Index Admin API

    @es_kwargs('recovery', 'snapshot')
    def status(self, index=None, query_params=None):
        """
        Retrieve the status of one or more indices

        :arg index: An index or iterable thereof

        See `ES's index-status API`_ for more detail.

        .. _`ES's index-status API`:
            http://www.elastic.co/guide/en/elasticsearch/reference/current/indices-status.html
        """
        return self.send_request('GET', [concat(index), '_status'],
                                 query_params=query_params)

    def update_aliases(self):
        pass

    @es_kwargs('ignore_unavailable')
    def get_aliases(self, index=None, alias='*', query_params=None):
        """
        Retrieve a listing of aliases

        :arg index: The name of an index or an iterable of indices from which
            to fetch aliases. If omitted, look in all indices.
        :arg alias: The name of the alias to return or an iterable of them.
            Wildcard * is supported. If this arg is omitted, return all
            aliases.

        See `ES's indices-aliases API`_.

        .. _`ES's indices-aliases API`:
            http://www.elastic.co/guide/en/elasticsearch/reference/current/indices-aliases.html
        """
        return self.send_request('GET',
                                 [concat(index), '_aliases', concat(alias)],
                                 query_params=query_params)

    def create_index(self):
        pass

    @es_kwargs()
    def delete_index(self, index, query_params=None):
        """
        Delete an index.

        :arg index: An index or iterable thereof to delete

        If the index is not found, raise
        :class:`~pyelasticsearch.exceptions.ElasticHttpNotFoundError`.

        See `ES's delete-index API`_ for more detail.

        .. _`ES's delete-index API`:
            http://www.elastic.co/guide/en/elasticsearch/reference/current/indices-delete-index.html
        """
        if not index:
            raise ValueError('No indexes specified. To delete all indexes, use'
                             ' delete_all_indexes().')
        return self.send_request('DELETE', [concat(index)],
                                 query_params=query_params)

    def delete_all_indexes(self, **kwargs):
        """Delete all indexes."""
        return self.delete_index('_all', **kwargs)

    @es_kwargs()
    def close_index(self, index, query_params=None):
        """
        Close an index.

        :arg index: The index to close

        See `ES's close-index API`_ for more detail.

        .. _`ES's close-index API`:
            http://www.elastic.co/guide/en/elasticsearch/reference/current/indices-open-close.html
        """
        return self.send_request('POST', [index, '_close'],
                                 query_params=query_params)

    @es_kwargs()
    def open_index(self, index, query_params=None):
        """
        Open an index.

        :arg index: The index to open

        See `ES's open-index API`_ for more detail.

        .. _`ES's open-index API`:
            http://www.elastic.co/guide/en/elasticsearch/reference/current/indices-open-close.html
        """
        return self.send_request('POST', [index, '_open'],
                                 query_params=query_params)

    @es_kwargs()
    def get_settings(self, index, query_params=None):
        """
        Get the settings of one or more indexes.

        :arg index: An index or iterable of indexes

        See `ES's get-settings API`_ for more detail.

        .. _`ES's get-settings API`:
            http://www.elastic.co/guide/en/elasticsearch/reference/current/indices-get-settings.html
        """
        return self.send_request('GET',
                                 [concat(index), '_settings'],
                                 query_params=query_params)

    def update_settings(self):
        pass

    def update_all_settings(self):
        pass

    @es_kwargs('refresh')
    def flush(self, index=None, query_params=None):
        """
        Flush one or more indices (clear memory).

        :arg index: An index or iterable of indexes

        See `ES's flush API`_ for more detail.

        .. _`ES's flush API`:
            http://www.elastic.co/guide/en/elasticsearch/reference/current/indices-flush.html
        """
        return self.send_request('POST',
                                 [concat(index), '_flush'],
                                 query_params=query_params)

    @es_kwargs()
    def refresh(self, index=None, query_params=None):
        """
        Refresh one or more indices.

        :arg index: An index or iterable of indexes

        See `ES's refresh API`_ for more detail.

        .. _`ES's refresh API`:
            http://www.elastic.co/guide/en/elasticsearch/reference/current/indices-refresh.html
        """
        return self.send_request('POST', [concat(index), '_refresh'],
                                 query_params=query_params)

    @es_kwargs()
    def gateway_snapshot(self, index=None, query_params=None):
        """
        Gateway snapshot one or more indices.

        :arg index: An index or iterable of indexes

        See `ES's gateway-snapshot API`_ for more detail.

        .. _`ES's gateway-snapshot API`:
            http://www.elasticsearch.org/guide/reference/api/admin-indices-gateway-snapshot.html
        """
        return self.send_request(
            'POST',
            [concat(index), '_gateway', 'snapshot'],
            query_params=query_params)

    @es_kwargs('max_num_segments', 'only_expunge_deletes', 'refresh', 'flush',
               'wait_for_merge')
    def optimize(self, index=None, query_params=None):
        """
        Optimize one or more indices.

        :arg index: An index or iterable of indexes

        See `ES's optimize API`_ for more detail.

        .. _`ES's optimize API`:
            http://www.elastic.co/guide/en/elasticsearch/reference/current/indices-optimize.html
        """
        return self.send_request('POST',
                                 [concat(index), '_optimize'],
                                 query_params=query_params)

    @es_kwargs('level', 'wait_for_status', 'wait_for_relocating_shards',
               'wait_for_nodes', 'timeout')
    def health(self, index=None, query_params=None):
        """
        Report on the health of the cluster or certain indices.

        :arg index: The index or iterable of indexes to examine

        See `ES's cluster-health API`_ for more detail.

        .. _`ES's cluster-health API`:
            http://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-health.html
        """
        return self.send_request(
            'GET',
            ['_cluster', 'health', concat(index)],
            query_params=query_params)

    @es_kwargs('local')
    def cluster_state(self, metric='_all', index='_all', query_params=None):
        """
        Return state information about the cluster.

        :arg metric: Which metric to return: one of "version", "master_node",
            "nodes", "routing_table", "meatadata", or "blocks", an iterable
            of them, or a comma-delimited string of them. Defaults to all
            metrics.
        :arg index: An index or iterable of indexes to return info about

        See `ES's cluster-state API`_ for more detail.

        .. _`ES's cluster-state API`:
            http://www.elastic.co/guide/en/elasticsearch/reference/current/cluster-state.html
        """
        return self.send_request(
            'GET',
            ['_cluster', 'state', concat(metric), concat(index)],
            query_params=query_params)

    def percolate(self):
        pass
