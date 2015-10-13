from six.moves.urllib.parse import urlparse, urlencode
import certifi

from pyelasticsearch.client import JsonEncoder
from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from tornado import gen

from elastictornado.utils import join_path, es_kwargs


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

    def delete(self):
        pass

    def delete_all(self):
        pass

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

    def status(self):
        pass

    def update_aliases(self):
        pass

    def get_aliases(self):
        pass

    def aliases(self):
        pass

    def create_index(self):
        pass

    def delete_index(self):
        pass

    def delete_all_indexes(self):
        pass

    def close_index(self):
        pass

    def open_index(self):
        pass

    def get_settings(self):
        pass

    def update_settings(self):
        pass

    def update_all_settings(self):
        pass

    def flush(self):
        pass

    def refresh(self):
        pass

    def gateway_snapshot(self):
        pass

    def optimize(self):
        pass

    def health(self):
        pass

    def cluster_state(self):
        pass

    def percolate(self):
        pass
