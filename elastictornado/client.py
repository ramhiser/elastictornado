from operator import itemgetter
from six import string_types, iteritems
from six.moves.urllib.parse import urlparse, urlencode
import certifi
import json

from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from tornado import gen
from tornado.httputil import url_concat

from pyelasticsearch.exceptions import BulkError
from pyelasticsearch.client import JsonEncoder

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

        path = join_path(path_components)

        request_url = 'https://' if self.use_ssl else 'http://'
        request_url += self.host
        if self.port:
            request_url += ':' + self.port
        request_url = join_path([request_url, path])

        # As far as I can tell, pyelasticsearch uses both the `requests`
        # package as well as the `urllib`. Query parameters are appended to the
        # URL as params. The body is either a string or a dictionary.
        request_url = url_concat(request_url, query_params)
        body = urlencode(body)

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

    def _encode_json(self, value):
        """
        Convert a Python value to a form suitable for ElasticSearch's JSON DSL.
        """
        # You can set this attribute on an instance to customize JSON encoding.
        # The stock JsonEncoder class maps Python datetimes to ES-style
        # datetimes and Python sets to ES lists. You can subclass it to add
        # more.
        return json.dumps(value, cls=JsonEncoder, use_decimal=True)

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

    @es_kwargs('consistency', 'refresh', 'replication', 'routing', 'timeout')
    def bulk(self, actions, index=None, doc_type=None, query_params=None):
        """
        Perform multiple index, delete, create, or update actions per request.

        Used with helper routines :meth:`index_op()`, :meth:`delete_op()`, and
        :meth:`update_op()`, this provides an efficient, readable way to do
        large-scale changes. This contrived example illustrates the structure::

            es.bulk([es.index_op({'title': 'All About Cats', 'pages': 20}),
                     es.index_op({'title': 'And Rats', 'pages': 47}),
                     es.index_op({'title': 'And Bats', 'pages': 23})],
                    doc_type='book',
                    index='library')

        More often, you'll want to index (or delete or update) a larger number
        of documents. In those cases, yield your documents from a generator,
        and use :func:`~pyelasticsearch.bulk_chunks()` to divide them into
        multiple requests::

            from pyelasticsearch import bulk_chunks

            def documents():
                for book in books:
                    yield es.index_op({'title': book.title,
                                       'pages': book.pages})
                    # index_op() also takes kwargs like index= and id= in case
                    # you want more control.
                    #
                    # You could also yield some delete_ops or update_ops here.

            # bulk_chunks() breaks your documents into smaller requests for
            # speed:
            for chunk in bulk_chunks(documents(),
                                     docs_per_chunk=500,
                                     bytes_per_chunk=10000):
                # We specify a default index and doc type here so we don't
                # have to repeat them in every operation:
                es.bulk(chunk, doc_type='book', index='library')

        :arg actions: An iterable of bulk actions, generally the output of
            :func:`~pyelasticsearch.bulk_chunks()` but sometimes a list
            of calls to :meth:`index_op()`, :meth:`delete_op()`, and
            :meth:`update_op()` directly. Specifically, an iterable of
            JSON-encoded bytestrings that can be joined with newlines and
            sent to ES.
        :arg index: Default index to operate on
        :arg doc_type: Default type of document to operate on. Cannot be
            specified without ``index``.

        Return the decoded JSON response on success.

        Raise :class:`~pyelasticsearch.exceptions.BulkError` if any of the
        individual actions fail. The exception provides enough about the
        failed actions to identify them for retrying.

        Sometimes there is an error with the request in general, not with
        any individual actions. If there is a connection error, timeout,
        or other transport error, a more general exception will be raised, as
        with other methods; see :ref:`error-handling`.

        See `ES's bulk API`_ for more detail.

        .. _`ES's bulk API`:
            http://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
        """
        # To summarize the flow: index_op() encodes a bytestring.
        #                        bulk_chunks() groups.
        #                        bulk() joins with \n.

        if doc_type is not None and index is None:
            raise ValueError(
                'Please also pass `index` if you pass `doc_type`.')

        def is_error(item):
            for op, subdict in iteritems(item):
                break
            return not 200 <= subdict.get('status', 999) < 300

        response = self.send_request('POST',
                                     [index, doc_type, '_bulk'],
                                     body='\n'.join(actions) + '\n',
                                     query_params=query_params)

        # Sometimes the request worked, but individual actions fail:
        if response.get('errors', True):
            errors, successes = [], []
            for item in response['items']:
                if is_error(item):
                    errors.append(item)
                else:
                    successes.append(item)
            if errors:
                raise BulkError(errors, successes)
        return response

    def index_op(self, doc, doc_type=None, overwrite_existing=True, **meta):
        """
        Return a document-indexing operation that can be passed to
        :meth:`bulk()`. (See there for examples.)

        Specifically, return a 2-line, JSON-encoded bytestring.

        :arg doc: A mapping of property names to values.
        :arg doc_type: The type of the document to index, if different from
            the one you pass to :meth:`bulk()`
        :arg overwrite_existing: Whether we should overwrite existing
            documents of the same ID and doc type. (If False, this does a
            `create` operation.)
        :arg meta: Other args controlling how the document is indexed,
            like ``id`` (most common), ``index`` (next most common),
            ``version``, and ``routing``. See `ES's bulk API`_ for details on
            these.

        .. _`ES's bulk API`:
            http://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html

        """
        operation = 'index' if overwrite_existing else 'create'
        return self._bulk_op(operation, doc=doc, meta=meta, doc_type=doc_type)

    def delete_op(self, doc_type=None, **meta):
        """
        Return a document-deleting operation that can be passed to
        :meth:`bulk()`. ::

            def actions():
                ...
                yield es.delete_op(id=7)
                yield es.delete_op(id=9,
                                   index='some-non-default-index',
                                   doc_type='some-non-default-type')
                ...

            es.bulk(actions(), ...)

        Specifically, return a JSON-encoded bytestring.

        :arg doc_type: The type of the document to delete, if different
            from the one passed to :meth:`bulk()`
        :arg meta: A description of what document to delete and how to do it.
            Example: ``{"index": "library", "id": 2, "version": 4}``.  See
            `ES's bulk API`_ for a list of all the options.

        .. _`ES's bulk API`:
            http://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html

        """
        return self._bulk_op('delete', meta=meta, doc_type=doc_type)

    def update_op(self, doc=None, doc_type=None, upsert=None,
                  doc_as_upsert=None, script=None, params=None, lang=None,
                  **meta):
        """
        Return a document-updating operation that can be passed to
        :meth:`bulk()`. ::

            def actions():
                ...
                yield es.update_op(doc={'pages': 4},
                                   id=7,
                                   version=21)
                ...

            es.bulk(actions(), ...)

        Specifically, return a JSON-encoded bytestring.

        :arg doc: A partial document to be merged into the existing document
        :arg doc_type: The type of the document to update, if different
            from the one passed to :meth:`bulk()`
        :arg upsert: The content for the new document created if the
            document does not exist
        :arg script: The script to be used to update the document
        :arg params: A dict of the params to be put in scope of the script
        :arg lang: The language of the script. Omit to use the default,
            specified by ``script.default_lang``.
        :arg meta: Other args controlling what document to update and how
            to do it, like ``id``, ``index``, and ``retry_on_conflict``,
            destined for the action line itself rather than the payload.  See
            `ES's bulk API`_ for details on these.

        .. _`ES's bulk API`:
            http://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html

        """
        payload = dict((k, v) for k, v in [('doc', doc), ('upsert', upsert),
                                           ('doc_as_upsert', doc_as_upsert),
                                           ('script', script),
                                           ('params', params),
                                           ('lang', lang)] if v is not None)
        return self._bulk_op('update',
                             doc=payload,
                             meta=meta,
                             doc_type=doc_type)

    def _bulk_op(self, operation, doc=None, meta=None, doc_type=None):
        """
        Return an arbitrary bulk indexing operation as a bytestring.

        :arg operation: One of 'index', 'delete', 'update', or 'create'
        :arg doc: A mapping of fields
        :arg meta: A mapping of underscore-prefixed fields with special
            meaning to ES, like ``_id`` and ``_type``
        :arg doc_type: The value that is to become the ``_type`` field of
            the action line. We go to special trouble to keep the name
            "doc_type" for consistency with other routines.
        """
        def underscore_keys(d):
            """Return a dict with every key prefixed by an underscore."""
            return dict(('_%s' % k, v) for k, v in iteritems(d))

        if meta is None:
            meta = {}
        if doc_type is not None:
            meta['type'] = doc_type

        ret = self._encode_json({operation: underscore_keys(meta)})
        if doc is not None:
            ret += '\n' + self._encode_json(doc)
        return ret

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

    @es_kwargs('q', 'df', 'analyzer', 'default_operator', 'source' 'routing',
               'replication', 'consistency')
    def delete_by_query(self, index, doc_type, query, query_params=None):
        """
        Delete typed JSON documents from a specific index based on query.

        :arg index: An index or iterable thereof from which to delete
        :arg doc_type: The type of document or iterable thereof to delete
        :arg query: A dictionary that will convert to ES's query DSL or a
            string that will serve as a textual query to be passed as the ``q``
            query string parameter. (Passing the ``q`` kwarg yourself is
            deprecated.)

        See `ES's delete-by-query API`_ for more detail.

        .. _`ES's delete-by-query API`:
            http://www.elastic.co/guide/en/elasticsearch/reference/current/docs-delete-by-query.html
        """
        if isinstance(query, string_types) and 'q' not in query_params:
            query_params['q'] = query
            body = ''
        else:
            body = {'query': query}
        return self.send_request('DELETE',
                                 [concat(index), concat(doc_type), '_query'],
                                 body,
                                 query_params=query_params)

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

    @es_kwargs()
    def multi_get(self, ids, index=None, doc_type=None, fields=None,
                  query_params=None):
        """
        Get multiple typed JSON documents from ES.

        :arg ids: An iterable, each element of which can be either an a dict or
            an id (int or string). IDs are taken to be document IDs. Dicts are
            passed through the Multi Get API essentially verbatim, except that
            any missing ``_type``, ``_index``, or ``fields`` keys are filled in
            from the defaults given in the ``doc_type``, ``index``, and
            ``fields`` args.
        :arg index: Default index name from which to retrieve
        :arg doc_type: Default type of document to get
        :arg fields: Default fields to return

        See `ES's Multi Get API`_ for more detail.

        .. _`ES's Multi Get API`:
            http://www.elastic.co/guide/en/elasticsearch/reference/current/docs-multi-get.html
        """
        doc_template = dict(
            filter(
                itemgetter(1),
                [('_index', index), ('_type', doc_type), ('fields', fields)]))

        docs = []
        for id in ids:
            doc = doc_template.copy()
            if isinstance(id, dict):
                doc.update(id)
            else:
                doc['_id'] = id
                docs.append(doc)

                return self.send_request(
                    'GET', ['_mget'], body={'docs': docs},
                    query_params=query_params)

    @es_kwargs('routing', 'parent', 'timeout', 'replication', 'consistency',
               'percolate', 'refresh', 'retry_on_conflict', 'fields')
    def update(self, index, doc_type, id, script=None, params=None, lang=None,
               query_params=None, doc=None, upsert=None, doc_as_upsert=None):
        """
        Update an existing document. Raise ``TypeError`` if ``script``, ``doc``
        and ``upsert`` are all unspecified.

        :arg index: The name of the index containing the document
        :arg doc_type: The type of the document
        :arg id: The ID of the document
        :arg script: The script to be used to update the document
        :arg params: A dict of the params to be put in scope of the script
        :arg lang: The language of the script. Omit to use the default,
            specified by ``script.default_lang``.
        :arg doc: A partial document to be merged into the existing document
        :arg upsert: The content for the new document created if the document
            does not exist
        :arg doc_as_upsert: The provided document will be inserted if the
            document does not already exist

        See `ES's Update API`_ for more detail.

        .. _`ES's Update API`:
            http://www.elastic.co/guide/en/elasticsearch/reference/current/docs-update.html
        """
        if script is None and doc is None and upsert is None:
            raise TypeError('At least one of the script, doc, or upsert '
                            'kwargs must be provided.')

        body = {}
        if script:
            body['script'] = script
        if lang and script:
            body['lang'] = lang
        if doc:
            body['doc'] = doc
        if upsert:
            body['upsert'] = upsert
        if params:
            body['params'] = params
        if doc_as_upsert:
            body['doc_as_upsert'] = doc_as_upsert
            return self.send_request(
                'POST',
                [index, doc_type, id, '_update'],
                body=body,
                query_params=query_params)

    def _search_or_count(self, kind, query, index=None, doc_type=None,
                         query_params=None):
        if isinstance(query, string_types):
            query_params['q'] = query
            body = ''
        else:
            body = query

        return self.send_request('GET',
                                 [concat(index), concat(doc_type), kind],
                                 body,
                                 query_params=query_params)

    @es_kwargs('routing', 'size')
    def search(self, query, **kwargs):
        """
        Execute a search query against one or more indices and get back search
        hits.

        :arg query: A dictionary that will convert to ES's query DSL or a
            string that will serve as a textual query to be passed as the ``q``
            query string parameter
        :arg index: An index or iterable of indexes to search. Omit to search
            all.
        :arg doc_type: A document type or iterable thereof to search. Omit to
            search all.
        :arg size: Limit the number of results to ``size``. Use with
            ``es_from`` to implement paginated searching.

        See `ES's search API`_ for more detail.

        .. _`ES's search API`:
            http://www.elastic.co/guide/en/elasticsearch/reference/current/_the_search_api.html
        """
        return self._search_or_count('_search', query, **kwargs)

    @es_kwargs('df', 'analyzer', 'default_operator', 'source', 'routing')
    def count(self, query, **kwargs):
        """
        Execute a query against one or more indices and get hit count.

        :arg query: A dictionary that will convert to ES's query DSL or a
            string that will serve as a textual query to be passed as the ``q``
            query string parameter
        :arg index: An index or iterable of indexes to search. Omit to search
            all.
        :arg doc_type: A document type or iterable thereof to search. Omit to
            search all.

        See `ES's count API`_ for more detail.

        .. _`ES's count API`:
            http://www.elastic.co/guide/en/elasticsearch/reference/current/search-count.html
        """
        return self._search_or_count('_count', query, **kwargs)

    @es_kwargs()
    def get_mapping(self, index=None, doc_type=None, query_params=None):
        """
        Fetch the mapping definition for a specific index and type.

        :arg index: An index or iterable thereof
        :arg doc_type: A document type or iterable thereof

        Omit both arguments to get mappings for all types and indexes.

        See `ES's get-mapping API`_ for more detail.

        .. _`ES's get-mapping API`:
            http://www.elastic.co/guide/en/elasticsearch/reference/current/indices-get-mapping.html
        """
        return self.send_request(
            'GET',
            [concat(index), concat(doc_type), '_mapping'],
            query_params=query_params)

    @es_kwargs('ignore_conflicts')
    def put_mapping(self, index, doc_type, mapping, query_params=None):
        """
        Register specific mapping definition for a specific type against one or
        more indices.

        :arg index: An index or iterable thereof
        :arg doc_type: The document type to set the mapping of
        :arg mapping: A dict representing the mapping to install. For example,
            this dict can have top-level keys that are the names of doc types.

        See `ES's put-mapping API`_ for more detail.

        .. _`ES's put-mapping API`:
            http://www.elastic.co/guide/en/elasticsearch/reference/current/indices-put-mapping.html
        """
        return self.send_request(
            'PUT',
            [concat(index), doc_type, '_mapping'],
            mapping,
            query_params=query_params)

    @es_kwargs('search_type', 'search_indices', 'search_types',
               'search_scroll', 'search_size', 'search_from',
               'like_text', 'percent_terms_to_match', 'min_term_freq',
               'max_query_terms', 'stop_words', 'min_doc_freq', 'max_doc_freq',
               'min_word_len', 'max_word_len', 'boost_terms', 'boost',
               'analyzer')
    def more_like_this(self, index, doc_type, id, mlt_fields, body='',
                       query_params=None):
        """
        Execute a "more like this" search query against one or more fields and
        get back search hits.

        :arg index: The index to search and where the document for comparison
            lives
        :arg doc_type: The type of document to find others like
        :arg id: The ID of the document to find others like
        :arg mlt_fields: The list of fields to compare on
        :arg body: A dictionary that will convert to ES's query DSL and be
            passed as the request body

        See `ES's more-like-this API`_ for more detail.

        .. _`ES's more-like-this API`:
            http://www.elastic.co/guide/en/elasticsearch/reference/current/search-more-like-this.html
        """
        query_params['mlt_fields'] = concat(mlt_fields)
        return self.send_request('GET',
                                 [index, doc_type, id, '_mlt'],
                                 body=body,
                                 query_params=query_params)

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

    @es_kwargs()
    def update_aliases(self, actions, query_params=None):
        """
        Atomically add, remove, or update aliases in bulk.

        :arg actions: A list of the actions to perform

        See `ES's indices-aliases API`_.

        .. _`ES's indices-aliases API`:
            http://www.elastic.co/guide/en/elasticsearch/reference/current/indices-aliases.html
        """
        return self.send_request('POST', ['_aliases'],
                                 body={'actions': actions},
                                 query_params=query_params)

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

    @es_kwargs()
    def create_index(self, index, settings=None, query_params=None):
        """
        Create an index with optional settings.

        :arg index: The name of the index to create
        :arg settings: A dictionary of settings

        If the index already exists, raise
        :class:`~pyelasticsearch.exceptions.IndexAlreadyExistsError`.

        See `ES's create-index API`_ for more detail.

        .. _`ES's create-index API`:
            http://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html
        """
        return self.send_request('PUT', [index], body=settings or {},
                                 query_params=query_params)

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

    @es_kwargs()
    def update_settings(self, index, settings, query_params=None):
        """
        Change the settings of one or more indexes.

        :arg index: An index or iterable of indexes
        :arg settings: A dictionary of settings

        See `ES's update-settings API`_ for more detail.

        .. _`ES's update-settings API`:
            http://www.elastic.co/guide/en/elasticsearch/reference/current/indices-update-settings.html
        """
        if not index:
            raise ValueError('No indexes specified. To update all indexes, use'
                             ' update_all_settings().')
        # If we implement the "update cluster settings" API, call that
        # update_cluster_settings().
        return self.send_request('PUT',
                                 [concat(index), '_settings'],
                                 body=settings,
                                 query_params=query_params)

    @es_kwargs()
    def update_all_settings(self, settings, query_params=None):
        """
        Update the settings of all indexes.

        :arg settings: A dictionary of settings

        See `ES's update-settings API`_ for more detail.

        .. _`ES's update-settings API`:
            http://www.elastic.co/guide/en/elasticsearch/reference/current/indices-update-settings.html
        """
        return self.send_request('PUT', ['_settings'], body=settings,
                                 query_params=query_params)

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

    @es_kwargs('routing', 'preference', 'ignore_unavailable',
               'percolate_format')
    def percolate(self, index, doc_type, doc, query_params=None):
        """
        Run a JSON document through the registered percolator queries, and
        return which ones match.

        :arg index: The name of the index to which the document pretends to
            belong
        :arg doc_type: The type the document should be treated as if it has
        :arg doc: A Python mapping object, convertible to JSON, representing
            the document

        Use :meth:`index()` to register percolators. See `ES's percolate API`_
        for more detail.

        .. _`ES's percolate API`:
            http://www.elastic.co/guide/en/elasticsearch/reference/current/search-percolate.html#_percolate_api
        """
        return self.send_request('GET',
                                 [index, doc_type, '_percolate'],
                                 doc,
                                 query_params=query_params)
