# elastictornado

This Python package provides an asynchronous ElasticSearch client via the
[Tornado framework](http://www.tornadoweb.org/). Our implementation closely follows that
of  [pyelasticsearch](https://github.com/pyelasticsearch/pyelasticsearch).

Asynchronous request support has been added for methods which require the use of
asynchronous libraries (like Tornado's `AsyncHTTPClient` library.)

## Example Usage

Connection to ElasticSearch database:

```python
from elastictornado import ElasticTornado
et = ElasticTornado('http://localhost:9200/')
```

Index a document:

```python
doc = {'name': 'William Adama', 'title': 'Admiral', 'nickname': 'Husker',
       'colony': 'Caprica', 'is_cylon': False}
et.index('contacts',
         'person',
         doc,
         id=1)
```

Index multiple documents using the bulk-indexing API:

```python
docs = [{'name': 'Kara Thrace', 'title': 'Captain', 'nickname': 'Starbuck',
         'colony': 'Caprica', 'is_cylon': False, 'id': 2},
        {'name': 'Sharon Valerii', 'title': 'Lieutenant', 'nickname': 'Boomer',
         'colony': 'Troy', 'is_cylon': True, 'id': 3}]

et.bulk((et.index_op(doc, id=doc.pop('id')) for doc in docs),
        index='contacts',
        doc_type='person')
```

Refresh the index:

```python
et.refresh('contacts')
# {u'ok': True, u'_shards': {u'successful': 5, u'failed': 0, u'total': 10}}
```

Get Starbuck's document:

```python
et.get('contacts', 'person', 2)
```

Perform a simple search:

```python
et.search('nickname:Starbuck OR nickname:Boomer', index='contacts')
```

Perform a search using the [ElasticSearch DSL](http://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html):

```python
query = {
    'query': {
        'filtered': {
            'query': {
                'query_string': {'query': 'colony:Caprica'}
            },
        },
    },
}
et.search(query, index='contacts')
```

Delete the index:

```python
et.delete_index('contacts')
```


## Installation

You can install the stable version from PyPI via:

```
pip install elastictornado
```

Alternatively, you can download the source from the
[GitHub repository](https://github.com/ramhiser/elastictornado) and manually
install it like so:

```
git clone git@github.com:ramhiser/elastictornado.git
cd elastictornado
python setup.py install
```

## License

The `elastictornado` package is licensed under the
[MIT License](http://opensource.org/licenses/MIT). Please consult the licensing
terms for more details.
