from six.moves.urllib.parse import quote_plus as qp
from six import binary_type, text_type


def utf8(thing):
    """Convert any arbitrary ``thing`` to a utf-8 bytestring.

    Stolen from pyelasticsearch.
    TODO: Don't plagiarize.
    """
    if isinstance(thing, binary_type):
        return thing
    if not isinstance(thing, text_type):
        thing = text_type(thing)
    return thing.encode('utf-8')


def join_path(path_components):
    """
    Smush together the path components, omitting '' and None ones.
    Unicodes get encoded to strings via utf-8. Incoming strings are assumed
    to be utf-8-encoded already.

    Stolen from pyelasticsearch.
    TODO: Don't plagiarize.
    """
    comp = path_components
    path = '/'.join(qp(utf8(p), '') for p in comp if p is not None and p != '')

    if not path.startswith('/'):
        path = '/' + path
    return path
