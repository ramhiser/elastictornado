from six.moves.urllib.parse import quote_plus as qp
from six import binary_type, text_type, iterkeys
from functools import wraps


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


def es_kwargs(*args_to_convert):
        """
        Mark which kwargs will become query string params in the eventual ES
        call. Return a decorator that grabs the kwargs of the given names, plus
        any beginning with "es_", subtracts them from the ordinary kwargs, and
        passes them to the decorated function through the ``query_params``
        kwarg. The remaining kwargs and the args are passed through unscathed.
        Also, if any of the given kwargs are undocumented in the decorated
        method's docstring, add stub documentation for them.

        TODO: Taken from pyelasticsearch. Update?
        """
        convertible_args = set(args_to_convert)

        # TODO: Similar to pyelasticsearch, need to add docs for query params
        def decorator(func):
            @wraps(func)
            def decorate(*args, **kwargs):
                # Make kwargs the map of normal kwargs and query_params the map
                # of kwargs destined for query string params.

                # Let one @es_kwargs-wrapped function call another:
                query_params = kwargs.pop('query_params', {})

                # Make a copy; we mutate kwargs.
                for k in list(iterkeys(kwargs)):
                    if k.startswith('es_'):
                        query_params[k[3:]] = kwargs.pop(k)
                    elif k in convertible_args:
                        query_params[k] = kwargs.pop(k)
                        return func(*args, query_params=query_params, **kwargs)
                return decorate
            return decorator
