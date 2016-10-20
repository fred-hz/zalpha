from functools import wraps


def assert_class(obj, cls):
    def _assert(func):
        @wraps(func)
        def _decorator(*args, **kwargs):
            if not isinstance(obj, cls):
                raise ValueError('data should be in class {class_name}, but is {data_class} instead'.format(
                    class_name=cls.__name__, data_class=type(obj)
                ))
            func(args, kwargs)
        return _decorator
    return _assert
