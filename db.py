import functools
import time

from google.cloud.datastore import Client

__all__ = ("client", "transaction")

client = Client()


def transaction(read_only: bool = False, retry: bool = False, retry_timeout: float = 0.005):
    def wrap_wrap(function):
        @functools.wraps(function)
        def wrap(*args, **kwargs):
            while True:
                try:
                    with client.transaction(read_only=read_only) as batch:
                        result = function(*args, **kwargs, batch=batch)
                except Exception:  # TODO narrow clause
                    if retry:
                        time.sleep(retry_timeout)
                        continue
                    else:
                        return result
                else:
                    return result

        return wrap

    return wrap_wrap
