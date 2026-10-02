"""Django middleware entry point.

Referenced from Django settings as::

    MIDDLEWARE = [
        ...
        "dataflow.django_middleware.DataflowMiddleware",
    ]

The class itself lives in :mod:`dataflow.contrib` and is duck-typed — this
module imports cleanly without Django installed.
"""

from .contrib import DataflowMiddleware

__all__ = ["DataflowMiddleware"]
