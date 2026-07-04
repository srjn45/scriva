"""filedbv2 — official Python client for FileDB v2.

The public surface is the :class:`FileDB` client class::

    from filedbv2 import FileDB

    db = FileDB("localhost", 5433, "dev-key")
"""

from .client import AlreadyExistsError, FileDB, FileDBError, NotFoundError

__all__ = ["FileDB", "FileDBError", "NotFoundError", "AlreadyExistsError"]
__version__ = "0.7.0"
