"""scriva — official Python client for ScrivaDB.

The public surface is the :class:`ScrivaDB` client class::

    from scriva import ScrivaDB

    db = ScrivaDB("localhost", 5433, "dev-key")
"""

from .client import AlreadyExistsError, ScrivaDB, ScrivaDBError, NotFoundError, XTxError

__all__ = ["ScrivaDB", "ScrivaDBError", "NotFoundError", "AlreadyExistsError", "XTxError"]
__version__ = "1.2.1"
