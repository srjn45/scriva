"""scriva — official Python client for ScrivaDB.

The public surface is the :class:`ScrivaDB` client class::

    from scriva import ScrivaDB

    db = ScrivaDB("localhost", 5433, "dev-key")
"""

from .client import AlreadyExistsError, ScrivaDB, ScrivaDBError, NotFoundError

__all__ = ["ScrivaDB", "ScrivaDBError", "NotFoundError", "AlreadyExistsError"]
__version__ = "1.2.0"
