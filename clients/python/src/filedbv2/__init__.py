"""filedbv2 — official Python client for FileDB v2.

The public surface is the :class:`FileDB` client class::

    from filedbv2 import FileDB

    db = FileDB("localhost", 5433, "dev-key")
"""

from .client import FileDB

__all__ = ["FileDB"]
__version__ = "0.1.0"
