"""Unified convenience package bringing duckdb and polars together.

This package does not (yet) vendor the underlying wheels; it declares dependencies
so installing it pulls compatible versions of duckdb + polars for Apple Silicon.

If you need a single *physical* wheel that embeds both projects' compiled
artifacts, you would need a more advanced build process (potentially using
`delocated`/`auditwheel` equivalents and vendoring sources). For many use-cases
this lightweight meta-package is sufficient.
"""
from importlib.metadata import version, PackageNotFoundError

__all__ = ["duckdb", "polars"]

try:
    __version__ = version("unified-duckpolars")
except PackageNotFoundError:  # pragma: no cover
    __version__ = "0.0.0"

# Re-export commonly used namespaces for convenience.
try:  # pragma: no cover - simple import pass-through
    import duckdb  # type: ignore
    import polars as pl  # type: ignore
except Exception:  # pragma: no cover
    duckdb = None  # type: ignore
    pl = None  # type: ignore
