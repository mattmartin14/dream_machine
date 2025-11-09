"""Local development patches for Airflow 3.x with structlog>=25.

Airflow 3.1.1 still imports `Styles` from `structlog.dev`, which was removed in
structlog 25. We inject a minimal shim so the import path succeeds.

We also add a helper to print confirmation that patching occurred when
`AIRFLOW_DEBUG_PATCHES=1` is set.
"""

import os

def _patch_structlog_styles():
    try:
        import structlog.dev as _dev  # type: ignore
    except Exception as e:  # pragma: no cover
        if os.getenv("AIRFLOW_DEBUG_PATCHES"):
            print("[sitecustomize] structlog.dev import failed", e)
        return

    if hasattr(_dev, "Styles"):
        # Already present (older structlog), nothing to do.
        if os.getenv("AIRFLOW_DEBUG_PATCHES"):
            print("[sitecustomize] Styles already present")
        return

    class _StylesShim:  # minimal interface used by Airflow's PercentFormatRender
        reset = ""

    _dev.Styles = _StylesShim  # type: ignore[attr-defined]
    if os.getenv("AIRFLOW_DEBUG_PATCHES"):
        print("[sitecustomize] Injected Styles shim into structlog.dev")

_patch_structlog_styles()
