"""Safe environment-file injection for executable entry points.

Process variables supplied by a shell, container runtime, or Databricks always win.  The
default file is only ``.env`` in the process working directory; parent directories are never
searched implicitly.
"""

from __future__ import annotations

import os
from collections.abc import MutableMapping
from pathlib import Path

from dotenv import dotenv_values

ENV_FILE_VARIABLE = "RETRIEVAL_ENV_FILE"


class EnvironmentInjectionError(RuntimeError):
    """Raised when an explicitly selected environment file cannot be loaded safely."""


def inject_environment(
    *,
    environ: MutableMapping[str, str] | None = None,
    cwd: Path | None = None,
) -> Path | None:
    """Add values from a local environment file without replacing process variables.

    ``RETRIEVAL_ENV_FILE`` selects an explicit file. A relative path is resolved from ``cwd``
    (or the current working directory), while an empty value disables file loading. Without the
    selector, a missing ``.env`` is an intentional no-op.
    """

    target = os.environ if environ is None else environ
    working_directory = Path.cwd() if cwd is None else Path(cwd)
    configured_path = target.get(ENV_FILE_VARIABLE)
    explicit = configured_path is not None

    if explicit:
        selected = configured_path.strip()
        if not selected:
            return None
        source = Path(selected).expanduser()
        if not source.is_absolute():
            source = working_directory / source
    else:
        source = working_directory / ".env"

    try:
        source = source.resolve()
        exists = source.exists()
        is_file = source.is_file()
    except OSError as exc:
        raise EnvironmentInjectionError(f"could not inspect environment file {source}") from exc

    if not exists:
        if explicit:
            raise EnvironmentInjectionError(f"environment file does not exist: {source}")
        return None
    if not is_file:
        raise EnvironmentInjectionError(f"environment file is not a regular file: {source}")

    try:
        values = dotenv_values(dotenv_path=source, encoding="utf-8", interpolate=False)
    except (OSError, UnicodeError) as exc:
        raise EnvironmentInjectionError(f"could not read environment file {source}") from exc

    for name, value in values.items():
        # python-dotenv represents a key without '=' as None; like load_dotenv, do not inject it.
        if value is not None:
            target.setdefault(name, value)
    return source


__all__ = ["ENV_FILE_VARIABLE", "EnvironmentInjectionError", "inject_environment"]
