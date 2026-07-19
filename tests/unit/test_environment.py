from __future__ import annotations

import importlib
from pathlib import Path

import pytest

from retrieval.environment import (
    ENV_FILE_VARIABLE,
    EnvironmentInjectionError,
    inject_environment,
)


def _write(path: Path, contents: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(contents, encoding="utf-8")
    return path


def test_default_dotenv_injects_missing_values_without_overriding_process_environment(
    tmp_path: Path,
) -> None:
    dotenv = _write(
        tmp_path / ".env",
        'INJECTED="hello world"\nPRESERVED=from-file\nBARE_KEY\n',
    )
    environ = {"PRESERVED": "from-process"}

    loaded = inject_environment(environ=environ, cwd=tmp_path)

    assert loaded == dotenv.resolve()
    assert environ == {"INJECTED": "hello world", "PRESERVED": "from-process"}


def test_explicit_relative_environment_file_is_resolved_from_working_directory(
    tmp_path: Path,
) -> None:
    dotenv = _write(tmp_path / "config" / "runtime.env", "INJECTED=explicit\n")
    environ = {ENV_FILE_VARIABLE: "config/runtime.env"}

    loaded = inject_environment(environ=environ, cwd=tmp_path)

    assert loaded == dotenv.resolve()
    assert environ["INJECTED"] == "explicit"


@pytest.mark.parametrize("selector", ["", " ", "\t"])
def test_empty_environment_file_selector_disables_default_loading(
    tmp_path: Path,
    selector: str,
) -> None:
    _write(tmp_path / ".env", "INJECTED=unexpected\n")
    environ = {ENV_FILE_VARIABLE: selector}

    assert inject_environment(environ=environ, cwd=tmp_path) is None
    assert "INJECTED" not in environ


def test_missing_default_dotenv_is_a_noop(tmp_path: Path) -> None:
    environ: dict[str, str] = {}

    assert inject_environment(environ=environ, cwd=tmp_path) is None
    assert environ == {}


def test_default_loading_does_not_search_parent_directories(tmp_path: Path) -> None:
    _write(tmp_path / ".env", "INJECTED=unexpected\n")
    child = tmp_path / "child"
    child.mkdir()
    environ: dict[str, str] = {}

    assert inject_environment(environ=environ, cwd=child) is None
    assert environ == {}


def test_missing_explicit_environment_file_fails_clearly(tmp_path: Path) -> None:
    environ = {ENV_FILE_VARIABLE: "missing.env"}

    with pytest.raises(EnvironmentInjectionError, match="environment file does not exist"):
        inject_environment(environ=environ, cwd=tmp_path)


def test_explicit_environment_file_must_be_a_regular_file(tmp_path: Path) -> None:
    environ = {ENV_FILE_VARIABLE: str(tmp_path)}

    with pytest.raises(EnvironmentInjectionError, match="not a regular file"):
        inject_environment(environ=environ, cwd=tmp_path)


def test_invalid_utf8_fails_without_exposing_file_contents(tmp_path: Path) -> None:
    dotenv = tmp_path / "invalid.env"
    dotenv.write_bytes(b"SECRET=do-not-expose-\xff\n")
    environ = {ENV_FILE_VARIABLE: str(dotenv)}

    with pytest.raises(EnvironmentInjectionError, match="could not read") as raised:
        inject_environment(environ=environ, cwd=tmp_path)

    assert "do-not-expose" not in str(raised.value)


def test_existing_empty_process_value_is_not_replaced(tmp_path: Path) -> None:
    _write(tmp_path / ".env", "PRESERVED=from-file\n")
    environ = {"PRESERVED": ""}

    inject_environment(environ=environ, cwd=tmp_path)

    assert environ["PRESERVED"] == ""


def test_dotenv_interpolation_is_disabled(tmp_path: Path) -> None:
    _write(tmp_path / ".env", "REFERENCE=${PROCESS_SECRET}\n")
    environ = {"PROCESS_SECRET": "must-not-be-copied"}

    inject_environment(environ=environ, cwd=tmp_path)

    assert environ["REFERENCE"] == "${PROCESS_SECRET}"


def test_demo_app_main_injects_before_reading_the_port(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import apps.retrieval_demo.app as demo_app

    events: list[str] = []

    def fake_inject_environment() -> None:
        events.append("inject")
        monkeypatch.setenv("DATABRICKS_APP_PORT", "8765")

    def fake_run(application: object, *, host: str, port: int) -> None:
        events.append("run")
        assert application is demo_app.app
        assert host == "0.0.0.0"
        assert port == 8765

    monkeypatch.setattr(demo_app, "inject_environment", fake_inject_environment)
    monkeypatch.setattr("uvicorn.run", fake_run)

    demo_app.main()

    assert events == ["inject", "run"]


def test_worker_main_injects_before_starting_workers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import retrieval.temporal.worker as worker

    events: list[str] = []

    def fake_inject_environment() -> None:
        events.append("inject")

    async def fake_run_worker() -> None:
        events.append("run")

    monkeypatch.setattr(worker, "inject_environment", fake_inject_environment)
    monkeypatch.setattr(worker, "run_worker", fake_run_worker)

    worker.main()

    assert events == ["inject", "run"]


@pytest.mark.parametrize(
    ("module_name", "argv", "raises_exit"),
    [
        ("retrieval.lakebase.migrations", ["--check"], True),
        ("retrieval.demo.migrations", ["--check"], True),
        (
            "retrieval.lakebase.grants",
            ["--app-role", "app-role", "--worker-role", "worker-role"],
            False,
        ),
    ],
)
def test_database_cli_injects_before_running(
    monkeypatch: pytest.MonkeyPatch,
    module_name: str,
    argv: list[str],
    raises_exit: bool,
) -> None:
    module = importlib.import_module(module_name)
    events: list[str] = []

    def fake_inject_environment() -> None:
        events.append("inject")

    async def fake_run_cli(args: object) -> int:
        events.append("run")
        return 0

    monkeypatch.setattr(module, "inject_environment", fake_inject_environment)
    monkeypatch.setattr(module, "_run_cli", fake_run_cli)

    if raises_exit:
        with pytest.raises(SystemExit) as raised:
            module.main(argv)
        assert raised.value.code == 0
    else:
        module.main(argv)

    assert events == ["inject", "run"]
