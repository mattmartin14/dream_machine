from datetime import date

import pytest

from hardcharger_demo.config import Settings, select_processing_dates


def test_processing_date_defaults_to_previous_day() -> None:
    assert select_processing_dates(today=date(2026, 10, 24)) == [date(2026, 10, 23)]


def test_processing_date_accepts_one_day() -> None:
    assert select_processing_dates(processing_date="2026-10-21") == [date(2026, 10, 21)]


def test_processing_date_accepts_inclusive_range() -> None:
    assert select_processing_dates(
        start_date="2026-10-21", end_date="2026-10-23"
    ) == [date(2026, 10, 21), date(2026, 10, 22), date(2026, 10, 23)]


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        (
            {"processing_date": "2026-10-21", "start_date": "2026-10-21", "end_date": "2026-10-22"},
            "not both",
        ),
        ({"start_date": "2026-10-21"}, "Both"),
        (
            {"start_date": "2026-10-23", "end_date": "2026-10-21"},
            "on or after",
        ),
    ],
)
def test_processing_date_rejects_invalid_ranges(kwargs: dict[str, str], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        select_processing_dates(**kwargs)


def test_config_requires_explicit_connection_or_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RAW_BUCKET", "test-bucket")
    monkeypatch.delenv("HARDCHARGER_URI", raising=False)
    monkeypatch.delenv("HARDCHARGER_USERNAME", raising=False)
    monkeypatch.delenv("HARDCHARGER_PASSWORD", raising=False)
    monkeypatch.delenv("HARDCHARGER_SECRET_ARN", raising=False)
    settings = Settings.from_env()
    with pytest.raises(ValueError, match="HARDCHARGER_SECRET_ARN"):
        settings.require_etl()


def test_secret_credentials_are_read_without_logging(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RAW_BUCKET", "test-bucket")
    monkeypatch.setenv("HARDCHARGER_SECRET_ARN", "arn:aws:secretsmanager:region:account:secret:x")
    settings = Settings.from_env()

    class SecretsClient:
        def get_secret_value(self, *, SecretId: str) -> dict[str, str]:
            assert SecretId.endswith(":secret:x")
            return {
                "SecretString": (
                    '{"uri":"hardcharger://example.test","username":"etl","password":"private"}'
                )
            }

    assert settings.connection_credentials(SecretsClient()) == (
        "hardcharger://example.test",
        "etl",
        "private",
    )


def test_secret_must_be_well_formed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RAW_BUCKET", "test-bucket")
    monkeypatch.setenv("HARDCHARGER_SECRET_ARN", "secret-arn")
    settings = Settings.from_env()

    class SecretsClient:
        def get_secret_value(self, **_: str) -> dict[str, str]:
            return {"SecretString": "not-json"}

    with pytest.raises(ValueError, match="JSON object"):
        settings.connection_credentials(SecretsClient())
