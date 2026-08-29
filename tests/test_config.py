from omnicall.config import Settings


def test_disclosure_is_enabled_by_default(monkeypatch) -> None:
    monkeypatch.delenv("DISCLOSURE_ENABLED", raising=False)

    settings = Settings.from_env(require_secrets=False)

    assert settings.disclosure_enabled is True


def test_disclosure_can_be_disabled(monkeypatch) -> None:
    monkeypatch.setenv("DISCLOSURE_ENABLED", "false")

    settings = Settings.from_env(require_secrets=False)

    assert settings.disclosure_enabled is False
