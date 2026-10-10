"""Shared fixtures for the aiometeogate tests."""

import json
from pathlib import Path
from typing import Any

import pytest

FIXTURES = Path(__file__).parent / "fixtures"

FMI_STATION = "0-246-0-100971"
MET_STATION = "0-20000-0-01492"
KNMI_STATION = "0-20000-0-06260"


def load_json(name: str) -> Any:
    """Load a JSON fixture by its path below tests/fixtures."""
    return json.loads((FIXTURES / name).read_text())


def load_notification(name: str) -> tuple[str, bytes]:
    """Return the topic and raw payload of a recorded MQTT notification."""
    data = load_json(f"mqtt/notification_{name}.json")
    return data["topic"], json.dumps(data["payload"]).encode()


@pytest.fixture
def fmi_notification() -> tuple[str, bytes]:
    """Return a recorded FMI wind speed notification."""
    return load_notification("fmi")
