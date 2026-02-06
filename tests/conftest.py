"""pytest configuration and shared fixtures."""

import pytest


@pytest.fixture
def sample_data():
    """Sample source data for tests."""
    return {
        "user": {
            "name": "Alice",
            "age": "30",
            "email": "alice@example.com",
        },
        "items": [
            {"id": 1, "price": 10},
            {"id": 2, "price": 20},
            {"id": 1, "price": 15},  # duplicate id
        ],
        "config": {
            "enabled": True,
            "timeout": 5000,
        },
    }


@pytest.fixture
def empty_dest():
    """Empty destination document."""
    return {}
