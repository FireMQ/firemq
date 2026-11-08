import os
import uuid

import pytest

import firemq

URL = os.environ.get("FIREMQ_URL", firemq.DEFAULT_URL)


@pytest.fixture
def url():
    return URL


@pytest.fixture
def client():
    with firemq.connect(URL) as c:
        yield c


@pytest.fixture
def queue(client):
    """A fresh queue, deleted after the test."""
    name = f"py.{uuid.uuid4().hex[:12]}"
    client.declare_queue(name)
    yield name
    try:
        client.delete_queue(name)
    except firemq.FireMQError:
        pass
