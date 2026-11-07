# firemq for Python

A Python client for [FireMQ](https://github.com/FireMQ/firemq), built on the Rust client
with [UniFFI](https://mozilla.github.io/uniffi-rs/) and [maturin](https://www.maturin.rs/).

```python
import firemq

with firemq.connect("fire://guest:guest@localhost:5673") as client:
    client.declare_queue("jobs")
    client.publish("", "jobs", "hello", headers={"attempt": 1}, content_type="text/plain")

    with client.subscribe("jobs", prefetch=10) as sub:
        for msg in sub:
            print(msg.text, msg.headers)
            msg.ack()
```

Calls block until the broker answers and release the GIL while they wait. Errors are
subclasses of `firemq.FireMQError`: `ServerError` (with an AMQP `code`), `UnroutableError`,
`NackedError`, `ClosedError` and `OtherError`.

## Develop

```bash
uv venv && uv pip install maturin pytest
source .venv/bin/activate
maturin develop
FIREMQ_URL=fire://guest:guest@localhost:5673 pytest
```

The conformance driver, `conformance_driver.py`, runs the SDK conformance suite:

```bash
cargo run -p firemq-conformance -- run --driver "bindings/python/.venv/bin/python bindings/python/conformance_driver.py"
```
