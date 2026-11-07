"""Conformance driver for the Python binding. See spec/conformance.md.

Reads one JSON command per line on stdin and writes one JSON reply per line on stdout.

    firemq-conformance run --driver "python bindings/python/conformance_driver.py"
"""

import base64
import json
import sys

import firemq


def error(kind, code=0, message=""):
    return {"kind": kind, "code": code, "message": message}


def error_of(e):
    if isinstance(e, firemq.ServerError):
        return error("server", e.code, e.message)
    if isinstance(e, firemq.UnroutableError):
        return error("unroutable", 312, e.message)
    if isinstance(e, firemq.NackedError):
        return error("nacked", e.code, e.message)
    if isinstance(e, firemq.ClosedError):
        return error("closed", 0, e.message)
    return error("other", 0, str(e))


class Failure(Exception):
    def __init__(self, err):
        super().__init__(err["message"])
        self.err = err


class Driver:
    def __init__(self):
        self.client = None
        self.subs = {}
        self.pending = {}
        self.next_id = 0

    def conn(self):
        if self.client is None:
            raise Failure(error("closed", 0, "not connected"))
        return self.client

    def keep(self, msg):
        self.next_id += 1
        tag = str(self.next_id)
        self.pending[tag] = msg
        try:
            text = msg.body.decode("utf-8")
        except UnicodeDecodeError:
            text = None
        return {
            "body": text,
            "body_base64": base64.b64encode(msg.body).decode("ascii"),
            "exchange": msg.exchange,
            "routing_key": msg.routing_key,
            "redelivered": msg.redelivered,
            "tag": tag,
            "properties": msg.properties,
            "headers": msg.headers,
        }

    def take(self, c):
        msg = self.pending.pop(c["tag"], None)
        if msg is None:
            raise Failure(error("other", 0, "unknown tag"))
        return msg

    def handle(self, op, c):
        if op == "hello":
            return {"sdk": "firemq-python", "version": firemq.__version__, "capabilities": []}
        if op == "connect":
            self.client = firemq.connect(c["url"])
            return {}
        if op == "close":
            self.subs.clear()
            self.pending.clear()
            client, self.client = self.client, None
            if client is not None:
                client.close()
            return {}
        if op == "declare_queue":
            q = self.conn().declare_queue(
                c["name"],
                durable=c["durable"],
                exclusive=c["exclusive"],
                auto_delete=c["auto_delete"],
                arguments=c["args"],
            )
            return {"name": q.name, "messages": q.messages, "consumers": q.consumers}
        if op == "delete_queue":
            return {"messages": self.conn().delete_queue(c["name"])}
        if op == "purge_queue":
            return {"messages": self.conn().purge_queue(c["name"])}
        if op == "declare_exchange":
            self.conn().declare_exchange(c["name"], c["type"], durable=c["durable"])
            return {}
        if op == "delete_exchange":
            self.conn().delete_exchange(c["name"])
            return {}
        if op == "bind_queue":
            self.conn().bind_queue(c["queue"], c["exchange"], c["key"], arguments=c["args"])
            return {}
        if op == "unbind_queue":
            self.conn().unbind_queue(c["queue"], c["exchange"], c["key"])
            return {}
        if op == "bind_exchange":
            self.conn().bind_exchange(c["destination"], c["source"], c["key"])
            return {}
        if op == "publish":
            if c.get("body_base64") is not None:
                body = base64.b64decode(c["body_base64"])
            else:
                body = c["body"].encode("utf-8")
            self.conn().publish(
                c["exchange"],
                c["key"],
                body,
                persistent=c["persistent"],
                mandatory=c["mandatory"],
                properties=c["properties"],
                headers=c["headers"],
            )
            return {}
        if op == "subscribe":
            sub = self.conn().subscribe(c["queue"], prefetch=c["prefetch"], no_ack=c["no_ack"])
            self.next_id += 1
            sid = f"sub-{self.next_id}"
            self.subs[sid] = sub
            return {"sub": sid}
        if op == "next":
            sub = self.subs.get(c["sub"])
            if sub is None:
                raise Failure(error("other", 0, "unknown sub"))
            try:
                msg = sub.next(timeout=c["timeout_ms"] / 1000)
            except firemq.ClosedError:
                msg = None
            return {"message": None if msg is None else self.keep(msg)}
        if op == "ack":
            self.take(c).ack()
            return {}
        if op == "nack":
            self.take(c).nack(requeue=c["requeue"])
            return {}
        if op == "unsubscribe":
            sub = self.subs.pop(c["sub"], None)
            if sub is not None:
                sub.cancel()
            return {}
        if op == "get":
            msg = self.conn().get(c["queue"], no_ack=c["no_ack"])
            return {"message": None if msg is None else self.keep(msg)}
        raise Failure(error("unsupported", 0, f"unknown op {op}"))


def main():
    driver = Driver()
    for line in sys.stdin:
        if not line.strip():
            continue
        try:
            cmd = json.loads(line)
        except ValueError as e:
            print(f"driver: bad command: {e}", file=sys.stderr)
            continue
        try:
            reply = {"id": cmd["id"], "ok": True, "result": driver.handle(cmd["op"], cmd)}
        except Failure as f:
            reply = {"id": cmd["id"], "ok": False, "error": f.err}
        except firemq.FireMQError as e:
            reply = {"id": cmd["id"], "ok": False, "error": error_of(e)}
        except Exception as e:  # report driver bugs instead of dying
            reply = {"id": cmd["id"], "ok": False, "error": error("other", 0, repr(e))}
        sys.stdout.write(json.dumps(reply) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
