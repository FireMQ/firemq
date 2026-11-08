// Conformance driver for the Swift binding. See spec/conformance.md.
//
// Reads one JSON command per line on stdin and writes one JSON reply per line on stdout.
//
//   swift build -Xlinker -L../../target/debug
//   firemq-conformance run --driver bindings/swift/.build/debug/firemq-conformance-driver
import FireMQ
import Foundation

struct Failure: Error {
    let kind: String
    let message: String
}

func errorObject(_ kind: String, _ code: Int, _ message: String) -> [String: Any] {
    ["kind": kind, "code": code, "message": message]
}

func errorObject(_ e: FireMqError) -> [String: Any] {
    switch e {
    case .Server(let code, let message): return errorObject("server", Int(code), message)
    case .Unroutable(let message): return errorObject("unroutable", 312, message)
    case .Nacked(let code, let message): return errorObject("nacked", Int(code), message)
    case .Closed(let message): return errorObject("closed", 0, message)
    case .Other(let message): return errorObject("other", 0, message)
    }
}

/// Encodes a JSON value as text, or nil for a missing or null value.
func jsonText(_ value: Any?) throws -> String? {
    guard let value, !(value is NSNull) else { return nil }
    return String(decoding: try JSONSerialization.data(withJSONObject: value), as: UTF8.self)
}

func string(_ c: [String: Any], _ key: String) -> String { c[key] as? String ?? "" }
func bool(_ c: [String: Any], _ key: String) -> Bool { (c[key] as? NSNumber)?.boolValue ?? false }
func number(_ c: [String: Any], _ key: String) -> NSNumber? { c[key] as? NSNumber }

func properties(_ p: [String: Any]) -> MessageProperties {
    MessageProperties(
        contentType: p["content_type"] as? String,
        contentEncoding: p["content_encoding"] as? String,
        correlationId: p["correlation_id"] as? String,
        replyTo: p["reply_to"] as? String,
        expiration: number(p, "expiration")?.uint64Value,
        messageId: p["message_id"] as? String,
        timestamp: number(p, "timestamp")?.uint64Value,
        messageType: p["type"] as? String,
        userId: p["user_id"] as? String,
        appId: p["app_id"] as? String,
        priority: number(p, "priority")?.uint8Value)
}

func propertiesObject(_ p: MessageProperties) -> [String: Any] {
    let fields: [(String, Any?)] = [
        ("content_type", p.contentType), ("content_encoding", p.contentEncoding),
        ("correlation_id", p.correlationId), ("reply_to", p.replyTo),
        ("expiration", p.expiration), ("message_id", p.messageId), ("timestamp", p.timestamp),
        ("type", p.messageType), ("user_id", p.userId), ("app_id", p.appId),
        ("priority", p.priority),
    ]
    var out: [String: Any] = [:]
    for (key, value) in fields {
        if let value { out[key] = value }
    }
    return out
}

final class Driver {
    var client: Client?
    var subs: [String: Subscription] = [:]
    var pending: [String: Delivery] = [:]
    var nextId = 0

    func conn() throws -> Client {
        guard let client else { throw Failure(kind: "closed", message: "not connected") }
        return client
    }

    func keep(_ m: Delivery) throws -> [String: Any] {
        nextId += 1
        let tag = String(nextId)
        pending[tag] = m
        let body = m.body()
        return [
            "body": String(data: body, encoding: .utf8) as Any? ?? NSNull(),
            "body_base64": body.base64EncodedString(),
            "exchange": m.exchange(),
            "routing_key": m.routingKey(),
            "redelivered": m.redelivered(),
            "tag": tag,
            "properties": propertiesObject(m.properties()),
            "headers": try JSONSerialization.jsonObject(with: Data(m.headersJson().utf8)),
        ]
    }

    func take(_ c: [String: Any]) throws -> Delivery {
        guard let m = pending.removeValue(forKey: string(c, "tag")) else {
            throw Failure(kind: "other", message: "unknown tag")
        }
        return m
    }

    func handle(_ op: String, _ c: [String: Any]) throws -> [String: Any] {
        switch op {
        case "hello":
            return ["sdk": "firemq-swift", "version": "0.1.0", "capabilities": [String]()]
        case "connect":
            client = try Client.connect(url: string(c, "url"))
            return [:]
        case "close":
            subs.removeAll()
            pending.removeAll()
            let old = client
            client = nil
            try old?.close()
            return [:]
        case "declare_queue":
            let q = try conn().declareQueue(
                name: string(c, "name"), durable: bool(c, "durable"),
                exclusive: bool(c, "exclusive"), autoDelete: bool(c, "auto_delete"),
                argsJson: jsonText(c["args"]))
            return ["name": q.name, "messages": q.messages, "consumers": q.consumers]
        case "delete_queue":
            return ["messages": try conn().deleteQueue(name: string(c, "name"))]
        case "purge_queue":
            return ["messages": try conn().purgeQueue(name: string(c, "name"))]
        case "declare_exchange":
            try conn().declareExchange(
                name: string(c, "name"), kind: string(c, "type"), durable: bool(c, "durable"))
            return [:]
        case "delete_exchange":
            try conn().deleteExchange(name: string(c, "name"))
            return [:]
        case "bind_queue":
            try conn().bindQueue(
                queue: string(c, "queue"), exchange: string(c, "exchange"),
                key: string(c, "key"), argsJson: jsonText(c["args"]))
            return [:]
        case "unbind_queue":
            try conn().unbindQueue(
                queue: string(c, "queue"), exchange: string(c, "exchange"), key: string(c, "key"))
            return [:]
        case "bind_exchange":
            try conn().bindExchange(
                destination: string(c, "destination"), source: string(c, "source"),
                key: string(c, "key"))
            return [:]
        case "publish":
            let body: Data
            if let b64 = c["body_base64"] as? String {
                guard let data = Data(base64Encoded: b64) else {
                    throw Failure(kind: "other", message: "invalid base64 body")
                }
                body = data
            } else {
                body = Data(string(c, "body").utf8)
            }
            let options = PublishOptions(
                persistent: bool(c, "persistent"), mandatory: bool(c, "mandatory"), confirm: true,
                properties: properties(c["properties"] as? [String: Any] ?? [:]),
                headersJson: try jsonText(c["headers"]))
            try conn().publish(
                exchange: string(c, "exchange"), key: string(c, "key"), body: body,
                options: options)
            return [:]
        case "subscribe":
            let sub = try conn().subscribe(
                queue: string(c, "queue"), prefetch: number(c, "prefetch")?.uint32Value ?? 100,
                noAck: bool(c, "no_ack"))
            nextId += 1
            let id = "sub-\(nextId)"
            subs[id] = sub
            return ["sub": id]
        case "next":
            guard let sub = subs[string(c, "sub")] else {
                throw Failure(kind: "other", message: "unknown sub")
            }
            let timeout = number(c, "timeout_ms")?.uint64Value ?? 1000
            let message: Delivery?
            do {
                message = try sub.next(timeoutMs: timeout)
            } catch FireMqError.Closed {
                message = nil
            }
            return ["message": try message.map(keep) ?? NSNull()]
        case "ack":
            try take(c).ack()
            return [:]
        case "nack":
            try take(c).nack(requeue: bool(c, "requeue"))
            return [:]
        case "unsubscribe":
            try subs.removeValue(forKey: string(c, "sub"))?.cancel()
            return [:]
        case "get":
            let m = try conn().get(queue: string(c, "queue"), noAck: bool(c, "no_ack"))
            return ["message": try m.map(keep) ?? NSNull()]
        default:
            throw Failure(kind: "unsupported", message: "unknown op \(op)")
        }
    }
}

let driver = Driver()
while let line = readLine() {
    if line.trimmingCharacters(in: .whitespaces).isEmpty { continue }
    guard let cmd = (try? JSONSerialization.jsonObject(with: Data(line.utf8))) as? [String: Any]
    else {
        FileHandle.standardError.write(Data("driver: bad command: \(line)\n".utf8))
        continue
    }
    let id = cmd["id"] ?? NSNull()
    var reply: [String: Any]
    do {
        reply = ["id": id, "ok": true, "result": try driver.handle(string(cmd, "op"), cmd)]
    } catch let f as Failure {
        reply = ["id": id, "ok": false, "error": errorObject(f.kind, 0, f.message)]
    } catch let e as FireMqError {
        reply = ["id": id, "ok": false, "error": errorObject(e)]
    } catch {
        reply = ["id": id, "ok": false, "error": errorObject("other", 0, "\(error)")]
    }
    let data = (try? JSONSerialization.data(withJSONObject: reply)) ?? Data("{}".utf8)
    FileHandle.standardOutput.write(data + Data("\n".utf8))
}
