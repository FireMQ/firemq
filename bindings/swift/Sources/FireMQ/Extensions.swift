import Foundation

extension Client {
    /// Publishes UTF-8 text. See `publish(exchange:key:body:options:)`.
    public func publish(
        exchange: String, key: String, text: String, options: PublishOptions = PublishOptions()
    ) throws {
        try publish(exchange: exchange, key: key, body: Data(text.utf8), options: options)
    }
}

extension PublishOptions {
    /// Options with application headers given as JSON-compatible values.
    public init(
        headers: [String: Any], properties: MessageProperties = MessageProperties(),
        persistent: Bool = true, mandatory: Bool = false, confirm: Bool = true
    ) throws {
        let json = try JSONSerialization.data(withJSONObject: headers, options: [.sortedKeys])
        self.init(
            persistent: persistent, mandatory: mandatory, confirm: confirm,
            properties: properties, headersJson: String(decoding: json, as: UTF8.self))
    }
}

extension Delivery {
    /// The body decoded as UTF-8, or nil when it is not valid UTF-8.
    public var text: String? { String(data: body(), encoding: .utf8) }

    /// Application headers decoded from JSON. Binary values arrive base64-encoded.
    public func headers() throws -> [String: Any] {
        let object = try JSONSerialization.jsonObject(with: Data(headersJson().utf8))
        return object as? [String: Any] ?? [:]
    }
}

extension FireMqError {
    /// The AMQP reply code, or 0 when there is none.
    public var code: UInt16 {
        switch self {
        case .Server(let code, _), .Nacked(let code, _): return code
        case .Unroutable: return 312
        case .Closed, .Other: return 0
        }
    }
}
