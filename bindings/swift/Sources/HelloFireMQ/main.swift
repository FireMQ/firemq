// Publishes a message to a temporary queue, consumes it and acknowledges it.
import FireMQ
import Foundation

let url = ProcessInfo.processInfo.environment["FIREMQ_URL"] ?? "fire://guest:guest@localhost:5673"

do {
    let client = try Client.connect(url: url)
    let queue = try client.declareQueue(name: "", durable: false, exclusive: true)

    let options = try PublishOptions(
        headers: ["lang": "swift", "attempt": 1],
        properties: MessageProperties(contentType: "text/plain"))
    try client.publish(exchange: "", key: queue.name, text: "hello from Swift", options: options)

    let subscription = try client.subscribe(queue: queue.name, prefetch: 10)
    guard let message = try subscription.next(timeoutMs: 5000) else {
        fatalError("no message within 5 s")
    }
    let headers = try message.headers()
    print("received \"\(message.text ?? "")\" on \(message.routingKey()), headers \(headers)")
    precondition(message.text == "hello from Swift")
    precondition(headers["lang"] as? String == "swift")
    precondition(message.properties().contentType == "text/plain")
    try message.ack()
    try subscription.cancel()

    do {
        try client.bindQueue(queue: queue.name, exchange: "no-such-exchange", key: "")
    } catch let error as FireMqError {
        print("binding to a missing exchange failed with code \(error.code)")
    }

    try client.close()
    print("ok")
} catch {
    print("error: \(error)")
    exit(1)
}
