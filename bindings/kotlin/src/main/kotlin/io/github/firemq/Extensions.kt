// Conveniences over the generated bindings in firemq_client_ffi.kt.
package io.github.firemq

/** Publishes UTF-8 text. See [Client.publish]. */
fun Client.publish(
    exchange: String,
    key: String,
    text: String,
    options: PublishOptions = PublishOptions(),
) = publish(exchange, key, text.toByteArray(Charsets.UTF_8), options)

/** The body decoded as UTF-8. */
val Delivery.text: String
    get() = body().toString(Charsets.UTF_8)

/**
 * The subscription's messages, until it ends. Each wait in the native library lasts at most
 * [pollMs] milliseconds, so an interrupted thread stops promptly.
 */
fun Subscription.asSequence(pollMs: ULong = 500uL): Sequence<Delivery> = sequence {
    while (!Thread.currentThread().isInterrupted) {
        val delivery = try {
            next(pollMs)
        } catch (e: FireMqException.Closed) {
            return@sequence
        }
        if (delivery != null) yield(delivery)
    }
}

/** The AMQP reply code, for example 403, 404, 406 or 312, or 0 when there is none. */
val FireMqException.replyCode: Int
    get() = when (this) {
        is FireMqException.Server -> code.toInt()
        is FireMqException.Nacked -> code.toInt()
        is FireMqException.Unroutable -> 312
        is FireMqException.Closed, is FireMqException.Other -> 0
    }
