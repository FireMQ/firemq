// Conformance driver for the Kotlin binding. See spec/conformance.md.
//
// Reads one JSON command per line on stdin and writes one JSON reply per line on stdout.
//
//   gradle conformanceDriver
//   firemq-conformance run --driver bindings/kotlin/build/conformance-driver
package io.github.firemq.conformance

import io.github.firemq.Client
import io.github.firemq.Delivery
import io.github.firemq.FireMqException
import io.github.firemq.MessageProperties
import io.github.firemq.PublishOptions
import io.github.firemq.Subscription
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlinx.serialization.json.put
import java.util.Base64

class Failure(val kind: String, message: String) : Exception(message)

fun error(kind: String, code: Int, message: String?) = buildJsonObject {
    put("kind", kind)
    put("code", code)
    put("message", message ?: "")
}

fun errorOf(e: FireMqException): JsonObject = when (e) {
    is FireMqException.Server -> error("server", e.code.toInt(), e.reason)
    is FireMqException.Unroutable -> error("unroutable", 312, e.reason)
    is FireMqException.Nacked -> error("nacked", e.code.toInt(), e.reason)
    is FireMqException.Closed -> error("closed", 0, e.reason)
    is FireMqException.Other -> error("other", 0, e.reason)
}

fun JsonObject.str(key: String): String = this[key]?.jsonPrimitive?.content ?: ""
fun JsonObject.bool(key: String): Boolean = this[key]?.jsonPrimitive?.boolean ?: false
fun JsonObject.num(key: String): Long? = (this[key] as? JsonPrimitive)?.takeIf { !it.isString }?.long
fun JsonObject.optStr(key: String): String? =
    (this[key] as? JsonPrimitive)?.takeIf { it.isString }?.content

/** A JSON object as text, or null for a missing or null value. */
fun JsonObject.jsonText(key: String): String? = this[key]?.takeIf { it !is JsonNull }?.toString()

fun properties(p: JsonObject) = MessageProperties(
    contentType = p.optStr("content_type"),
    contentEncoding = p.optStr("content_encoding"),
    correlationId = p.optStr("correlation_id"),
    replyTo = p.optStr("reply_to"),
    expiration = p.num("expiration")?.toULong(),
    messageId = p.optStr("message_id"),
    timestamp = p.num("timestamp")?.toULong(),
    messageType = p.optStr("type"),
    userId = p.optStr("user_id"),
    appId = p.optStr("app_id"),
    priority = p.num("priority")?.toUByte(),
)

fun propertiesJson(p: MessageProperties) = buildJsonObject {
    p.contentType?.let { put("content_type", it) }
    p.contentEncoding?.let { put("content_encoding", it) }
    p.correlationId?.let { put("correlation_id", it) }
    p.replyTo?.let { put("reply_to", it) }
    p.expiration?.let { put("expiration", it.toLong()) }
    p.messageId?.let { put("message_id", it) }
    p.timestamp?.let { put("timestamp", it.toLong()) }
    p.messageType?.let { put("type", it) }
    p.userId?.let { put("user_id", it) }
    p.appId?.let { put("app_id", it) }
    p.priority?.let { put("priority", it.toInt()) }
}

class Driver {
    private var client: Client? = null
    private val subs = mutableMapOf<String, Subscription>()
    private val pending = mutableMapOf<String, Delivery>()
    private var nextId = 0

    private fun conn() = client ?: throw Failure("closed", "not connected")

    private fun keep(m: Delivery): JsonObject {
        val tag = (++nextId).toString()
        pending[tag] = m
        val body = m.body()
        val text = Charsets.UTF_8.newDecoder().runCatching {
            decode(java.nio.ByteBuffer.wrap(body)).toString()
        }.getOrNull()
        return buildJsonObject {
            put("body", text)
            put("body_base64", Base64.getEncoder().encodeToString(body))
            put("exchange", m.exchange())
            put("routing_key", m.routingKey())
            put("redelivered", m.redelivered())
            put("tag", tag)
            put("properties", propertiesJson(m.properties()))
            put("headers", Json.parseToJsonElement(m.headersJson()))
        }
    }

    private fun take(c: JsonObject) = pending.remove(c.str("tag")) ?: throw Failure("other", "unknown tag")

    private fun messages(n: ULong) = buildJsonObject { put("messages", n.toLong()) }

    fun handle(op: String, c: JsonObject): JsonElement {
        val empty = JsonObject(emptyMap())
        return when (op) {
            "hello" -> buildJsonObject {
                put("sdk", "firemq-kotlin")
                put("version", "0.1.0")
                put("capabilities", kotlinx.serialization.json.JsonArray(emptyList()))
            }
            "connect" -> {
                client = Client.connect(c.str("url"))
                empty
            }
            "close" -> {
                subs.clear()
                pending.clear()
                val old = client
                client = null
                old?.disconnect()
                empty
            }
            "declare_queue" -> {
                val q = conn().declareQueue(
                    c.str("name"), c.bool("durable"), c.bool("exclusive"), c.bool("auto_delete"),
                    c.jsonText("args"),
                )
                buildJsonObject {
                    put("name", q.name)
                    put("messages", q.messages.toLong())
                    put("consumers", q.consumers.toLong())
                }
            }
            "delete_queue" -> messages(conn().deleteQueue(c.str("name")))
            "purge_queue" -> messages(conn().purgeQueue(c.str("name")))
            "declare_exchange" -> {
                conn().declareExchange(c.str("name"), c.str("type"), durable = c.bool("durable"))
                empty
            }
            "delete_exchange" -> {
                conn().deleteExchange(c.str("name"))
                empty
            }
            "bind_queue" -> {
                conn().bindQueue(c.str("queue"), c.str("exchange"), c.str("key"), c.jsonText("args"))
                empty
            }
            "unbind_queue" -> {
                conn().unbindQueue(c.str("queue"), c.str("exchange"), c.str("key"))
                empty
            }
            "bind_exchange" -> {
                conn().bindExchange(c.str("destination"), c.str("source"), c.str("key"))
                empty
            }
            "publish" -> {
                val b64 = c.optStr("body_base64")
                val body = b64?.let { Base64.getDecoder().decode(it) } ?: c.str("body").toByteArray()
                val options = PublishOptions(
                    persistent = c.bool("persistent"),
                    mandatory = c.bool("mandatory"),
                    confirm = true,
                    properties = properties(c["properties"]?.jsonObject ?: JsonObject(emptyMap())),
                    headersJson = c.jsonText("headers"),
                )
                conn().publish(c.str("exchange"), c.str("key"), body, options)
                empty
            }
            "subscribe" -> {
                val sub = conn().subscribe(
                    c.str("queue"), (c.num("prefetch") ?: 100).toUInt(), c.bool("no_ack"),
                )
                val id = "sub-${++nextId}"
                subs[id] = sub
                buildJsonObject { put("sub", id) }
            }
            "next" -> {
                val sub = subs[c.str("sub")] ?: throw Failure("other", "unknown sub")
                val m = try {
                    sub.next((c.num("timeout_ms") ?: 1000).toULong())
                } catch (e: FireMqException.Closed) {
                    null
                }
                buildJsonObject { put("message", m?.let(::keep) ?: JsonNull) }
            }
            "ack" -> {
                take(c).ack()
                empty
            }
            "nack" -> {
                take(c).nack(c.bool("requeue"))
                empty
            }
            "unsubscribe" -> {
                subs.remove(c.str("sub"))?.cancel()
                empty
            }
            "get" -> {
                val m = conn().get(c.str("queue"), c.bool("no_ack"))
                buildJsonObject { put("message", m?.let(::keep) ?: JsonNull) }
            }
            else -> throw Failure("unsupported", "unknown op $op")
        }
    }
}

fun main() {
    val driver = Driver()
    val out = System.out
    while (true) {
        val line = readlnOrNull() ?: break
        if (line.isBlank()) continue
        val cmd = try {
            Json.parseToJsonElement(line).jsonObject
        } catch (e: Exception) {
            System.err.println("driver: bad command: $e")
            continue
        }
        val id = cmd["id"] ?: JsonNull
        val reply = try {
            buildJsonObject {
                put("id", id)
                put("ok", true)
                put("result", driver.handle(cmd.str("op"), cmd))
            }
        } catch (e: Exception) {
            val err = when (e) {
                is Failure -> error(e.kind, 0, e.message)
                is FireMqException -> errorOf(e)
                else -> error("other", 0, e.toString())
            }
            buildJsonObject {
                put("id", id)
                put("ok", false)
                put("error", err)
            }
        }
        out.println(reply.toString())
        out.flush()
    }
}
