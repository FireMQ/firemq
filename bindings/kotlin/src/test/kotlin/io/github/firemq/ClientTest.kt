package io.github.firemq

import java.util.UUID
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ClientTest {
    private val url = System.getenv("FIREMQ_URL") ?: "fire://guest:guest@localhost:5673"

    private fun queueName() = "kt." + UUID.randomUUID().toString().take(12)

    @Test
    fun publishConsumeAck() {
        Client.connect(url).use { client ->
            val q = client.declareQueue(queueName(), durable = false, exclusive = true)
            val options = PublishOptions(
                properties = MessageProperties(contentType = "text/plain", priority = 3u),
                headersJson = """{"lang":"kotlin","n":5}""",
            )
            client.publish("", q.name, "hello from Kotlin", options)

            val sub = client.subscribe(q.name, prefetch = 10u)
            val msg = assertNotNull(sub.next(5000uL))
            assertEquals("hello from Kotlin", msg.text)
            assertEquals(q.name, msg.routingKey())
            assertEquals("", msg.exchange())
            assertFalse(msg.redelivered())
            assertEquals("text/plain", msg.properties().contentType)
            assertEquals(3.toUByte(), msg.properties().priority)
            assertEquals("""{"lang":"kotlin","n":5}""", msg.headersJson())
            msg.ack()
            assertNull(sub.next(50uL))
            sub.cancel()
            assertFailsWith<FireMqException.Closed> { sub.next(0uL) }
            client.disconnect()
            assertTrue(client.isClosed())
        }
    }

    @Test
    fun sequenceAndRequeue() {
        Client.connect(url).use { client ->
            val q = client.declareQueue(queueName(), durable = false, exclusive = true)
            repeat(5) { client.publish("", q.name, "m$it") }
            val sub = client.subscribe(q.name, prefetch = 2u)
            val bodies = sub.asSequence().take(5).map { it.ack(); it.text }.toList()
            assertEquals(List(5) { "m$it" }, bodies)
            sub.cancel()

            client.publish("", q.name, "again")
            client.get(q.name)!!.nack(true)
            val again = assertNotNull(client.get(q.name, noAck = true))
            assertTrue(again.redelivered())
            assertEquals(0uL, client.purgeQueue(q.name))
            client.disconnect()
        }
    }

    @Test
    fun errorsCarryReplyCodes() {
        val denied = assertFailsWith<FireMqException.Server> {
            Client.connect(url.replace("guest:guest", "guest:wrong"))
        }
        assertEquals(403, denied.replyCode)

        Client.connect(url).use { client ->
            val q = client.declareQueue(queueName(), durable = false, exclusive = true)
            val lost = assertFailsWith<FireMqException.Unroutable> {
                client.publish("", q.name + ".nowhere", "x", PublishOptions(mandatory = true))
            }
            assertEquals(312, lost.replyCode)
            val missing = assertFailsWith<FireMqException.Server> {
                client.bindQueue(q.name, q.name + ".missing", "")
            }
            assertEquals(404, missing.replyCode)
            assertFailsWith<FireMqException.Other> {
                client.declareQueue(queueName(), argsJson = "[1]")
            }
            client.disconnect()
            assertFailsWith<FireMqException.Closed> { client.purgeQueue(q.name) }
        }
    }
}
