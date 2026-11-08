// RabbitMQ tutorial scenarios with the RabbitMQ Java client against FireMQ.
// Run: java -cp amqp-client.jar:slf4j-api.jar Compat.java   (AMQP_URL selects the broker)

import com.rabbitmq.client.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

public class Compat {
    static int failures = 0;
    static Connection conn;

    interface Check { void run() throws Exception; }

    static void test(String name, Check c) {
        try {
            c.run();
            System.out.println("ok    " + name);
        } catch (Throwable e) {
            failures++;
            System.out.println("FAIL  " + name + ": " + e);
        }
    }

    static void expect(boolean cond, String what) {
        if (!cond) throw new AssertionError(what);
    }

    static String uid(String p) { return p + "-" + UUID.randomUUID().toString().substring(0, 8); }

    static byte[] b(String s) { return s.getBytes(StandardCharsets.UTF_8); }

    static String s(byte[] b) { return new String(b, StandardCharsets.UTF_8); }

    public static void main(String[] args) throws Exception {
        ConnectionFactory f = new ConnectionFactory();
        f.setUri(System.getenv().getOrDefault("AMQP_URL", "amqp://guest:guest@localhost:5672"));
        conn = f.newConnection("firemq-compat-java");

        test("hello world", () -> {
            Channel ch = conn.createChannel();
            String q = uid("hello");
            ch.queueDeclare(q, false, false, false, null);
            ch.basicPublish("", q, null, b("Hello World!"));
            BlockingQueue<String> got = new ArrayBlockingQueue<>(1);
            ch.basicConsume(q, true, (tag, d) -> got.add(s(d.getBody())), tag -> {});
            expect("Hello World!".equals(got.poll(5, TimeUnit.SECONDS)), "message received");
            ch.close();
        });

        test("work queues with qos and manual ack", () -> {
            Channel ch = conn.createChannel();
            String q = uid("tasks");
            ch.queueDeclare(q, true, false, false, null);
            for (int i = 0; i < 5; i++) ch.basicPublish("", q, MessageProperties.PERSISTENT_TEXT_PLAIN, b("task " + i));
            ch.basicQos(1);
            BlockingQueue<String> got = new LinkedBlockingQueue<>();
            ch.basicConsume(q, false, (tag, d) -> {
                got.add(s(d.getBody()));
                ch.basicAck(d.getEnvelope().getDeliveryTag(), false);
            }, tag -> {});
            for (int i = 0; i < 5; i++) expect(("task " + i).equals(got.poll(5, TimeUnit.SECONDS)), "task " + i);
            ch.close();
        });

        test("publish/subscribe and routing", () -> {
            Channel ch = conn.createChannel();
            String ex = uid("logs");
            ch.exchangeDeclare(ex, BuiltinExchangeType.FANOUT);
            String q1 = ch.queueDeclare().getQueue();
            String q2 = ch.queueDeclare().getQueue();
            ch.queueBind(q1, ex, "");
            ch.queueBind(q2, ex, "");
            ch.basicPublish(ex, "", null, b("broadcast"));
            Thread.sleep(100);
            for (String q : List.of(q1, q2)) {
                GetResponse r = ch.basicGet(q, true);
                expect(r != null && "broadcast".equals(s(r.getBody())), "fanout to " + q);
            }
            ch.close();
        });

        test("publisher confirms", () -> {
            Channel ch = conn.createChannel();
            String q = uid("confirmed");
            ch.queueDeclare(q, true, false, false, null);
            ch.confirmSelect();
            for (int i = 0; i < 100; i++) ch.basicPublish("", q, MessageProperties.PERSISTENT_BASIC, b("m" + i));
            ch.waitForConfirmsOrDie(5000);
            expect(ch.messageCount(q) == 100, "100 messages");
            ch.close();
        });

        test("mandatory unroutable publish is returned", () -> {
            Channel ch = conn.createChannel();
            BlockingQueue<Integer> codes = new ArrayBlockingQueue<>(1);
            ch.addReturnListener(r -> codes.add(r.getReplyCode()));
            ch.basicPublish("", uid("nowhere"), true, null, b("lost"));
            Integer code = codes.poll(5, TimeUnit.SECONDS);
            expect(code != null && code == 312, "return with 312");
            ch.close();
        });

        test("inequivalent redeclare closes only the channel with 406", () -> {
            Channel ch = conn.createChannel();
            String q = uid("strict");
            ch.queueDeclare(q, false, false, false, null);
            try {
                ch.queueDeclare(q, true, false, false, null);
                throw new AssertionError("expected an error");
            } catch (java.io.IOException e) {
                ShutdownSignalException sse = (ShutdownSignalException) e.getCause();
                AMQP.Channel.Close reason = (AMQP.Channel.Close) sse.getReason();
                expect(reason.getReplyCode() == 406, "406");
            }
            expect(conn.isOpen(), "connection still open");
        });

        test("consumer cancel notification on queue delete", () -> {
            Channel ch = conn.createChannel();
            String q = uid("doomed");
            ch.queueDeclare(q, false, false, false, null);
            CountDownLatch cancelled = new CountDownLatch(1);
            ch.basicConsume(q, true, (tag, d) -> {}, tag -> cancelled.countDown());
            Channel other = conn.createChannel();
            other.queueDelete(q);
            expect(cancelled.await(5, TimeUnit.SECONDS), "handleCancel called");
            other.close();
        });

        test("transactions commit and rollback", () -> {
            Channel ch = conn.createChannel();
            String q = uid("tx");
            ch.queueDeclare(q, false, false, false, null);
            ch.txSelect();
            ch.basicPublish("", q, null, b("rolled back"));
            ch.txRollback();
            ch.basicPublish("", q, null, b("committed"));
            ch.txCommit();
            GetResponse r = ch.basicGet(q, true);
            expect(r != null && "committed".equals(s(r.getBody())), "committed message only");
            expect(ch.basicGet(q, true) == null, "nothing else");
            ch.close();
        });

        conn.close();
        System.out.println(failures == 0 ? "all passed" : failures + " failed");
        System.exit(failures == 0 ? 0 : 1);
    }
}
