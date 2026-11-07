/*
 * Publishes a message to a temporary queue, consumes it and acknowledges it.
 *
 *   make run FIREMQ_URL=fire://guest:guest@localhost:5673
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "firemq.h"

static int fail(const char *what) {
    fprintf(stderr, "%s failed: %s (code %u)\n", what, firemq_last_error(),
            (unsigned)firemq_last_error_code());
    return 1;
}

int main(void) {
    const char *url = getenv("FIREMQ_URL");
    if (url == NULL) url = "fire://guest:guest@localhost:5673";

    FiremqClient *client = NULL;
    if (firemq_connect(url, &client) != FIREMQ_OK) return fail("connect");
    printf("connected with libfiremq_client_ffi %s\n", firemq_version());

    /* A server-named queue that disappears with the connection. */
    char *queue = NULL;
    if (firemq_declare_queue(client, "", false, true, false, NULL, &queue, NULL, NULL) != FIREMQ_OK)
        return fail("declare_queue");

    FiremqPublishOptions options = firemq_publish_options_default();
    options.properties_json = "{\"content_type\": \"text/plain\"}";
    options.headers_json = "{\"lang\": \"c\"}";
    const char *text = "hello from C";
    if (firemq_publish(client, "", queue, (const uint8_t *)text, strlen(text), &options) != FIREMQ_OK)
        return fail("publish");

    FiremqSubscription *sub = NULL;
    if (firemq_subscribe(client, queue, 10, false, &sub) != FIREMQ_OK) return fail("subscribe");

    FiremqMessage *msg = NULL;
    if (firemq_next(sub, 5000, &msg) != FIREMQ_OK) return fail("next");
    if (msg == NULL) {
        fprintf(stderr, "no message within 5 s\n");
        return 1;
    }
    size_t len = 0;
    const uint8_t *body = firemq_message_body(msg, &len);
    printf("received \"%.*s\" on %s, headers %s, properties %s\n", (int)len, (const char *)body,
           firemq_message_routing_key(msg), firemq_message_headers_json(msg),
           firemq_message_properties_json(msg));
    int ok = len == strlen(text) && memcmp(body, text, len) == 0;
    if (firemq_ack(msg) != FIREMQ_OK) return fail("ack");
    firemq_message_free(msg);

    /* Errors carry the AMQP reply code. */
    if (firemq_bind_queue(client, queue, "no-such-exchange", "", NULL) == FIREMQ_ERR_SERVER)
        printf("binding to a missing exchange failed with code %u\n",
               (unsigned)firemq_last_error_code());

    firemq_subscription_free(sub);
    firemq_string_free(queue);
    if (firemq_close(client) != FIREMQ_OK) return fail("close");
    firemq_free(client);
    if (!ok) {
        fprintf(stderr, "body changed in transit\n");
        return 1;
    }
    printf("ok\n");
    return 0;
}
