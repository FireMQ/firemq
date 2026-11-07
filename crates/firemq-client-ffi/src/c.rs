//! The C ABI, described by `bindings/c/firemq.h`.
//!
//! Conventions, repeated in the header:
//!
//! - Fallible functions return `FIREMQ_OK` (0) or an error status, and write results through
//!   out-pointers. After an error, `firemq_last_error` and `firemq_last_error_code` describe it
//!   on the calling thread.
//! - Handles (`FiremqClient`, `FiremqSubscription`, `FiremqMessage`) are owned by the caller
//!   and released with their `_free` function. They may be used from any thread.
//! - `char *` and `uint8_t *` results are owned by the caller and released with
//!   `firemq_string_free` and `firemq_buffer_free`. `const` results are borrowed from the
//!   handle they came from and stay valid until it is freed.
//! - String arguments are NUL-terminated UTF-8. Arguments documented as optional may be NULL.
//!
//! Every function is `unsafe` because it trusts the caller to follow these rules.

#![allow(clippy::missing_safety_doc)]

use std::cell::RefCell;
use std::ffi::{CStr, CString, c_char};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use crate::{
    Client, Delivery, FireMqError, MessageProperties, PublishOptions, Subscription, json, runtime,
};

/// The call succeeded.
pub const FIREMQ_OK: i32 = 0;
/// The broker refused a request; `firemq_last_error_code` is the AMQP reply code.
pub const FIREMQ_ERR_SERVER: i32 = 1;
/// A mandatory publish matched no queue.
pub const FIREMQ_ERR_UNROUTABLE: i32 = 2;
/// The broker rejected a publish, for example because a queue was full.
pub const FIREMQ_ERR_NACKED: i32 = 3;
/// The connection or subscription is closed.
pub const FIREMQ_ERR_CLOSED: i32 = 4;
/// Anything else, including invalid arguments.
pub const FIREMQ_ERR_OTHER: i32 = 5;

/// A connection to FireMQ.
pub struct FiremqClient(Client);

/// A stream of messages from a queue.
pub struct FiremqSubscription(Arc<Subscription>);

/// A delivered message.
pub struct FiremqMessage {
    delivery: Arc<Delivery>,
    exchange: CString,
    routing_key: CString,
    properties_json: CString,
    headers_json: CString,
}

/// Options for `firemq_publish`. Get the defaults from `firemq_publish_options_default`.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct FiremqPublishOptions {
    /// Store the message on disk in durable queues. Default true.
    pub persistent: bool,
    /// Fail with `FIREMQ_ERR_UNROUTABLE` when no queue receives the message. Default false.
    pub mandatory: bool,
    /// Wait for the broker's confirm. Default true.
    pub confirm: bool,
    /// Optional properties as a JSON object with the protocol's keys: `content_type`,
    /// `content_encoding`, `correlation_id`, `reply_to`, `expiration`, `message_id`,
    /// `timestamp`, `type`, `user_id`, `app_id` and `priority`.
    pub properties_json: *const c_char,
    /// Optional application headers as a JSON object.
    pub headers_json: *const c_char,
}

struct LastError {
    status: i32,
    code: u16,
    message: CString,
}

thread_local! {
    static LAST_ERROR: RefCell<Option<LastError>> = const { RefCell::new(None) };
}

fn status_of(e: &FireMqError) -> i32 {
    match e {
        FireMqError::Server { .. } => FIREMQ_ERR_SERVER,
        FireMqError::Unroutable { .. } => FIREMQ_ERR_UNROUTABLE,
        FireMqError::Nacked { .. } => FIREMQ_ERR_NACKED,
        FireMqError::Closed { .. } => FIREMQ_ERR_CLOSED,
        FireMqError::Other { .. } => FIREMQ_ERR_OTHER,
    }
}

fn c_string(s: &str) -> CString {
    CString::new(s)
        .unwrap_or_else(|_| CString::new(s.replace('\0', "")).expect("NUL bytes were removed"))
}

fn invalid(message: impl Into<String>) -> FireMqError {
    FireMqError::Other {
        message: message.into(),
    }
}

/// Runs `f`, catching panics, and records its error for `firemq_last_error`.
fn call(f: impl FnOnce() -> Result<(), FireMqError>) -> i32 {
    let result = catch_unwind(AssertUnwindSafe(f))
        .unwrap_or_else(|_| Err(invalid("internal error: the FireMQ library panicked")));
    LAST_ERROR.with(|last| {
        let mut last = last.borrow_mut();
        match result {
            Ok(()) => {
                *last = None;
                FIREMQ_OK
            }
            Err(e) => {
                let status = status_of(&e);
                *last = Some(LastError {
                    status,
                    code: e.code(),
                    message: c_string(&e.to_string()),
                });
                status
            }
        }
    })
}

unsafe fn required<'a>(p: *const c_char, what: &str) -> Result<&'a str, FireMqError> {
    if p.is_null() {
        return Err(invalid(format!("{what} must not be NULL")));
    }
    // SAFETY: the caller passes a NUL-terminated string that outlives the call.
    unsafe { CStr::from_ptr(p) }
        .to_str()
        .map_err(|_| invalid(format!("{what} is not valid UTF-8")))
}

unsafe fn optional<'a>(p: *const c_char, what: &str) -> Result<Option<&'a str>, FireMqError> {
    if p.is_null() {
        Ok(None)
    } else {
        unsafe { required(p, what) }.map(Some)
    }
}

unsafe fn handle<'a, T>(p: *const T, what: &str) -> Result<&'a T, FireMqError> {
    // SAFETY: the caller passes NULL or a live handle created by this library.
    unsafe { p.as_ref() }.ok_or_else(|| invalid(format!("{what} must not be NULL")))
}

unsafe fn put<T>(out: *mut T, value: T) {
    if !out.is_null() {
        // SAFETY: the caller passes NULL or a pointer to writable memory for a `T`.
        unsafe { out.write(value) };
    }
}

fn message_handle(delivery: Arc<Delivery>) -> *mut FiremqMessage {
    let m = &delivery.inner;
    let message = FiremqMessage {
        exchange: c_string(&m.exchange),
        routing_key: c_string(&m.routing_key),
        properties_json: c_string(&json::properties_to_json(&m.properties)),
        headers_json: c_string(&delivery.headers_json()),
        delivery,
    };
    Box::into_raw(Box::new(message))
}

/// The library version, for example "0.1.0". Static; do not free.
#[unsafe(no_mangle)]
pub extern "C" fn firemq_version() -> *const c_char {
    concat!(env!("CARGO_PKG_VERSION"), "\0").as_ptr().cast()
}

/// The message of the last failed call on this thread, or NULL when the last call
/// succeeded. Borrowed: valid until the next FireMQ call on this thread.
#[unsafe(no_mangle)]
pub extern "C" fn firemq_last_error() -> *const c_char {
    LAST_ERROR.with(|last| {
        last.borrow()
            .as_ref()
            .map_or(std::ptr::null(), |e| e.message.as_ptr())
    })
}

/// The AMQP reply code of the last failed call on this thread, for example 403, 404, 406 or
/// 312 (unroutable), or 0 when there is none.
#[unsafe(no_mangle)]
pub extern "C" fn firemq_last_error_code() -> u16 {
    LAST_ERROR.with(|last| last.borrow().as_ref().map_or(0, |e| e.code))
}

/// The status of the last call on this thread: `FIREMQ_OK` or an error status.
#[unsafe(no_mangle)]
pub extern "C" fn firemq_last_error_status() -> i32 {
    LAST_ERROR.with(|last| last.borrow().as_ref().map_or(FIREMQ_OK, |e| e.status))
}

/// Connects with a URL such as `fire://guest:guest@localhost:5673/vhost`. On success writes
/// a new client to `*out_client`; release it with `firemq_free`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn firemq_connect(
    url: *const c_char,
    out_client: *mut *mut FiremqClient,
) -> i32 {
    call(|| {
        if out_client.is_null() {
            return Err(invalid("out_client must not be NULL"));
        }
        let url = unsafe { required(url, "url") }?;
        let inner = runtime().block_on(firemq_client::Client::connect(url))?;
        let client = FiremqClient(Client { inner });
        unsafe { put(out_client, Box::into_raw(Box::new(client))) };
        Ok(())
    })
}

/// Closes the connection gracefully. Unacknowledged messages return to their queues. The
/// handle stays allocated until `firemq_free`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn firemq_close(client: *const FiremqClient) -> i32 {
    call(|| unsafe { handle(client, "client") }?.0.close())
}

/// Releases a client. The connection closes once its subscriptions and messages are freed
/// too. Accepts NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn firemq_free(client: *mut FiremqClient) {
    if !client.is_null() {
        // SAFETY: the handle came from `firemq_connect` and is freed once.
        drop(unsafe { Box::from_raw(client) });
    }
}

/// Declares a queue. An empty name asks the broker to generate one. `args_json` (optional)
/// holds queue arguments such as `{"x-max-length": 1000}`. The out-pointers are optional;
/// a name written to `*out_name` is owned by the caller (`firemq_string_free`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn firemq_declare_queue(
    client: *const FiremqClient,
    name: *const c_char,
    durable: bool,
    exclusive: bool,
    auto_delete: bool,
    args_json: *const c_char,
    out_name: *mut *mut c_char,
    out_messages: *mut u64,
    out_consumers: *mut u32,
) -> i32 {
    call(|| {
        let client = unsafe { handle(client, "client") }?;
        let name = unsafe { required(name, "name") }?;
        let args = unsafe { optional(args_json, "args_json") }?;
        let q = client.0.declare_queue(
            name.into(),
            durable,
            exclusive,
            auto_delete,
            args.map(String::from),
        )?;
        unsafe {
            put(out_name, c_string(&q.name).into_raw());
            put(out_messages, q.messages);
            put(out_consumers, q.consumers);
        }
        Ok(())
    })
}

/// Deletes a queue. `*out_messages` (optional) receives how many messages it held.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn firemq_delete_queue(
    client: *const FiremqClient,
    name: *const c_char,
    out_messages: *mut u64,
) -> i32 {
    call(|| {
        let client = unsafe { handle(client, "client") }?;
        let name = unsafe { required(name, "name") }?;
        let n = client.0.delete_queue(name.into())?;
        unsafe { put(out_messages, n) };
        Ok(())
    })
}

/// Removes every ready message from a queue. `*out_messages` (optional) receives how many.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn firemq_purge_queue(
    client: *const FiremqClient,
    name: *const c_char,
    out_messages: *mut u64,
) -> i32 {
    call(|| {
        let client = unsafe { handle(client, "client") }?;
        let name = unsafe { required(name, "name") }?;
        let n = client.0.purge_queue(name.into())?;
        unsafe { put(out_messages, n) };
        Ok(())
    })
}

/// Declares an exchange of type `direct`, `fanout`, `topic` or `headers`. `args_json` is
/// optional.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn firemq_declare_exchange(
    client: *const FiremqClient,
    name: *const c_char,
    kind: *const c_char,
    durable: bool,
    auto_delete: bool,
    args_json: *const c_char,
) -> i32 {
    call(|| {
        let client = unsafe { handle(client, "client") }?;
        let name = unsafe { required(name, "name") }?;
        let kind = unsafe { required(kind, "kind") }?;
        let args = unsafe { optional(args_json, "args_json") }?;
        client.0.declare_exchange(
            name.into(),
            kind.into(),
            durable,
            auto_delete,
            args.map(String::from),
        )
    })
}

/// Deletes an exchange; with `if_unused` only when nothing is bound to it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn firemq_delete_exchange(
    client: *const FiremqClient,
    name: *const c_char,
    if_unused: bool,
) -> i32 {
    call(|| {
        let client = unsafe { handle(client, "client") }?;
        let name = unsafe { required(name, "name") }?;
        client.0.delete_exchange(name.into(), if_unused)
    })
}

/// Binds `queue` to `exchange` with a routing key or pattern. `args_json` is optional.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn firemq_bind_queue(
    client: *const FiremqClient,
    queue: *const c_char,
    exchange: *const c_char,
    key: *const c_char,
    args_json: *const c_char,
) -> i32 {
    call(|| {
        let client = unsafe { handle(client, "client") }?;
        let queue = unsafe { required(queue, "queue") }?;
        let exchange = unsafe { required(exchange, "exchange") }?;
        let key = unsafe { required(key, "key") }?;
        let args = unsafe { optional(args_json, "args_json") }?;
        client.0.bind_queue(
            queue.into(),
            exchange.into(),
            key.into(),
            args.map(String::from),
        )
    })
}

/// Removes a queue binding.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn firemq_unbind_queue(
    client: *const FiremqClient,
    queue: *const c_char,
    exchange: *const c_char,
    key: *const c_char,
) -> i32 {
    call(|| {
        let client = unsafe { handle(client, "client") }?;
        let queue = unsafe { required(queue, "queue") }?;
        let exchange = unsafe { required(exchange, "exchange") }?;
        let key = unsafe { required(key, "key") }?;
        client
            .0
            .unbind_queue(queue.into(), exchange.into(), key.into())
    })
}

/// Binds exchange `destination` to exchange `source`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn firemq_bind_exchange(
    client: *const FiremqClient,
    destination: *const c_char,
    source: *const c_char,
    key: *const c_char,
) -> i32 {
    call(|| {
        let client = unsafe { handle(client, "client") }?;
        let destination = unsafe { required(destination, "destination") }?;
        let source = unsafe { required(source, "source") }?;
        let key = unsafe { required(key, "key") }?;
        client
            .0
            .bind_exchange(destination.into(), source.into(), key.into())
    })
}

/// The default publish options: persistent, confirmed, not mandatory, no properties or
/// headers.
#[unsafe(no_mangle)]
pub extern "C" fn firemq_publish_options_default() -> FiremqPublishOptions {
    FiremqPublishOptions {
        persistent: true,
        mandatory: false,
        confirm: true,
        properties_json: std::ptr::null(),
        headers_json: std::ptr::null(),
    }
}

/// Publishes `body_len` bytes from `body` (which may be NULL when `body_len` is 0). Use
/// exchange "" to send straight to the queue named by `key`. `options` may be NULL for the
/// defaults, which wait for the broker's confirm.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn firemq_publish(
    client: *const FiremqClient,
    exchange: *const c_char,
    key: *const c_char,
    body: *const u8,
    body_len: usize,
    options: *const FiremqPublishOptions,
) -> i32 {
    call(|| {
        let client = unsafe { handle(client, "client") }?;
        let exchange = unsafe { required(exchange, "exchange") }?;
        let key = unsafe { required(key, "key") }?;
        let body = match (body.is_null(), body_len) {
            (_, 0) => Vec::new(),
            (true, _) => return Err(invalid("body must not be NULL")),
            // SAFETY: the caller passes `body_len` readable bytes.
            (false, n) => unsafe { std::slice::from_raw_parts(body, n) }.to_vec(),
        };
        let o = unsafe { options.as_ref() }
            .copied()
            .unwrap_or_else(|| firemq_publish_options_default());
        let props = unsafe { optional(o.properties_json, "properties_json") }?;
        let headers = unsafe { optional(o.headers_json, "headers_json") }?;
        let options = PublishOptions {
            persistent: o.persistent,
            mandatory: o.mandatory,
            confirm: o.confirm,
            properties: MessageProperties::from(&json::properties_from_json(props)?),
            headers_json: headers.map(String::from),
        };
        client.0.publish(exchange.into(), key.into(), body, options)
    })
}

/// Starts consuming from a queue. On success writes a subscription to `*out_subscription`;
/// release it with `firemq_subscription_free`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn firemq_subscribe(
    client: *const FiremqClient,
    queue: *const c_char,
    prefetch: u32,
    no_ack: bool,
    out_subscription: *mut *mut FiremqSubscription,
) -> i32 {
    call(|| {
        if out_subscription.is_null() {
            return Err(invalid("out_subscription must not be NULL"));
        }
        let client = unsafe { handle(client, "client") }?;
        let queue = unsafe { required(queue, "queue") }?;
        let sub = client.0.subscribe(queue.into(), prefetch, no_ack)?;
        unsafe {
            put(
                out_subscription,
                Box::into_raw(Box::new(FiremqSubscription(sub))),
            )
        };
        Ok(())
    })
}

/// Waits up to `timeout_ms` for the next message. Writes the message, or NULL on timeout, to
/// `*out_message`; release a message with `firemq_message_free`. Returns `FIREMQ_ERR_CLOSED`
/// once the subscription has ended.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn firemq_next(
    subscription: *const FiremqSubscription,
    timeout_ms: u64,
    out_message: *mut *mut FiremqMessage,
) -> i32 {
    call(|| {
        if out_message.is_null() {
            return Err(invalid("out_message must not be NULL"));
        }
        unsafe { put(out_message, std::ptr::null_mut()) };
        let sub = unsafe { handle(subscription, "subscription") }?;
        if let Some(d) = sub.0.next(timeout_ms)? {
            unsafe { put(out_message, message_handle(d)) };
        }
        Ok(())
    })
}

/// Stops a subscription. Messages already received can still be acknowledged.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn firemq_subscription_cancel(
    subscription: *const FiremqSubscription,
) -> i32 {
    call(|| unsafe { handle(subscription, "subscription") }?.0.cancel())
}

/// Releases a subscription, cancelling it if it is still active. Accepts NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn firemq_subscription_free(subscription: *mut FiremqSubscription) {
    if !subscription.is_null() {
        // SAFETY: the handle came from `firemq_subscribe` and is freed once.
        drop(unsafe { Box::from_raw(subscription) });
    }
}

/// Takes one message from a queue. Writes the message, or NULL when the queue is empty, to
/// `*out_message`; release a message with `firemq_message_free`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn firemq_get(
    client: *const FiremqClient,
    queue: *const c_char,
    no_ack: bool,
    out_message: *mut *mut FiremqMessage,
) -> i32 {
    call(|| {
        if out_message.is_null() {
            return Err(invalid("out_message must not be NULL"));
        }
        unsafe { put(out_message, std::ptr::null_mut()) };
        let client = unsafe { handle(client, "client") }?;
        let queue = unsafe { required(queue, "queue") }?;
        if let Some(d) = client.0.get(queue.into(), no_ack)? {
            unsafe { put(out_message, message_handle(d)) };
        }
        Ok(())
    })
}

/// Acknowledges a message. Does nothing when it was already settled or consumed with
/// `no_ack`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn firemq_ack(message: *const FiremqMessage) -> i32 {
    call(|| unsafe { handle(message, "message") }?.delivery.ack())
}

/// Rejects a message. With `requeue` it is delivered again; otherwise it is dead-lettered
/// when the queue has a dead-letter exchange, and dropped if not.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn firemq_nack(message: *const FiremqMessage, requeue: bool) -> i32 {
    call(|| {
        unsafe { handle(message, "message") }?
            .delivery
            .nack(requeue)
    })
}

/// The message body. Borrowed from the message; `*out_len` receives its length.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn firemq_message_body(
    message: *const FiremqMessage,
    out_len: *mut usize,
) -> *const u8 {
    match unsafe { message.as_ref() } {
        Some(m) => {
            let body = &m.delivery.inner.body;
            unsafe { put(out_len, body.len()) };
            body.as_ptr()
        }
        None => {
            unsafe { put(out_len, 0) };
            std::ptr::null()
        }
    }
}

/// A copy of the message body that outlives the message. Owned by the caller: release it
/// with `firemq_buffer_free(buffer, *out_len)`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn firemq_message_body_copy(
    message: *const FiremqMessage,
    out_len: *mut usize,
) -> *mut u8 {
    let body: Box<[u8]> = match unsafe { message.as_ref() } {
        Some(m) => m.delivery.inner.body.to_vec().into_boxed_slice(),
        None => Box::default(),
    };
    unsafe { put(out_len, body.len()) };
    Box::into_raw(body).cast()
}

/// The exchange the message was published to. Borrowed from the message.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn firemq_message_exchange(message: *const FiremqMessage) -> *const c_char {
    unsafe { message.as_ref() }.map_or(std::ptr::null(), |m| m.exchange.as_ptr())
}

/// The routing key the message was published with. Borrowed from the message.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn firemq_message_routing_key(
    message: *const FiremqMessage,
) -> *const c_char {
    unsafe { message.as_ref() }.map_or(std::ptr::null(), |m| m.routing_key.as_ptr())
}

/// True when the message was delivered before and not acknowledged.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn firemq_message_redelivered(message: *const FiremqMessage) -> bool {
    unsafe { message.as_ref() }.is_some_and(|m| m.delivery.inner.redelivered)
}

/// The delivery tag, unique per connection.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn firemq_message_delivery_tag(message: *const FiremqMessage) -> u64 {
    unsafe { message.as_ref() }.map_or(0, |m| m.delivery.inner.delivery_tag)
}

/// The properties as a JSON object with the protocol's keys, for example
/// `{"content_type":"text/plain"}`. Borrowed from the message.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn firemq_message_properties_json(
    message: *const FiremqMessage,
) -> *const c_char {
    unsafe { message.as_ref() }.map_or(std::ptr::null(), |m| m.properties_json.as_ptr())
}

/// The application headers as a JSON object. Binary values are base64-encoded strings.
/// Borrowed from the message.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn firemq_message_headers_json(
    message: *const FiremqMessage,
) -> *const c_char {
    unsafe { message.as_ref() }.map_or(std::ptr::null(), |m| m.headers_json.as_ptr())
}

/// Releases a message. An unacknowledged message stays unacknowledged until its connection
/// closes. Accepts NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn firemq_message_free(message: *mut FiremqMessage) {
    if !message.is_null() {
        // SAFETY: the handle came from `firemq_next` or `firemq_get` and is freed once.
        drop(unsafe { Box::from_raw(message) });
    }
}

/// Releases a string returned by this library. Accepts NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn firemq_string_free(s: *mut c_char) {
    if !s.is_null() {
        // SAFETY: the string came from `CString::into_raw` in this library.
        drop(unsafe { CString::from_raw(s) });
    }
}

/// Releases a buffer returned by this library, passing the length it was returned with.
/// Accepts NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn firemq_buffer_free(buffer: *mut u8, len: usize) {
    if !buffer.is_null() {
        // SAFETY: the buffer came from a boxed slice of `len` bytes in this library.
        drop(unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(buffer, len)) });
    }
}
