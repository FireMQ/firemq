use std::ffi::{CStr, CString};
use std::ptr;

use super::*;
use crate::c::*;

/// Starts an in-memory broker on the bindings' runtime and returns it with its URL.
fn broker() -> (firemq::Server, String) {
    let server = runtime()
        .block_on(firemq::builder().ephemeral().random_ports().start())
        .expect("broker starts");
    let port = server.fire_addr().expect("fire listener").port();
    (server, format!("fire://guest:guest@127.0.0.1:{port}"))
}

#[test]
fn publish_subscribe_ack() {
    let (_server, url) = broker();
    let client = Client::connect(url).unwrap();
    let q = client
        .declare_queue("ffi.basic".into(), true, false, false, None)
        .unwrap();
    assert_eq!(q.name, "ffi.basic");

    let options = PublishOptions {
        properties: MessageProperties {
            content_type: Some("text/plain".into()),
            message_type: Some("greeting".into()),
            priority: Some(4),
            ..Default::default()
        },
        headers_json: Some(r#"{"trace":"abc","n":5,"f":1.5,"l":[1,"two"]}"#.into()),
        ..Default::default()
    };
    client
        .publish("".into(), q.name.clone(), b"hello".to_vec(), options)
        .unwrap();

    let sub = client.subscribe(q.name.clone(), 10, false).unwrap();
    let d = sub.next(3000).unwrap().expect("a delivery");
    assert_eq!(d.body(), b"hello");
    assert_eq!(d.exchange(), "");
    assert_eq!(d.routing_key(), "ffi.basic");
    assert!(!d.redelivered());
    assert_eq!(d.properties().content_type.as_deref(), Some("text/plain"));
    assert_eq!(d.properties().message_type.as_deref(), Some("greeting"));
    assert_eq!(d.properties().priority, Some(4));
    let headers: serde_json::Value = serde_json::from_str(&d.headers_json()).unwrap();
    assert_eq!(
        headers,
        serde_json::json!({"trace": "abc", "n": 5, "f": 1.5, "l": [1, "two"]})
    );
    d.ack().unwrap();
    assert!(sub.next(50).unwrap().is_none(), "the queue is empty");

    sub.cancel().unwrap();
    assert!(matches!(sub.next(10), Err(FireMqError::Closed { .. })));
    client.close().unwrap();
    assert!(client.is_closed());
}

#[test]
fn get_nack_and_counts() {
    let (_server, url) = broker();
    let client = Client::connect(url).unwrap();
    let q = client
        .declare_queue("".into(), false, true, true, None)
        .unwrap();
    assert!(q.name.starts_with("amq.gen-"));
    assert!(client.get(q.name.clone(), false).unwrap().is_none());

    client
        .publish(
            "".into(),
            q.name.clone(),
            b"again".to_vec(),
            Default::default(),
        )
        .unwrap();
    let d = client.get(q.name.clone(), false).unwrap().unwrap();
    d.nack(true).unwrap();
    let d = client.get(q.name.clone(), true).unwrap().unwrap();
    assert!(d.redelivered());
    d.ack().unwrap(); // a no-op for no_ack messages

    for _ in 0..3 {
        client
            .publish("".into(), q.name.clone(), vec![], Default::default())
            .unwrap();
    }
    assert_eq!(client.purge_queue(q.name.clone()).unwrap(), 3);
    client
        .publish("".into(), q.name.clone(), vec![], Default::default())
        .unwrap();
    assert_eq!(client.delete_queue(q.name).unwrap(), 1);
}

#[test]
fn exchanges_and_errors() {
    let (_server, url) = broker();
    let wrong = url.replace("guest:guest", "guest:nope");
    match Client::connect(wrong) {
        Err(FireMqError::Server { code: 403, .. }) => {}
        Err(other) => panic!("expected 403, got {other:?}"),
        Ok(_) => panic!("connected with a wrong password"),
    }

    let client = Client::connect(url).unwrap();
    client
        .declare_exchange("ffi.up".into(), "direct".into(), true, false, None)
        .unwrap();
    client
        .declare_exchange("ffi.down".into(), "fanout".into(), true, false, None)
        .unwrap();
    client
        .declare_queue("ffi.q".into(), true, false, false, None)
        .unwrap();
    client
        .bind_queue("ffi.q".into(), "ffi.down".into(), "".into(), None)
        .unwrap();
    client
        .bind_exchange("ffi.down".into(), "ffi.up".into(), "audit".into())
        .unwrap();
    client
        .publish(
            "ffi.up".into(),
            "audit".into(),
            b"yes".to_vec(),
            Default::default(),
        )
        .unwrap();
    let d = client.get("ffi.q".into(), true).unwrap().unwrap();
    assert_eq!(d.body(), b"yes");

    let mandatory = PublishOptions {
        mandatory: true,
        ..Default::default()
    };
    let e = client
        .publish("".into(), "ffi.nowhere".into(), vec![], mandatory)
        .unwrap_err();
    assert!(matches!(e, FireMqError::Unroutable { .. }), "{e:?}");
    assert_eq!(e.code(), 312);

    let e = client
        .declare_queue("ffi.q".into(), false, false, false, None)
        .unwrap_err();
    assert!(matches!(e, FireMqError::Server { code: 406, .. }), "{e:?}");
    let e = client
        .bind_queue("ffi.q".into(), "ffi.missing".into(), "".into(), None)
        .unwrap_err();
    assert!(matches!(e, FireMqError::Server { code: 404, .. }), "{e:?}");
    let e = client
        .declare_queue("ffi.bad".into(), true, false, false, Some("[]".into()))
        .unwrap_err();
    assert!(matches!(e, FireMqError::Other { .. }), "{e:?}");

    client
        .unbind_queue("ffi.q".into(), "ffi.down".into(), "".into())
        .unwrap();
    client.delete_exchange("ffi.up".into(), false).unwrap();

    client
        .declare_queue(
            "ffi.full".into(),
            true,
            false,
            false,
            Some(r#"{"x-max-length": 1, "x-overflow": "reject-publish"}"#.into()),
        )
        .unwrap();
    client
        .publish("".into(), "ffi.full".into(), vec![1], Default::default())
        .unwrap();
    let e = client
        .publish("".into(), "ffi.full".into(), vec![2], Default::default())
        .unwrap_err();
    assert!(matches!(e, FireMqError::Nacked { .. }), "{e:?}");

    client.close().unwrap();
    let e = client.purge_queue("ffi.q".into()).unwrap_err();
    assert!(matches!(e, FireMqError::Closed { .. }), "{e:?}");
}

fn cstr(s: &str) -> CString {
    CString::new(s).unwrap()
}

fn last_error() -> String {
    let p = firemq_last_error();
    assert!(!p.is_null());
    unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned()
}

#[test]
fn c_abi_round_trip() {
    let (_server, url) = broker();
    unsafe {
        let mut client = ptr::null_mut();
        let bad = cstr(&url.replace("guest:guest", "guest:nope"));
        assert_eq!(firemq_connect(bad.as_ptr(), &mut client), FIREMQ_ERR_SERVER);
        assert_eq!(firemq_last_error_code(), 403);
        assert!(client.is_null());
        assert!(!last_error().is_empty());

        let url = cstr(&url);
        assert_eq!(firemq_connect(url.as_ptr(), &mut client), FIREMQ_OK);
        assert!(firemq_last_error().is_null());

        let mut name = ptr::null_mut();
        let mut messages = 99;
        let rc = firemq_declare_queue(
            client,
            cstr("").as_ptr(),
            false,
            true,
            false,
            ptr::null(),
            &mut name,
            &mut messages,
            ptr::null_mut(),
        );
        assert_eq!(rc, FIREMQ_OK);
        assert_eq!(messages, 0);
        let queue = CStr::from_ptr(name).to_owned();
        firemq_string_free(name);

        let headers = cstr(r#"{"trace":"abc"}"#);
        let props = cstr(r#"{"content_type":"text/plain"}"#);
        let mut options = firemq_publish_options_default();
        options.headers_json = headers.as_ptr();
        options.properties_json = props.as_ptr();
        let body = b"hello from C";
        let rc = firemq_publish(
            client,
            cstr("").as_ptr(),
            queue.as_ptr(),
            body.as_ptr(),
            body.len(),
            &options,
        );
        assert_eq!(rc, FIREMQ_OK);

        let mut sub = ptr::null_mut();
        assert_eq!(
            firemq_subscribe(client, queue.as_ptr(), 10, false, &mut sub),
            FIREMQ_OK
        );
        let mut msg = ptr::null_mut();
        assert_eq!(firemq_next(sub, 3000, &mut msg), FIREMQ_OK);
        assert!(!msg.is_null());
        let mut len = 0;
        let data = firemq_message_body(msg, &mut len);
        assert_eq!(std::slice::from_raw_parts(data, len), body);
        let copy = firemq_message_body_copy(msg, &mut len);
        assert_eq!(std::slice::from_raw_parts(copy, len), body);
        firemq_buffer_free(copy, len);
        assert_eq!(
            CStr::from_ptr(firemq_message_routing_key(msg)),
            queue.as_c_str()
        );
        assert_eq!(CStr::from_ptr(firemq_message_exchange(msg)), c"");
        assert_eq!(
            CStr::from_ptr(firemq_message_headers_json(msg)),
            cr#"{"trace":"abc"}"#
        );
        assert_eq!(
            CStr::from_ptr(firemq_message_properties_json(msg)),
            cr#"{"content_type":"text/plain"}"#
        );
        assert!(!firemq_message_redelivered(msg));
        assert!(firemq_message_delivery_tag(msg) > 0);
        assert_eq!(firemq_ack(msg), FIREMQ_OK);
        firemq_message_free(msg);

        let mut msg = ptr::null_mut();
        assert_eq!(firemq_next(sub, 50, &mut msg), FIREMQ_OK);
        assert!(msg.is_null(), "timeout gives NULL");
        assert_eq!(firemq_subscription_cancel(sub), FIREMQ_OK);
        assert_eq!(firemq_next(sub, 50, &mut msg), FIREMQ_ERR_CLOSED);
        firemq_subscription_free(sub);

        assert_eq!(
            firemq_publish(
                client,
                cstr("").as_ptr(),
                queue.as_ptr(),
                ptr::null(),
                0,
                ptr::null()
            ),
            FIREMQ_OK
        );
        let mut msg = ptr::null_mut();
        let rc = firemq_get(client, queue.as_ptr(), false, &mut msg);
        assert_eq!(rc, FIREMQ_OK, "{}", last_error());
        assert!(!msg.is_null());
        assert_eq!(
            firemq_message_body(msg, &mut len),
            firemq_message_body(msg, ptr::null_mut())
        );
        assert_eq!(len, 0);
        assert_eq!(firemq_nack(msg, false), FIREMQ_OK);
        firemq_message_free(msg);

        let mut options = firemq_publish_options_default();
        options.mandatory = true;
        let rc = firemq_publish(
            client,
            cstr("").as_ptr(),
            cstr("nowhere").as_ptr(),
            ptr::null(),
            0,
            &options,
        );
        assert_eq!(rc, FIREMQ_ERR_UNROUTABLE);
        assert_eq!(firemq_last_error_code(), 312);

        assert_eq!(
            firemq_bind_queue(client, ptr::null(), ptr::null(), ptr::null(), ptr::null()),
            FIREMQ_ERR_OTHER
        );
        assert!(last_error().contains("NULL"), "{}", last_error());

        assert_eq!(firemq_close(client), FIREMQ_OK);
        let mut msg = ptr::null_mut();
        assert_eq!(
            firemq_get(client, queue.as_ptr(), true, &mut msg),
            FIREMQ_ERR_CLOSED
        );
        firemq_free(client);
        firemq_free(ptr::null_mut());
        firemq_message_free(ptr::null_mut());
        firemq_string_free(ptr::null_mut());
    }
    assert_eq!(
        unsafe { CStr::from_ptr(firemq_version()) }
            .to_str()
            .unwrap(),
        env!("CARGO_PKG_VERSION")
    );
}
