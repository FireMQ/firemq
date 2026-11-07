//! Property tests: the decoder must never panic, and valid data must round-trip.

use bytes::{Bytes, BytesMut};
use firemq_amqp_codec::{FieldTable, Method, Value, decode_frame};
use proptest::prelude::*;

fn value() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        any::<bool>().prop_map(Value::Bool),
        any::<i8>().prop_map(Value::I8),
        any::<u8>().prop_map(Value::U8),
        any::<i16>().prop_map(Value::I16),
        any::<u16>().prop_map(Value::U16),
        any::<i32>().prop_map(Value::I32),
        any::<u32>().prop_map(Value::U32),
        any::<i64>().prop_map(Value::I64),
        any::<u64>().prop_map(Value::Timestamp),
        ".{0,20}".prop_map(Value::string),
        proptest::collection::vec(any::<u8>(), 0..16)
            .prop_map(|v| Value::ByteArray(Bytes::from(v))),
        Just(Value::Void),
    ];
    leaf.prop_recursive(3, 32, 4, |inner| {
        prop_oneof![
            proptest::collection::vec(inner.clone(), 0..4).prop_map(Value::Array),
            proptest::collection::vec(("[a-z]{1,8}", inner), 0..4)
                .prop_map(|kv| Value::Table(FieldTable(kv))),
        ]
    })
}

proptest! {
    #[test]
    fn decoding_random_bytes_never_panics(data in proptest::collection::vec(any::<u8>(), 0..512)) {
        let mut buf = BytesMut::from(&data[..]);
        let _ = decode_frame(&mut buf, 131072);
        let _ = Method::decode(Bytes::from(data.clone()));
        let _ = FieldTable::decode(Bytes::from(data));
    }

    #[test]
    fn tables_round_trip(entries in proptest::collection::vec(("[a-zA-Z0-9-]{1,16}", value()), 0..8)) {
        let table = FieldTable(entries);
        let mut out = BytesMut::new();
        table.write(&mut out);
        prop_assert_eq!(FieldTable::decode(out.freeze()).unwrap(), table);
    }
}
