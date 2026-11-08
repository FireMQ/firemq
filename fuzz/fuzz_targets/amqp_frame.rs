#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut buf = BytesMut::from(data);
    while let Ok(Some(frame)) = firemq_amqp_codec::decode_frame(&mut buf, 131_072) {
        // Whatever decodes must encode again without panicking.
        let mut out = BytesMut::new();
        firemq_amqp_codec::encode_frame(&mut out, &frame);
    }
});
