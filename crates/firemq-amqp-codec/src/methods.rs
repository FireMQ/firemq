//! Every AMQP 0-9-1 method RabbitMQ implements, as one declarative table.
//!
//! Each entry names the method, its class and method ids, and its arguments in wire order.
//! `bool` arguments are AMQP `bit`s; consecutive bits share an octet. `String` is a short
//! string, [`LongString`] a long string and [`FieldTable`] a table.

use bytes::{Bytes, BytesMut};

use crate::CodecError;
use crate::rw::{Field, Reader, Writer};
pub use crate::table::{FieldTable, LongString};

macro_rules! methods {
    ($(
        $(#[$doc:meta])*
        $name:ident = ($class:literal, $method:literal) { $($field:ident : $ty:ty),* $(,)? }
    )*) => {
        $(
            $(#[$doc])*
            #[derive(Debug, Clone, Default, PartialEq)]
            pub struct $name { $(pub $field: $ty),* }

            impl From<$name> for Method {
                fn from(m: $name) -> Method { Method::$name(m) }
            }
        )*

        /// A decoded AMQP method.
        #[derive(Debug, Clone, PartialEq)]
        pub enum Method { $($name($name)),* }

        impl Method {
            /// Class and method ids.
            pub fn id(&self) -> (u16, u16) {
                match self { $(Method::$name(_) => ($class, $method)),* }
            }

            /// The method's name as RabbitMQ prints it, for example `queue.declare`.
            pub fn name(&self) -> &'static str {
                match self { $(Method::$name(_) => method_name($class, $method)),* }
            }

            /// Decodes method arguments for the given ids.
            pub fn decode_args(class: u16, method: u16, r: &mut Reader) -> Result<Method, CodecError> {
                match (class, method) {
                    $(($class, $method) => {
                        #[allow(clippy::redundant_field_names)]
                        let m = $name { $($field: <$ty as Field>::read(r)?),* };
                        Ok(Method::$name(m))
                    })*
                    _ => Err(CodecError::UnknownMethod(class, method)),
                }
            }

            /// Encodes class id, method id and arguments.
            pub fn encode(&self, out: &mut BytesMut) {
                let mut w = Writer::new(out);
                match self {
                    $(Method::$name(_m) => {
                        w.u16($class);
                        w.u16($method);
                        $(Field::write(&_m.$field, &mut w);)*
                    })*
                }
                w.flush_bits();
            }
        }
    };
}

impl Method {
    /// Decodes a method frame payload: class id, method id and arguments.
    pub fn decode(payload: Bytes) -> Result<Method, CodecError> {
        let mut r = Reader::new(payload);
        let class = r.u16()?;
        let method = r.u16()?;
        let m = Method::decode_args(class, method, &mut r)?;
        Ok(m)
    }

    /// True for methods followed by a content header and body frames.
    pub fn has_content(&self) -> bool {
        matches!(
            self,
            Method::BasicPublish(_)
                | Method::BasicReturn(_)
                | Method::BasicDeliver(_)
                | Method::BasicGetOk(_)
        )
    }
}

fn method_name(class: u16, method: u16) -> &'static str {
    match (class, method) {
        (10, 10) => "connection.start",
        (10, 11) => "connection.start-ok",
        (10, 20) => "connection.secure",
        (10, 21) => "connection.secure-ok",
        (10, 30) => "connection.tune",
        (10, 31) => "connection.tune-ok",
        (10, 40) => "connection.open",
        (10, 41) => "connection.open-ok",
        (10, 50) => "connection.close",
        (10, 51) => "connection.close-ok",
        (10, 60) => "connection.blocked",
        (10, 61) => "connection.unblocked",
        (10, 70) => "connection.update-secret",
        (10, 71) => "connection.update-secret-ok",
        (20, 10) => "channel.open",
        (20, 11) => "channel.open-ok",
        (20, 20) => "channel.flow",
        (20, 21) => "channel.flow-ok",
        (20, 40) => "channel.close",
        (20, 41) => "channel.close-ok",
        (40, 10) => "exchange.declare",
        (40, 11) => "exchange.declare-ok",
        (40, 20) => "exchange.delete",
        (40, 21) => "exchange.delete-ok",
        (40, 30) => "exchange.bind",
        (40, 31) => "exchange.bind-ok",
        (40, 40) => "exchange.unbind",
        (40, 51) => "exchange.unbind-ok",
        (50, 10) => "queue.declare",
        (50, 11) => "queue.declare-ok",
        (50, 20) => "queue.bind",
        (50, 21) => "queue.bind-ok",
        (50, 30) => "queue.purge",
        (50, 31) => "queue.purge-ok",
        (50, 40) => "queue.delete",
        (50, 41) => "queue.delete-ok",
        (50, 50) => "queue.unbind",
        (50, 51) => "queue.unbind-ok",
        (60, 10) => "basic.qos",
        (60, 11) => "basic.qos-ok",
        (60, 20) => "basic.consume",
        (60, 21) => "basic.consume-ok",
        (60, 30) => "basic.cancel",
        (60, 31) => "basic.cancel-ok",
        (60, 40) => "basic.publish",
        (60, 50) => "basic.return",
        (60, 60) => "basic.deliver",
        (60, 70) => "basic.get",
        (60, 71) => "basic.get-ok",
        (60, 72) => "basic.get-empty",
        (60, 80) => "basic.ack",
        (60, 90) => "basic.reject",
        (60, 100) => "basic.recover-async",
        (60, 110) => "basic.recover",
        (60, 111) => "basic.recover-ok",
        (60, 120) => "basic.nack",
        (85, 10) => "confirm.select",
        (85, 11) => "confirm.select-ok",
        (90, 10) => "tx.select",
        (90, 11) => "tx.select-ok",
        (90, 20) => "tx.commit",
        (90, 21) => "tx.commit-ok",
        (90, 30) => "tx.rollback",
        (90, 31) => "tx.rollback-ok",
        _ => "unknown",
    }
}

methods! {
    ConnectionStart = (10, 10) {
        version_major: u8,
        version_minor: u8,
        server_properties: FieldTable,
        mechanisms: LongString,
        locales: LongString,
    }
    ConnectionStartOk = (10, 11) {
        client_properties: FieldTable,
        mechanism: String,
        response: LongString,
        locale: String,
    }
    ConnectionSecure = (10, 20) { challenge: LongString }
    ConnectionSecureOk = (10, 21) { response: LongString }
    ConnectionTune = (10, 30) { channel_max: u16, frame_max: u32, heartbeat: u16 }
    ConnectionTuneOk = (10, 31) { channel_max: u16, frame_max: u32, heartbeat: u16 }
    ConnectionOpen = (10, 40) { virtual_host: String, capabilities: String, insist: bool }
    ConnectionOpenOk = (10, 41) { known_hosts: String }
    ConnectionClose = (10, 50) { reply_code: u16, reply_text: String, class_id: u16, method_id: u16 }
    ConnectionCloseOk = (10, 51) {}
    ConnectionBlocked = (10, 60) { reason: String }
    ConnectionUnblocked = (10, 61) {}
    ConnectionUpdateSecret = (10, 70) { new_secret: LongString, reason: String }
    ConnectionUpdateSecretOk = (10, 71) {}

    ChannelOpen = (20, 10) { out_of_band: String }
    ChannelOpenOk = (20, 11) { channel_id: LongString }
    ChannelFlow = (20, 20) { active: bool }
    ChannelFlowOk = (20, 21) { active: bool }
    ChannelClose = (20, 40) { reply_code: u16, reply_text: String, class_id: u16, method_id: u16 }
    ChannelCloseOk = (20, 41) {}

    ExchangeDeclare = (40, 10) {
        ticket: u16,
        exchange: String,
        kind: String,
        passive: bool,
        durable: bool,
        auto_delete: bool,
        internal: bool,
        nowait: bool,
        arguments: FieldTable,
    }
    ExchangeDeclareOk = (40, 11) {}
    ExchangeDelete = (40, 20) { ticket: u16, exchange: String, if_unused: bool, nowait: bool }
    ExchangeDeleteOk = (40, 21) {}
    ExchangeBind = (40, 30) {
        ticket: u16,
        destination: String,
        source: String,
        routing_key: String,
        nowait: bool,
        arguments: FieldTable,
    }
    ExchangeBindOk = (40, 31) {}
    ExchangeUnbind = (40, 40) {
        ticket: u16,
        destination: String,
        source: String,
        routing_key: String,
        nowait: bool,
        arguments: FieldTable,
    }
    ExchangeUnbindOk = (40, 51) {}

    QueueDeclare = (50, 10) {
        ticket: u16,
        queue: String,
        passive: bool,
        durable: bool,
        exclusive: bool,
        auto_delete: bool,
        nowait: bool,
        arguments: FieldTable,
    }
    QueueDeclareOk = (50, 11) { queue: String, message_count: u32, consumer_count: u32 }
    QueueBind = (50, 20) {
        ticket: u16,
        queue: String,
        exchange: String,
        routing_key: String,
        nowait: bool,
        arguments: FieldTable,
    }
    QueueBindOk = (50, 21) {}
    QueuePurge = (50, 30) { ticket: u16, queue: String, nowait: bool }
    QueuePurgeOk = (50, 31) { message_count: u32 }
    QueueDelete = (50, 40) { ticket: u16, queue: String, if_unused: bool, if_empty: bool, nowait: bool }
    QueueDeleteOk = (50, 41) { message_count: u32 }
    QueueUnbind = (50, 50) {
        ticket: u16,
        queue: String,
        exchange: String,
        routing_key: String,
        arguments: FieldTable,
    }
    QueueUnbindOk = (50, 51) {}

    BasicQos = (60, 10) { prefetch_size: u32, prefetch_count: u16, global: bool }
    BasicQosOk = (60, 11) {}
    BasicConsume = (60, 20) {
        ticket: u16,
        queue: String,
        consumer_tag: String,
        no_local: bool,
        no_ack: bool,
        exclusive: bool,
        nowait: bool,
        arguments: FieldTable,
    }
    BasicConsumeOk = (60, 21) { consumer_tag: String }
    BasicCancel = (60, 30) { consumer_tag: String, nowait: bool }
    BasicCancelOk = (60, 31) { consumer_tag: String }
    BasicPublish = (60, 40) {
        ticket: u16,
        exchange: String,
        routing_key: String,
        mandatory: bool,
        immediate: bool,
    }
    BasicReturn = (60, 50) { reply_code: u16, reply_text: String, exchange: String, routing_key: String }
    BasicDeliver = (60, 60) {
        consumer_tag: String,
        delivery_tag: u64,
        redelivered: bool,
        exchange: String,
        routing_key: String,
    }
    BasicGet = (60, 70) { ticket: u16, queue: String, no_ack: bool }
    BasicGetOk = (60, 71) {
        delivery_tag: u64,
        redelivered: bool,
        exchange: String,
        routing_key: String,
        message_count: u32,
    }
    BasicGetEmpty = (60, 72) { cluster_id: String }
    BasicAck = (60, 80) { delivery_tag: u64, multiple: bool }
    BasicReject = (60, 90) { delivery_tag: u64, requeue: bool }
    BasicRecoverAsync = (60, 100) { requeue: bool }
    BasicRecover = (60, 110) { requeue: bool }
    BasicRecoverOk = (60, 111) {}
    BasicNack = (60, 120) { delivery_tag: u64, multiple: bool, requeue: bool }

    ConfirmSelect = (85, 10) { nowait: bool }
    ConfirmSelectOk = (85, 11) {}

    TxSelect = (90, 10) {}
    TxSelectOk = (90, 11) {}
    TxCommit = (90, 20) {}
    TxCommitOk = (90, 21) {}
    TxRollback = (90, 30) {}
    TxRollbackOk = (90, 31) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::Value;

    fn round_trip(m: Method) {
        let mut out = BytesMut::new();
        m.encode(&mut out);
        let decoded = Method::decode(out.freeze()).unwrap();
        assert_eq!(decoded, m, "{}", m.name());
    }

    #[test]
    fn bits_pack_into_one_octet() {
        let m = Method::QueueDeclare(QueueDeclare {
            ticket: 0,
            queue: "q".into(),
            passive: false,
            durable: true,
            exclusive: false,
            auto_delete: true,
            nowait: true,
            arguments: FieldTable::new(),
        });
        let mut out = BytesMut::new();
        m.encode(&mut out);
        // class(2) method(2) ticket(2) shortstr(1+1) bits(1) table(4)
        assert_eq!(out.len(), 13);
        assert_eq!(out[8], 0b0001_1010);
        round_trip(m);
    }

    #[test]
    fn representative_methods_round_trip() {
        let mut args = FieldTable::new();
        args.insert("x-message-ttl", Value::I32(1000));
        round_trip(
            ConnectionStart {
                version_major: 0,
                version_minor: 9,
                server_properties: args.clone(),
                mechanisms: "PLAIN AMQPLAIN".into(),
                locales: "en_US".into(),
            }
            .into(),
        );
        round_trip(
            ConnectionStartOk {
                client_properties: args.clone(),
                mechanism: "PLAIN".into(),
                response: LongString::from(b"\0guest\0guest".to_vec()),
                locale: "en_US".into(),
            }
            .into(),
        );
        round_trip(
            ConnectionTune {
                channel_max: 2047,
                frame_max: 131072,
                heartbeat: 60,
            }
            .into(),
        );
        round_trip(
            ConnectionOpen {
                virtual_host: "/".into(),
                capabilities: String::new(),
                insist: false,
            }
            .into(),
        );
        round_trip(
            ExchangeDeclare {
                ticket: 0,
                exchange: "logs".into(),
                kind: "fanout".into(),
                passive: false,
                durable: true,
                auto_delete: false,
                internal: true,
                nowait: false,
                arguments: args.clone(),
            }
            .into(),
        );
        round_trip(
            BasicConsume {
                ticket: 0,
                queue: "q".into(),
                consumer_tag: "ctag".into(),
                no_local: false,
                no_ack: true,
                exclusive: true,
                nowait: false,
                arguments: args.clone(),
            }
            .into(),
        );
        round_trip(
            BasicDeliver {
                consumer_tag: "ctag".into(),
                delivery_tag: u64::MAX,
                redelivered: true,
                exchange: "".into(),
                routing_key: "q".into(),
            }
            .into(),
        );
        round_trip(
            BasicNack {
                delivery_tag: 7,
                multiple: true,
                requeue: false,
            }
            .into(),
        );
        round_trip(ConnectionCloseOk {}.into());
        round_trip(TxCommit {}.into());
        round_trip(ConfirmSelect { nowait: true }.into());
    }

    #[test]
    fn unknown_method_is_reported() {
        let data = Bytes::from_static(&[0, 60, 0, 99]);
        assert_eq!(Method::decode(data), Err(CodecError::UnknownMethod(60, 99)));
    }

    #[test]
    fn truncated_arguments_are_reported() {
        let mut out = BytesMut::new();
        Method::BasicAck(BasicAck {
            delivery_tag: 1,
            multiple: false,
        })
        .encode(&mut out);
        let short = out.freeze().slice(..6);
        assert!(matches!(
            Method::decode(short),
            Err(CodecError::Truncated(_))
        ));
    }

    #[test]
    fn names_follow_rabbitmq_convention() {
        let m: Method = QueueDeclareOk::default().into();
        assert_eq!(m.name(), "queue.declare-ok");
        assert_eq!(m.id(), (50, 11));
    }
}
