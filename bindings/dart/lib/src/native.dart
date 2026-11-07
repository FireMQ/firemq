// dart:ffi declarations for the C API in bindings/c/firemq.h.

import 'dart:ffi';
import 'dart:io';

import 'package:ffi/ffi.dart';

final class FiremqClient extends Opaque {}

final class FiremqSubscription extends Opaque {}

final class FiremqMessage extends Opaque {}

final class FiremqPublishOptions extends Struct {
  @Bool()
  external bool persistent;
  @Bool()
  external bool mandatory;
  @Bool()
  external bool confirm;
  external Pointer<Utf8> propertiesJson;
  external Pointer<Utf8> headersJson;
}

const firemqOk = 0;
const firemqErrServer = 1;
const firemqErrUnroutable = 2;
const firemqErrNacked = 3;
const firemqErrClosed = 4;
const firemqErrOther = 5;

/// Opens libfiremq_client_ffi: the path in `FIREMQ_LIB`, or the platform's library name on
/// the loader's search path.
DynamicLibrary _open() {
  final path = Platform.environment['FIREMQ_LIB'];
  if (path != null && path.isNotEmpty) return DynamicLibrary.open(path);
  if (Platform.isMacOS || Platform.isIOS) {
    return DynamicLibrary.open('libfiremq_client_ffi.dylib');
  }
  if (Platform.isWindows) return DynamicLibrary.open('firemq_client_ffi.dll');
  return DynamicLibrary.open('libfiremq_client_ffi.so');
}

final DynamicLibrary _lib = _open();

typedef _Str = Pointer<Utf8>;
typedef _C = Pointer<FiremqClient>;
typedef _S = Pointer<FiremqSubscription>;
typedef _M = Pointer<FiremqMessage>;

final lastError = _lib.lookupFunction<_Str Function(), _Str Function()>('firemq_last_error');
final lastErrorCode =
    _lib.lookupFunction<Uint16 Function(), int Function()>('firemq_last_error_code');
final version = _lib.lookupFunction<_Str Function(), _Str Function()>('firemq_version');

final connect = _lib.lookupFunction<Int32 Function(_Str, Pointer<_C>),
    int Function(_Str, Pointer<_C>)>('firemq_connect');
final close = _lib.lookupFunction<Int32 Function(_C), int Function(_C)>('firemq_close');

final declareQueue = _lib.lookupFunction<
    Int32 Function(_C, _Str, Bool, Bool, Bool, _Str, Pointer<_Str>, Pointer<Uint64>,
        Pointer<Uint32>),
    int Function(_C, _Str, bool, bool, bool, _Str, Pointer<_Str>, Pointer<Uint64>,
        Pointer<Uint32>)>('firemq_declare_queue');
final deleteQueue = _lib.lookupFunction<Int32 Function(_C, _Str, Pointer<Uint64>),
    int Function(_C, _Str, Pointer<Uint64>)>('firemq_delete_queue');
final purgeQueue = _lib.lookupFunction<Int32 Function(_C, _Str, Pointer<Uint64>),
    int Function(_C, _Str, Pointer<Uint64>)>('firemq_purge_queue');
final declareExchange = _lib.lookupFunction<Int32 Function(_C, _Str, _Str, Bool, Bool, _Str),
    int Function(_C, _Str, _Str, bool, bool, _Str)>('firemq_declare_exchange');
final deleteExchange = _lib.lookupFunction<Int32 Function(_C, _Str, Bool),
    int Function(_C, _Str, bool)>('firemq_delete_exchange');
final bindQueue = _lib.lookupFunction<Int32 Function(_C, _Str, _Str, _Str, _Str),
    int Function(_C, _Str, _Str, _Str, _Str)>('firemq_bind_queue');
final unbindQueue = _lib.lookupFunction<Int32 Function(_C, _Str, _Str, _Str),
    int Function(_C, _Str, _Str, _Str)>('firemq_unbind_queue');
final bindExchange = _lib.lookupFunction<Int32 Function(_C, _Str, _Str, _Str),
    int Function(_C, _Str, _Str, _Str)>('firemq_bind_exchange');

final publish = _lib.lookupFunction<
    Int32 Function(_C, _Str, _Str, Pointer<Uint8>, Size, Pointer<FiremqPublishOptions>),
    int Function(_C, _Str, _Str, Pointer<Uint8>, int,
        Pointer<FiremqPublishOptions>)>('firemq_publish');

final subscribe = _lib.lookupFunction<Int32 Function(_C, _Str, Uint32, Bool, Pointer<_S>),
    int Function(_C, _Str, int, bool, Pointer<_S>)>('firemq_subscribe');
final next = _lib.lookupFunction<Int32 Function(_S, Uint64, Pointer<_M>),
    int Function(_S, int, Pointer<_M>)>('firemq_next');
final cancel =
    _lib.lookupFunction<Int32 Function(_S), int Function(_S)>('firemq_subscription_cancel');
final get = _lib.lookupFunction<Int32 Function(_C, _Str, Bool, Pointer<_M>),
    int Function(_C, _Str, bool, Pointer<_M>)>('firemq_get');

final ack = _lib.lookupFunction<Int32 Function(_M), int Function(_M)>('firemq_ack');
final nack = _lib.lookupFunction<Int32 Function(_M, Bool), int Function(_M, bool)>('firemq_nack');
final messageBody = _lib.lookupFunction<Pointer<Uint8> Function(_M, Pointer<Size>),
    Pointer<Uint8> Function(_M, Pointer<Size>)>('firemq_message_body');
final messageExchange =
    _lib.lookupFunction<_Str Function(_M), _Str Function(_M)>('firemq_message_exchange');
final messageRoutingKey =
    _lib.lookupFunction<_Str Function(_M), _Str Function(_M)>('firemq_message_routing_key');
final messageRedelivered =
    _lib.lookupFunction<Bool Function(_M), bool Function(_M)>('firemq_message_redelivered');
final messageDeliveryTag =
    _lib.lookupFunction<Uint64 Function(_M), int Function(_M)>('firemq_message_delivery_tag');
final messagePropertiesJson =
    _lib.lookupFunction<_Str Function(_M), _Str Function(_M)>('firemq_message_properties_json');
final messageHeadersJson =
    _lib.lookupFunction<_Str Function(_M), _Str Function(_M)>('firemq_message_headers_json');

final stringFree =
    _lib.lookupFunction<Void Function(_Str), void Function(_Str)>('firemq_string_free');

final freeClient = _lib.lookup<NativeFinalizerFunction>('firemq_free');
final freeSubscription = _lib.lookup<NativeFinalizerFunction>('firemq_subscription_free');
final freeMessage = _lib.lookup<NativeFinalizerFunction>('firemq_message_free');

final clientFree = _lib.lookupFunction<Void Function(_C), void Function(_C)>('firemq_free');
final subscriptionFree =
    _lib.lookupFunction<Void Function(_S), void Function(_S)>('firemq_subscription_free');
final messageFree =
    _lib.lookupFunction<Void Function(_M), void Function(_M)>('firemq_message_free');
