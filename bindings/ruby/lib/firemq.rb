# frozen_string_literal: true

# FireMQ client for Ruby, built on the Rust client with UniFFI.
#
#   require "firemq"
#
#   FireMQ.connect("fire://guest:guest@localhost:5673") do |client|
#     client.declare_queue("jobs")
#     client.publish("", "jobs", "hello", headers: { "attempt" => 1 }, content_type: "text/plain")
#     client.subscribe("jobs", prefetch: 10).each do |msg|
#       puts msg.text
#       msg.ack
#       break
#     end
#   end
#
# The native library is found through FIREMQ_LIB (a path to the file), next to
# lib/firemq/firemq_client_ffi.rb, or on the system library path.

require "json"
require_relative "firemq/firemq_client_ffi"

module FireMQ
  VERSION = "0.1.0"
  DEFAULT_URL = "fire://guest:guest@localhost:5673"

  # Base class of every FireMQ error. +code+ is the AMQP reply code, or 0.
  class Error < StandardError
    attr_reader :code

    def initialize(message = nil, code = 0)
      super(message)
      @code = code
    end
  end

  # The broker refused a request; +code+ is 403, 404, 406 and so on.
  class ServerError < Error; end
  # A mandatory publish matched no queue.
  class UnroutableError < Error; end
  # The broker rejected a publish, for example because a queue was full.
  class NackedError < Error; end
  # The connection or subscription is closed.
  class ClosedError < Error; end
  # Anything else: I/O failures, invalid URLs or JSON, protocol errors.
  class OtherError < Error; end

  Native = ::FiremqClientFfi
  NativeError = Native::FireMqError

  # Property names as the protocol spells them, and as the native record spells them.
  PROPERTY_FIELDS = {
    content_type: :content_type, content_encoding: :content_encoding,
    correlation_id: :correlation_id, reply_to: :reply_to, expiration: :expiration,
    message_id: :message_id, timestamp: :timestamp, type: :message_type,
    user_id: :user_id, app_id: :app_id, priority: :priority
  }.freeze

  # Runs the block and converts native errors into FireMQ::Error subclasses.
  def self.translate
    yield
  rescue NativeError::Server => e
    raise ServerError.new(e.message, e.code)
  rescue NativeError::Unroutable => e
    raise UnroutableError.new(e.message, 312)
  rescue NativeError::Nacked => e
    raise NackedError.new(e.message, e.code)
  rescue NativeError::Closed => e
    raise ClosedError.new(e.message, 0)
  rescue NativeError::Other => e
    raise OtherError.new(e.message, 0)
  end

  def self.json_or_nil(value)
    value.nil? ? nil : JSON.generate(value)
  end

  # Connects with a URL such as fire://user:pass@host:5673/vhost. With a block, yields the
  # client and closes it afterwards.
  def self.connect(url = DEFAULT_URL)
    client = Client.new(url)
    return client unless block_given?

    begin
      yield client
    ensure
      client.close unless client.closed?
    end
  end

  # A connection to FireMQ.
  class Client
    def initialize(url = DEFAULT_URL)
      @inner = FireMQ.translate { Native::Client.connect(url) }
    end

    # Closes the connection. Unacknowledged messages return to their queues.
    def close
      FireMQ.translate { @inner.close }
    end

    def closed?
      @inner.is_closed
    end

    # Declares a queue; an empty name asks the broker to generate one. Returns an object with
    # +name+, +messages+ and +consumers+.
    def declare_queue(name = "", durable: true, exclusive: false, auto_delete: false, arguments: nil)
      FireMQ.translate do
        @inner.declare_queue(name, durable, exclusive, auto_delete, FireMQ.json_or_nil(arguments))
      end
    end

    # Deletes a queue and returns how many messages it held.
    def delete_queue(name)
      FireMQ.translate { @inner.delete_queue(name) }
    end

    # Removes every ready message and returns how many were removed.
    def purge_queue(name)
      FireMQ.translate { @inner.purge_queue(name) }
    end

    def declare_exchange(name, type = "direct", durable: true, auto_delete: false, arguments: nil)
      FireMQ.translate do
        @inner.declare_exchange(name, type, durable, auto_delete, FireMQ.json_or_nil(arguments))
      end
    end

    def delete_exchange(name, if_unused: false)
      FireMQ.translate { @inner.delete_exchange(name, if_unused) }
    end

    def bind_queue(queue, exchange, routing_key = "", arguments: nil)
      FireMQ.translate do
        @inner.bind_queue(queue, exchange, routing_key, FireMQ.json_or_nil(arguments))
      end
    end

    def unbind_queue(queue, exchange, routing_key = "")
      FireMQ.translate { @inner.unbind_queue(queue, exchange, routing_key) }
    end

    def bind_exchange(destination, source, routing_key = "")
      FireMQ.translate { @inner.bind_exchange(destination, source, routing_key) }
    end

    # Publishes a message and, by default, waits for the broker's confirm. Properties use the
    # protocol's names: content_type, correlation_id, type, priority, expiration and so on.
    def publish(exchange, routing_key, body, headers: nil, persistent: true, mandatory: false,
                confirm: true, **properties)
      unknown = properties.keys - PROPERTY_FIELDS.keys
      raise ArgumentError, "unknown message properties: #{unknown.join(', ')}" unless unknown.empty?

      props = Native::MessageProperties.new(
        **properties.compact.to_h { |k, v| [PROPERTY_FIELDS.fetch(k), v] }
      )
      options = Native::PublishOptions.new(
        persistent: persistent, mandatory: mandatory, confirm: confirm,
        properties: props, headers_json: FireMQ.json_or_nil(headers)
      )
      FireMQ.translate { @inner.publish(exchange, routing_key, body.to_s.b, options) }
    end

    # Starts consuming. +prefetch+ caps unacknowledged deliveries.
    def subscribe(queue, prefetch: 100, no_ack: false)
      Subscription.new(FireMQ.translate { @inner.subscribe(queue, prefetch, no_ack) })
    end

    # Takes one message from a queue, or returns nil when it is empty.
    def get(queue, no_ack: false)
      d = FireMQ.translate { @inner.get(queue, no_ack) }
      d && Message.new(d)
    end
  end

  # Messages from a queue.
  class Subscription
    include Enumerable

    def initialize(inner)
      @inner = inner
    end

    # Waits up to +timeout+ seconds for a message and returns nil on timeout. Raises
    # ClosedError once the subscription has ended.
    def next(timeout: 1.0)
      d = FireMQ.translate { @inner.next((timeout * 1000).to_i) }
      d && Message.new(d)
    end

    # Yields messages until the subscription ends.
    def each
      return enum_for(:each) unless block_given?

      loop do
        msg = self.next(timeout: 0.5)
        yield msg if msg
      rescue ClosedError
        break
      end
    end

    # Stops the subscription. Messages already received can still be acknowledged.
    def cancel
      FireMQ.translate { @inner.cancel }
    end
  end

  # A delivered message.
  class Message
    def initialize(delivery)
      @d = delivery
    end

    # The body as a binary String.
    def body
      @body ||= @d.body.b
    end

    # The body as UTF-8 text.
    def text
      body.dup.force_encoding(Encoding::UTF_8)
    end

    def json
      JSON.parse(body)
    end

    def exchange = @d.exchange
    def routing_key = @d.routing_key
    def redelivered? = @d.redelivered
    def delivery_tag = @d.delivery_tag

    # Application headers. Binary values arrive base64-encoded.
    def headers
      @headers ||= JSON.parse(@d.headers_json)
    end

    # The properties that are set, keyed by their protocol names as strings.
    def properties
      @properties ||= begin
        p = @d.properties
        PROPERTY_FIELDS.each_with_object({}) do |(name, field), h|
          v = p.public_send(field)
          h[name.to_s] = v unless v.nil?
        end
      end
    end

    def ack
      FireMQ.translate { @d.ack }
    end

    # Rejects the message. With +requeue+ it is delivered again; otherwise it is
    # dead-lettered when the queue has a dead-letter exchange, and dropped if not.
    def nack(requeue: true)
      FireMQ.translate { @d.nack(requeue) }
    end
  end
end
