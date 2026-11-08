# frozen_string_literal: true

# Conformance driver for the Ruby binding. See spec/conformance.md.
#
# Reads one JSON command per line on stdin and writes one JSON reply per line on stdout.
#
#   firemq-conformance run --driver "ruby -Ibindings/ruby/lib bindings/ruby/conformance_driver.rb"

require "json"
require "firemq"

class Driver
  Failure = Class.new(StandardError) do
    attr_reader :kind

    def initialize(kind, message)
      super(message)
      @kind = kind
    end
  end

  KINDS = {
    FireMQ::ServerError => "server", FireMQ::UnroutableError => "unroutable",
    FireMQ::NackedError => "nacked", FireMQ::ClosedError => "closed"
  }.freeze

  def self.error(kind, code, message) = { "kind" => kind, "code" => code, "message" => message }

  def self.error_of(err) = error(KINDS.fetch(err.class, "other"), err.code, err.message)

  def initialize
    @client = nil
    @subs = {}
    @pending = {}
    @next_id = 0
  end

  def conn
    @client or raise Failure.new("closed", "not connected")
  end

  def keep(msg)
    tag = (@next_id += 1).to_s
    @pending[tag] = msg
    text = msg.body.dup.force_encoding(Encoding::UTF_8)
    {
      "body" => text.valid_encoding? ? text : nil,
      "body_base64" => [msg.body].pack("m0"),
      "exchange" => msg.exchange,
      "routing_key" => msg.routing_key,
      "redelivered" => msg.redelivered?,
      "tag" => tag,
      "properties" => msg.properties,
      "headers" => msg.headers
    }
  end

  def take(c)
    @pending.delete(c["tag"]) or raise Failure.new("other", "unknown tag")
  end

  def handle(op, c)
    case op
    when "hello"
      { "sdk" => "firemq-ruby", "version" => FireMQ::VERSION, "capabilities" => [] }
    when "connect"
      @client = FireMQ.connect(c["url"])
      {}
    when "close"
      @subs.clear
      @pending.clear
      client = @client
      @client = nil
      client&.close
      {}
    when "declare_queue"
      q = conn.declare_queue(c["name"], durable: c["durable"], exclusive: c["exclusive"],
                                        auto_delete: c["auto_delete"], arguments: c["args"])
      { "name" => q.name, "messages" => q.messages, "consumers" => q.consumers }
    when "delete_queue"
      { "messages" => conn.delete_queue(c["name"]) }
    when "purge_queue"
      { "messages" => conn.purge_queue(c["name"]) }
    when "declare_exchange"
      conn.declare_exchange(c["name"], c["type"], durable: c["durable"])
      {}
    when "delete_exchange"
      conn.delete_exchange(c["name"])
      {}
    when "bind_queue"
      conn.bind_queue(c["queue"], c["exchange"], c["key"], arguments: c["args"])
      {}
    when "unbind_queue"
      conn.unbind_queue(c["queue"], c["exchange"], c["key"])
      {}
    when "bind_exchange"
      conn.bind_exchange(c["destination"], c["source"], c["key"])
      {}
    when "publish"
      body = c["body_base64"] ? c["body_base64"].unpack1("m0") : c["body"]
      props = (c["properties"] || {}).transform_keys(&:to_sym)
      conn.publish(c["exchange"], c["key"], body, headers: c["headers"],
                                                  persistent: c["persistent"], mandatory: c["mandatory"], **props)
      {}
    when "subscribe"
      sub = conn.subscribe(c["queue"], prefetch: c["prefetch"], no_ack: c["no_ack"])
      id = "sub-#{@next_id += 1}"
      @subs[id] = sub
      { "sub" => id }
    when "next"
      sub = @subs[c["sub"]] or raise Failure.new("other", "unknown sub")
      msg = begin
        sub.next(timeout: c["timeout_ms"] / 1000.0)
      rescue FireMQ::ClosedError
        nil
      end
      { "message" => msg && keep(msg) }
    when "ack"
      take(c).ack
      {}
    when "nack"
      take(c).nack(requeue: c["requeue"])
      {}
    when "unsubscribe"
      @subs.delete(c["sub"])&.cancel
      {}
    when "get"
      msg = conn.get(c["queue"], no_ack: c["no_ack"])
      { "message" => msg && keep(msg) }
    else
      raise Failure.new("unsupported", "unknown op #{op}")
    end
  end

  def run
    $stdout.sync = true
    $stdin.each_line do |line|
      next if line.strip.empty?

      begin
        cmd = JSON.parse(line)
      rescue JSON::ParserError => e
        warn "driver: bad command: #{e.message}"
        next
      end
      reply = begin
        { "id" => cmd["id"], "ok" => true, "result" => handle(cmd["op"], cmd) }
      rescue Failure => e
        { "id" => cmd["id"], "ok" => false, "error" => Driver.error(e.kind, 0, e.message) }
      rescue FireMQ::Error => e
        { "id" => cmd["id"], "ok" => false, "error" => Driver.error_of(e) }
      rescue StandardError => e
        { "id" => cmd["id"], "ok" => false, "error" => Driver.error("other", 0, e.inspect) }
      end
      $stdout.write("#{JSON.generate(reply)}\n")
    end
  end
end

Driver.new.run
