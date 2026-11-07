# frozen_string_literal: true

require "minitest/autorun"
require "securerandom"
require "firemq"

class FireMQTest < Minitest::Test
  URL = ENV.fetch("FIREMQ_URL", FireMQ::DEFAULT_URL)

  def setup
    @client = FireMQ.connect(URL)
    @queue = @client.declare_queue("rb.#{SecureRandom.hex(6)}", durable: false, exclusive: true).name
  end

  def teardown
    @client.close unless @client.closed?
  end

  def test_publish_consume_ack
    @client.publish("", @queue, "hello from Ruby", headers: { "lang" => "ruby", "n" => 5 },
                                                   content_type: "text/plain", priority: 3)
    sub = @client.subscribe(@queue, prefetch: 10)
    msg = sub.next(timeout: 5)
    refute_nil msg
    assert_equal "hello from Ruby", msg.text
    assert_equal @queue, msg.routing_key
    assert_equal "", msg.exchange
    refute msg.redelivered?
    assert_equal({ "lang" => "ruby", "n" => 5 }, msg.headers)
    assert_equal({ "content_type" => "text/plain", "priority" => 3 }, msg.properties)
    msg.ack
    assert_nil sub.next(timeout: 0.05)
    sub.cancel
    assert_raises(FireMQ::ClosedError) { sub.next(timeout: 0) }
  end

  def test_each_and_requeue
    5.times { |i| @client.publish("", @queue, "m#{i}") }
    sub = @client.subscribe(@queue, prefetch: 2)
    got = sub.each.lazy.map { |m| m.ack; m.text }.first(5)
    assert_equal (0...5).map { |i| "m#{i}" }, got
    sub.cancel

    @client.publish("", @queue, "\x00\xff".b)
    @client.get(@queue).nack(requeue: true)
    again = @client.get(@queue, no_ack: true)
    assert again.redelivered?
    assert_equal "\x00\xff".b, again.body
  end

  def test_errors
    err = assert_raises(FireMQ::ServerError) { FireMQ.connect(URL.sub("guest:guest", "guest:wrong")) }
    assert_equal 403, err.code
    err = assert_raises(FireMQ::UnroutableError) do
      @client.publish("", "#{@queue}.nowhere", "x", mandatory: true)
    end
    assert_equal 312, err.code
    err = assert_raises(FireMQ::ServerError) { @client.bind_queue(@queue, "#{@queue}.missing") }
    assert_equal 404, err.code
    assert_raises(FireMQ::OtherError) { @client.declare_queue("x", arguments: [1]) }
    assert_raises(ArgumentError) { @client.publish("", @queue, "x", colour: "red") }
    @client.close
    assert_raises(FireMQ::ClosedError) { @client.purge_queue(@queue) }
  end
end
