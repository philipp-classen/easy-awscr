require "http/client"

module EasyAwscr::CloudWatch::Internals
  # Keeps one connection per fiber and drops idle ones. The clients themselves
  # come from the default factory, so timeouts and the shared TLS context stay
  # the same.
  class ConnectionPool < Awscr::CloudWatch::DefaultHttpClientFactory
    getter created_at : Time

    # Not confirmed by official sources, but in tests it took around 5 seconds
    # until CloudWatch closes an idle connection. That means, we need to stay
    # below that number.
    DEFAULT_MAX_TTL = 3.seconds

    def initialize(*, @max_ttl : Time::Span? = DEFAULT_MAX_TTL, @max_size = 128)
      super()
      @pool = Hash(Fiber, {HTTP::Client, Time}).new
      @mutex = Mutex.new(:unchecked)
      @closed = false
      @created_at = Time.utc
    end

    def acquire_client(endpoint : URI) : HTTP::Client
      @mutex.synchronize { @pool.delete(Fiber.current) }.try do |client, last_checked|
        if expired?(last_checked)
          client.close
        else
          return client
        end
      end

      # creates a new client
      super
    end

    def release(client : HTTP::Client?)
      return unless client

      if @max_size == 0
        client.close
        return
      end

      now = Time.utc
      dead1 = nil
      dead2 = nil
      dead3 = nil

      current_fiber = Fiber.current
      @mutex.synchronize do
        if @closed
          dead1 = client # the pool closed while the request was in flight
        else
          @pool.first_key?.try do |fiber|
            dead1 = @pool.shift[1][0] if fiber.dead? || expired?(@pool.first_value[1], now)
            @pool.delete(current_fiber).try { |old_client, _| dead2 = old_client }
          end
          @pool[current_fiber] = {client, now}
          dead3 = @pool.shift[1][0] if @pool.size > @max_size
        end
      end
    ensure
      dead1.try &.close
      dead2.try &.close
      dead3.try &.close
    end

    def close
      @mutex.synchronize do
        return if @closed

        @closed = true
        @pool.values.each { |client, _| client.close }
        @pool.clear
      end
    end

    private def expired?(last_checked, now = Time.utc)
      @max_ttl.try { |ttl| now - last_checked > ttl }
    end
  end
end
