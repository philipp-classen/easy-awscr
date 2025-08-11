require "http/client"

module EasyAwscr::S3::Internals
  class ConnectionPool < Awscr::S3::HttpClientFactory
    getter created_at

    def initialize(*, @max_ttl : Time::Span? = 5.minutes, @max_size = 128)
      @pool = Hash(Fiber, {HTTP::Client, Time}).new
      @mutex = Mutex.new(:unchecked)
      @closed = false
      @created_at = Time.utc
    end

    def acquire_client(endpoint : URI, signer : Awscr::Signer::Signers::Interface) : HTTP::Client
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

    # Overwritten only to call "reset_headers" (see comment there for details).
    #
    # Note: At this point it is not clear if the workaround can be improved. Should
    # it become clear that it cannot, then this code should perhaps be moved into the
    # awscr-s3 library (since then every user would need to replicate the code here).
    protected def attach_signer(client, signer)
      if signer.is_a?(Awscr::Signer::Signers::V4)
        client.before_request do |req|
          reset_headers(req)
          signer.as(Awscr::Signer::Signers::V4).sign(req, encode_path: false)
        end
      else
        client.before_request do |req|
          reset_headers(req)
          signer.sign(req)
        end
      end
    end

    # Workaround to avoid signing errors when requests have to be repeated
    # after a TCPSocket has to be reconnected.
    #
    # Background:
    # * https://github.com/taylorfinnell/awscr-signer/issues/56
    # * https://github.com/crystal-lang/crystal/issues/16028
    private def reset_headers(req)
      req.headers.delete "Authorization"
      req.headers.delete "X-Amz-Content-Sha256"
      req.headers.delete "X-Amz-Date"
    end

    private def expired?(last_checked, now = Time.utc)
      @max_ttl.try { |ttl| now - last_checked > ttl }
    end

    def acquire_raw_client(endpoint : URI) : HTTP::Client
      HTTP::Client.new(endpoint)
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
        unless @closed
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
  end
end
