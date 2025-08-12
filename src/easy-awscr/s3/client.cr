require "awscr-s3"
require "./internals/connection_pool"
require "./internals/async_chunk_uploader"

module EasyAwscr::S3
  class Client
    @s3_client : Awscr::S3::Client?
    @client_factory : Internals::ConnectionPool?

    # An optional hook for managing the native S3 client ("awscr-s3" library)
    # yourself, instead of letting "easy-aws" handle it internally.
    #
    # The first parameter is a hint whether the connections or credentials should
    # be refreshed. It is only a hint, and implementations are allowed to ignore it.
    alias ClientProvider = Proc(Bool, Awscr::S3::Client)

    # This is a hard limit enforced by AWS for multipart uploads:
    # each part must be at least 5 MB.
    MINIMUM_PART_SIZE_5MB = 5242880

    # From time to time, we should recreated the connection pool, so it will reload
    # the SSL context (see https://github.com/crystal-lang/crystal/issues/15419).
    DEFAULT_POOL_REFRESH_INTERVAL = 24.hours

    def initialize(*,
                   @region = EasyAwscr::Config.default_region!,
                   @credential_provider = EasyAwscr::Config.default_credential_provider,
                   @client_provider : ClientProvider? = nil,
                   @endpoint : String? = nil,
                   lazy_init = false)
      @mutex = Mutex.new(:unchecked)
      client unless lazy_init
    end

    # Converts an existing client from "awscr-s3" to use the "easy-awscr" client interface.
    def self.from_native_client(native_client : Awscr::S3::Client) : self
      new(client_provider: ->(_force_new : Bool) { native_client }, region: "us-east-1", lazy_init: false)
    end

    # Closes this client. If used again, a new connection will be opened.
    def close
      @mutex.synchronize do
        @client_factory.try &.close
      ensure
        @s3_client = nil
        @client_factory = nil
      end
    end

    private def create_connection_pool
      # TODO: this uses the defaults, but it would make sense to
      # let the user overwrite them.
      Internals::ConnectionPool.new
    end

    # List s3 buckets
    #
    # ```
    # resp = client.list_buckets
    # p resp.buckets.map(&.name) # => ["bucket1", "bucket2"]
    # ```
    def list_buckets
      try_with_refresh &.list_buckets
    end

    # Create a bucket, optionally place it in a region.
    #
    # ```
    # resp = client.create_bucket("test")
    # p resp # => true
    # ```
    def put_bucket(bucket, region : String? = nil, headers = Hash(String, String).new)
      try_with_refresh &.put_bucket(bucket, region, headers)
    end

    # Delete a bucket, note: it must be empty
    #
    # ```
    # resp = client.delete_bucket("test")
    # p resp # => true
    # ```
    def delete_bucket(bucket)
      try_with_refresh &.delete_bucket(bucket)
    end

    # Start a multipart upload
    #
    # ```
    # resp = client.start_multipart_upload("bucket1", "obj")
    # p resp.upload_id # => someid
    # ```
    def start_multipart_upload(bucket : String, object : String, headers = Hash(String, String).new)
      try_with_refresh &.start_multipart_upload(bucket, object, headers)
    end

    # Upload a part, for use in multipart uploading
    #
    # ```
    # resp = client.upload_part("bucket1", "obj", "someid", 123, "MY DATA")
    # p resp.upload_id # => someid
    # ```
    def upload_part(bucket : String, object : String,
                    upload_id : String, part_number : Int32, part : IO | String)
      try_with_refresh &.upload_part(bucket, object, upload_id, part_number, part)
    end

    # Complete a multipart upload
    #
    # ```
    # resp = client.complete_multipart_upload("bucket1", "obj", "123", parts)
    # p resp.key # => obj
    # ```
    def complete_multipart_upload(bucket : String, object : String, upload_id : String, parts : Array(Awscr::S3::Response::UploadPartOutput))
      try_with_refresh &.complete_multipart_upload(bucket, object, upload_id, parts)
    end

    # Aborts a multi part upload. Returns true if the abort was a success, false
    # otherwise.
    #
    # ```
    # resp = client.abort_multipart_upload("bucket1", "obj", "123")
    # p resp # => true
    # ```
    def abort_multipart_upload(bucket : String, object : String, upload_id : String)
      try_with_refresh &.abort_multipart_upload(bucket, object, upload_id)
    end

    # Get information about a bucket, useful for determining if a bucket exists.
    # Raises a `Http::ServerError` if the bucket does not exist.
    #
    # ```
    # resp = client.head_bucket("bucket1")
    # p resp # => true
    # ```
    def head_bucket(bucket)
      try_with_refresh &.head_bucket(bucket)
    end

    # Delete an object from a bucket, returns `true` if successful, `false`
    # otherwise.
    #
    # ```
    # resp = client.delete_object("bucket1", "obj")
    # p resp # => true
    # ```
    def delete_object(bucket, object, headers = Hash(String, String).new)
      try_with_refresh &.delete_object(bucket, object, headers)
    end

    # Batch deletes a list of object keys in a single request.
    #
    # ```
    # resp = client.batch_delete("bucket1", ["obj", "obj2"])
    # p resp.success? # => true
    # ```
    def batch_delete(bucket, keys : Array(String))
      try_with_refresh &.batch_delete(bucket, keys)
    end

    # Copy an object from `source` to `destination` in a bucket.
    #
    # ```
    # client.copy_object("bucket1", "source_object", "destination_object")
    # ```
    def copy_object(bucket, source : String, destination : String,
                    headers = Hash(String, String).new)
      try_with_refresh &.copy_object(bucket, source, destination, headers)
    end

    # Add an object to a bucket.
    #
    # ```
    # resp = client.put_object("bucket1", "obj", "MY DATA")
    # p resp.key # => "obj"
    # ```
    def put_object(bucket, object : String, body : IO | String | Bytes,
                   headers = Hash(String, String).new)
      try_with_refresh &.put_object(bucket, object, body, headers)
    end

    # Get the contents of an object in a bucket
    #
    # ```
    # resp = client.get_object("bucket1", "obj")
    # p resp.body # => "MY DATA"
    # ```
    def get_object(bucket, object : String, headers = Hash(String, String).new)
      try_with_refresh &.get_object(bucket, object, headers)
    end

    # Get the contents of an object in a bucket as an IO object
    #
    # ```
    # client.get_object("bucket1", "obj") do |resp|
    #   IO.copy(resp.body_io, STDOUT) # => "MY DATA"
    # end
    # ```
    def get_object(bucket, object : String, headers = Hash(String, String).new, &)
      try_with_refresh do |client|
        client.get_object(bucket, object, headers) do |resp|
          yield resp
        end
      end
    end

    # Get the metadata of an object in a bucket
    #
    # ```
    # resp = client.head_object("bucket1", "obj")
    # p resp.size          # => 123
    # p resp.status        # => HTTP::Status::OK
    # p resp.last_modified # => "Wed, 19 Jun 2019 11:55:33 GMT"
    # p resp.etag          # => ""
    # p resp.meta          # => {"my_tag" => "my_value"}
    # ```
    def head_object(bucket, object : String, headers = Hash(String, String).new)
      try_with_refresh &.head_object(bucket, object, headers)
    end

    # List all the items in a bucket
    #
    # ```
    # resp = client.list_objects("bucket1", prefix: "test")
    # p resp.map(&.key) # => ["obj"]
    # ```
    def list_objects(bucket, *, prefix = nil, max_keys = nil) : Awscr::S3::Paginator::ListObjectsV2
      try_with_refresh &.list_objects(bucket, max_keys, prefix)
    end

    # Upload a file to a bucket. Returns true if successful; otherwise an
    # `Http::ServerError` is thrown.
    #
    # ```
    # File.open("/path/some/big/file.txt") do |io|
    #   success = client.upload_file("bucket1", "obj", io)
    #   p success => true
    # end
    # ```
    #
    # It uses Awscr::S3::FileUploader internally:
    # * If the file is 5MB or lower, it will be uploaded in a single request;
    #   but if the file is greater than 5MB, it will be uploaded in parts.
    # * If `with_content_type` is true, the uploader will automatically add
    #   a content type header
    def upload_file(bucket : String, object : String, io : IO, *,
                    headers = Hash(String, String).new, with_content_type = true, simultaneous_parts = 5) : Bool
      try_with_refresh do |client|
        options = Awscr::S3::FileUploader::Options.new(with_content_type, simultaneous_parts)
        uploader = Awscr::S3::FileUploader.new(client, options)
        uploader.upload(bucket, object, io, headers)
      end
    end

    # Provides `IO` that can be used to stream directly into an S3 file. In contrast
    # to `upload_file`, the size of the data does not have to be known before.
    # It will use `start_multipart_upload`, `upload_part`, and `complete_multipart_upload`
    # internally.
    #
    # Example: creates a file on S3
    #
    # ```
    # client.stream_to_s3("bucket1", "obj") do |io|
    #   io << ...
    # end
    # ```
    #
    # Intuitively, it is like writing to a local file, matching this pattern:
    # ```
    # File.open("/tmp/bucket1/obj", "w") do |io|
    #   io << ...
    # end
    # ```
    #
    # If you need more control, you can also get direct access to the `IO` object:
    #
    # ```
    # io = client.stream_to_s3("bucket1", "obj", auto_close: false) { |io| io }
    # io << ...
    # io.close
    # ```
    def stream_to_s3(bucket : String, object : String, *,
                     headers = Hash(String, String).new, part_size = MINIMUM_PART_SIZE_5MB,
                     max_workers = 8, auto_close = true, &)
      if part_size < MINIMUM_PART_SIZE_5MB
        raise IO::Error.new "AWS enforces a minimum part size of 5 MB (got: #{part_size})"
      end

      upload_handler = Internals::AsyncChunkUploader.new(self, bucket, object, headers, max_workers: max_workers)
      io = Internals::ChunkedIO.new(part_size, upload_handler)
      begin
        yield io
      ensure
        io.close if auto_close
      end
    end

    private def try_with_refresh(&)
      yield client
    rescue Awscr::S3::ExpiredToken
      yield client(force_new: true)
    end

    private def client(*, force_new = false) : Awscr::S3::Client
      @client_provider.try { |provider| return provider.call(force_new) }

      dead_client_factory = nil
      @mutex.synchronize do
        s3_client = @s3_client
        if s3_client && !force_new && !client_factory_needs_refresh?
          s3_client
        else
          cred = @credential_provider.credentials

          # refresh the connection pool (updates also the SSL context)
          client_factory = create_connection_pool
          dead_client_factory = @client_factory
          @client_factory = client_factory

          @s3_client = Awscr::S3::Client.new(
            @region,
            cred.access_key_id,
            cred.secret_access_key,
            cred.session_token,
            endpoint: @endpoint,
            client_factory: client_factory
          )
        end
      end
    ensure
      dead_client_factory.try &.close
    end

    private def client_factory_needs_refresh? : Bool
      if cf = @client_factory
        Time.utc - cf.created_at > DEFAULT_POOL_REFRESH_INTERVAL
      else
        false
      end
    end
  end
end
