require "awscr-s3"

module EasyAwscr::S3
  class Client
    @s3_client : Awscr::S3::Client?

    def initialize(*,
                   @region = EasyAwscr::Config.default_region!,
                   @credential_provider = EasyAwscr::Config.default_credential_provider,
                   lazy_init = false)
      @mutex = Mutex.new
      client unless lazy_init
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

    private def try_with_refresh(&)
      yield client
    rescue Awscr::S3::ExpiredToken
      yield client(force_new: true)
    end

    private def client(*, force_new = false) : Awscr::S3::Client
      @mutex.synchronize do
        s3_client = @s3_client
        if s3_client && !force_new
          s3_client
        else
          cred = @credential_provider.credentials
          @s3_client = Awscr::S3::Client.new(@region, cred.access_key_id, cred.secret_access_key, cred.session_token)
        end
      end
    end
  end
end
