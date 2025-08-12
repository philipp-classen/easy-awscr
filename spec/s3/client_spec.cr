require "../spec_helper"
require "wait_group"

record Api,
  id : Symbol,
  credential_provider : EasyAwscr::Config::Provider,
  region : String,
  endpoint : String? = nil

def test_provider(access_key = "admin", secret_access_key = "password") : EasyAwscr::Config::Provider
  EasyAwscr::Config::Provider.new(
    Aws::Credentials::Providers.new([
      Aws::Credentials::SimpleCredentials.new(access_key, secret_access_key).as(Aws::Credentials::Provider),
    ])
  )
end

def with_aws_api(& : Api -> Nil)
  found = 0

  # Uses a sandboxed MinIO environment (https://github.com/minio/minio).
  # Tip: you can start a local Docker instance by running:
  # $ make start-minio
  if env_set? "EASY_AWSCR_SPEC_USE_MINIO"
    found += 1
    yield Api.new(
      id: :minio,
      credential_provider: test_provider("admin", "password"),
      region: "unused",
      endpoint: "http://127.0.0.1:9000"
    )
  end

  # Be careful when running this against a real account, especially, against a
  # production account. Prever a test account, but note that using a real API
  # will create some costs.
  #
  # The test try to operate only on dedicated test buckets to limit the blast area.
  # Still, please check the tests and run them only if you know what you are doing.
  if env_set? "EASY_AWSCR_SPEC_USE_NATIVE_AWS__I_KNOW_THE_RISK__"
    found += 1
    yield Api.new(
      id: :native_aws,
      credential_provider: EasyAwscr::Config.default_credential_provider,
      region: EasyAwscr::Config.default_region!
    )
  end

  if found == 0
    puts "\nWARNING: No AWS API available. Tests will be skipped."
  end
end

def with_client(*, auto_close = true, & : EasyAwscr::S3::Client -> Nil)
  with_aws_api do |api|
    client = EasyAwscr::S3::Client.new(
      region: api.region,
      endpoint: api.endpoint,
      credential_provider: api.credential_provider
    )
    begin
      yield client, api
    ensure
      client.close if auto_close
    end
  end
end

def with_native_awscr_s3_client(& : Awscr::S3::Client -> Nil)
  with_aws_api do |api|
    cred = api.credential_provider.credentials
    yield Awscr::S3::Client.new(
      api.region,
      cred.access_key_id,
      cred.secret_access_key,
      cred.session_token,
      endpoint: api.endpoint
    )
  end
end

class SafeKeyGen
  getter keys_used

  def initialize(@id = UUID.random)
    @counter = 0
    @keys_used = [] of String
  end

  def create!
    key = "test-easy-awscr-tmp-key-#{@id}-#{@counter}"
    @counter += 1
    @keys_used << key
    key
  end
end

def with_temp_test_bucket(client : EasyAwscr::S3::Client,
                          *, bucket_name = "test-easy-awscr-tmp-bucket-#{UUID.random}",
                          auto_delete = true,
                          & : String, SafeKeyGen -> Nil)
  unless client.put_bucket(bucket_name)
    raise "Unable to create test bucket: #{bucket_name}"
  end

  safe_key_gen = SafeKeyGen.new
  begin
    yield bucket_name, safe_key_gen
  ensure
    if auto_delete
      safe_key_gen.keys_used.each do |key|
        client.delete_object(bucket_name, key)
      end
      client.delete_bucket(bucket_name)
    end
  end
end

# Creates test files containing "a" to "p" (then repeating, starting again with "a").
# Example: size 30 ==> "bcdefghijklmnopabcdefghijklmno"
class Testfile
  getter size : Int64

  def initialize(@size : Int64)
  end

  def self.byte(size : Int32 | Int64) : self
    self.new(size.to_i64)
  end

  def self.kilobyte(size : Int32 | Int64) : self
    self.new(1024_i64 * size)
  end

  def self.megabyte(size : Int32 | Int64) : self
    kilobyte(1024_i64 * size)
  end

  def self.gigabyte(size : Int32 | Int64) : self
    megabyte(1024_i64 * size)
  end

  def open(& : OpenedTestfile -> _)
    yield OpenedTestfile.new(@size)
  end

  private class OpenedTestfile < IO
    getter size : Int64
    getter pos : Int64

    def initialize(@size : Int64)
      @pos = 0
    end

    def read(slice : Bytes) : Int32
      remaining = @size - @pos
      len = remaining < slice.size ? remaining.to_i32 : slice.size
      len.times do |i|
        slice[i] = ('a'.ord &+ ((@pos &+ i) & 15)).to_u8!
      end
      @pos &+= len
      len
    end

    def write(slice : Bytes) : NoReturn
      raise IO::Error.new("Can't write to Testfile")
    end

    def rewind
      @pos = 0
    end
  end
end

def expect_file(client : EasyAwscr::S3::Client, bucket : String, key : String, testfile : Testfile)
  testfile.open do |io|
    client.get_object(bucket, key) do |resp|
      IO.same_content?(resp.body_io, io)
    end
  end
end

describe EasyAwscr::S3::Client do
  describe "verify test setup" do
    it "should detect a created bucket in the list" do
      with_client do |client|
        with_temp_test_bucket(client) do |bucket|
          client.head_bucket(bucket).should be_true
          client.list_buckets.buckets.should contain(bucket)
        end
      end
    end
  end

  describe "#put_object" do
    describe "with string as body" do
      it "should be able to upload text and read the content again" do
        with_client do |client|
          with_temp_test_bucket(client) do |bucket, safe_key|
            key = safe_key.create!
            client.put_object(bucket, key, "Some content")
            res = client.get_object(bucket, key)
            res.body.should eq("Some content")
          end
        end
      end

      it "should support files up to two 2 gb", tags: "slow" do
        with_client do |client|
          content = "a" * (1024 * 1024 * 1024)
          with_temp_test_bucket(client) do |bucket, safe_key|
            key = safe_key.create!
            client.put_object(bucket, key, content)
            res = client.get_object(bucket, key)
            res.body.should eq(content)
          end
        end
      end
    end

    describe "with IO as body" do
      it "should be able to upload text and read the content again" do
        with_client do |client|
          with_temp_test_bucket(client) do |bucket, safe_key|
            key = safe_key.create!
            content = Testfile.kilobyte(4)
            content.open { |io| client.put_object(bucket, key, io) }
            expect_file(client, bucket, key, content)
          end
        end
      end

      it "should support files over 2 gb", tags: "slow" do
        with_client do |client|
          with_temp_test_bucket(client) do |bucket, safe_key|
            key = safe_key.create!
            content = Testfile.gigabyte(8)
            content.open { |io| client.put_object(bucket, key, io) }
            expect_file(client, bucket, key, content)
          end
        end
      end
    end
  end

  describe "#stream_to_s3" do
    it "should work for small files" do
      with_client do |client|
        with_temp_test_bucket(client) do |bucket, safe_key|
          key = safe_key.create!
          content = Testfile.kilobyte(4)
          content.open do |input|
            client.stream_to_s3(bucket, key) do |io|
              io << input << 'a'
            end
          end
          expect_file(client, bucket, key, content)
        end
      end
    end

    it "should work for files over 2 gb", tags: "slow" do
      with_client do |client|
        with_temp_test_bucket(client) do |bucket, safe_key|
          key = safe_key.create!
          content = Testfile.gigabyte(8)
          content.open do |input|
            client.stream_to_s3(bucket, key) do |io|
              io << input << 'a'
            end
          end
          expect_file(client, bucket, key, content)
        end
      end
    end

    it "should work to stream multiple files over 2 gb in parallel", tags: "slow" do
      with_client do |client|
        with_temp_test_bucket(client) do |bucket, safe_key|
          keys = [safe_key.create!, safe_key.create!, safe_key.create!]

          WaitGroup.wait do |worker|
            keys.each do |key|
              worker.spawn do
                content = Testfile.gigabyte(4)
                content.open do |input|
                  client.stream_to_s3(bucket, key) do |io|
                    io << input << 'a'
                  end
                end
                expect_file(client, bucket, key, content)
              end
            end
          end
        end
      end
    end
  end

  describe "allow to use your own awscr-s3 client" do
    describe "#constructor" do
      it "should use the client_provider if configured" do
        with_native_awscr_s3_client do |awscr_s3_client|
          call_count = 0
          client_provider = ->(_force_new : Bool) do
            call_count += 1
            awscr_s3_client
          end

          client = EasyAwscr::S3::Client.new(client_provider: client_provider, region: "us-east-1", lazy_init: true)
          call_count.should eq(0)

          list1 = client.list_buckets
          call_count.should eq(1)
          list2 = client.list_buckets
          call_count.should eq(2)

          list1.buckets.should eq(list2.buckets)
        end
      end
    end

    describe "#from_native_client" do
      it "should be support to wrap a single call" do
        with_native_awscr_s3_client do |awscr_s3_client|
          list1 = EasyAwscr::S3::Client.from_native_client(awscr_s3_client).list_buckets
          list2 = EasyAwscr::S3::Client.from_native_client(awscr_s3_client).list_buckets
          list1.buckets.should eq(list2.buckets)
        end
      end

      it "should integrate with the normal client" do
        with_client do |client|
          with_temp_test_bucket(client) do |bucket|
            with_native_awscr_s3_client do |awscr_s3_client|
              native_client = EasyAwscr::S3::Client.from_native_client(awscr_s3_client)

              client.head_bucket(bucket).should be_true
              native_client.head_bucket(bucket).should be_true
            end
          end
        end
      end
    end
  end
end
