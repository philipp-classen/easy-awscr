require "uuid"

record CloudwatchApi,
  id : Symbol,
  credential_provider : EasyAwscr::Config::Provider,
  region : String,
  endpoint : String? = nil

def with_cloudwatch_api(& : CloudwatchApi -> Nil)
  found = 0

  # Uses a local, CloudWatch-compatible mock (https://github.com/getmoto/moto).
  # Tip: you can start a local Docker instance by running:
  # $ make start-moto
  # AWS_ENDPOINT_URL points the specs at a different mock (default: http://127.0.0.1:4566).
  if env_set? "EASY_AWSCR_SPEC_USE_CLOUDWATCH_MOCK"
    found += 1
    yield CloudwatchApi.new(
      id: :mock,
      credential_provider: test_provider("test", "test"),
      region: "us-east-1",
      endpoint: ENV.fetch("AWS_ENDPOINT_URL", "http://127.0.0.1:4566")
    )
  end

  # Be careful when running this against a real account, especially against a
  # production account. Prefer a test account, but note that using the real
  # API will create some costs.
  #
  # The tests write only into namespaces with a dedicated test prefix to limit
  # the blast area. Unlike S3 buckets, published metrics cannot be deleted,
  # so test data stays behind in the account. Still, please check the tests
  # and run them only if you know what you are doing.
  if env_set? "EASY_AWSCR_SPEC_USE_NATIVE_AWS__I_KNOW_THE_RISK__"
    found += 1
    yield CloudwatchApi.new(
      id: :native_aws,
      credential_provider: EasyAwscr::Config.default_credential_provider,
      region: EasyAwscr::Config.default_region!
    )
  end

  if found == 0
    puts "\nWARNING: No CloudWatch API available. CloudWatch tests will be skipped."
  end
end

# TODO: replace by this (but this currently crashes the Crystal compiler
# while a spec subclasses Client, see FlakyClient):
#
#   def with_cloudwatch_client(& : EasyAwscr::CloudWatch::Client -> Nil)
def with_cloudwatch_client(&)
  with_cloudwatch_api do |api|
    client = EasyAwscr::CloudWatch::Client.new(
      region: api.region,
      endpoint: api.endpoint,
      credential_provider: api.credential_provider
    )
    begin
      yield client
    ensure
      client.close
    end
  end
end

# Every test gets a fresh namespace. Unlike S3 objects, published metrics
# cannot be deleted, so a unique name is the only way to keep test runs
# from interfering with each other.
def with_temp_namespace(& : String -> Nil)
  yield "test-easy-awscr-tmp-namespace-#{UUID.random}"
end

# Reads back the sum of a counter.
def counter_sum(client : EasyAwscr::CloudWatch::Client, namespace : String, metric_name : String,
                dimensions : Hash(String, String)? = nil) : Float64
  client.get_metric_statistics(namespace, metric_name,
    start_time: Time.utc - 5.minutes, end_time: Time.utc + 5.minutes, period: 60,
    statistics: ["Sum", "SampleCount"], dimensions: dimensions
  ).datapoints.sum { |datapoint| datapoint.sum || 0.0 }
end

# Newly published metrics become queryable with a delay on real AWS.
# Local mocks answer immediately.
def cloudwatch_read_timeout : Time::Span
  env_set?("EASY_AWSCR_SPEC_USE_NATIVE_AWS__I_KNOW_THE_RISK__") ? 2.minutes : 5.seconds
end

def wait_for_counter(client : EasyAwscr::CloudWatch::Client, namespace : String, metric_name : String,
                     expected : Number, dimensions : Hash(String, String)? = nil) : Nil
  wait_until(cloudwatch_read_timeout) { counter_sum(client, namespace, metric_name, dimensions) == expected.to_f }
end
