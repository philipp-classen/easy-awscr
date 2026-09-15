require "../spec_helper"
require "../support/cloudwatch_api"

# Records the uploads and can be told to fail, to test the failure paths of
# Counters without a backend. Skips the first *skip* requests, then fails
# *fail_next* requests.
class FlakyClient < EasyAwscr::CloudWatch::Client
  getter uploads = [] of Array(Awscr::CloudWatch::MetricDatum)
  property skip = 0
  property fail_next = 0

  def initialize
    super(region: "us-east-1", credential_provider: test_provider, lazy_init: true)
  end

  def put_metric_data(namespace : String, metric_data : Array(Awscr::CloudWatch::MetricDatum)) : Nil
    if @skip > 0
      @skip -= 1
    elsif @fail_next > 0
      @fail_next -= 1
      raise Awscr::CloudWatch::ApiException.new("InjectedUploadFailure")
    end
    @uploads << metric_data
  end
end

describe EasyAwscr::CloudWatch::Counters do
  it "keeps the counts when the upload fails" do
    client = FlakyClient.new
    counters = EasyAwscr::CloudWatch::Counters.new("TestApp", client: client, flush_interval: nil)
    counters.count("requests.total", 2)
    client.fail_next = 1
    counters.flush

    counters.pending.should eq({"requests.total" => 2})
    client.uploads.should be_empty

    counters.flush
    client.uploads.size.should eq 1
    client.uploads.first.map(&.metric_name).should eq ["requests.total"]
    counters.pending.should be_empty
  end

  it "re-adds only the batches that were not sent" do
    client = FlakyClient.new
    counters = EasyAwscr::CloudWatch::Counters.new("TestApp", client: client, flush_interval: nil)
    1500.times { |i| counters.count("metric#{i}") }
    client.skip = 1      # the first request succeeds
    client.fail_next = 1 # the second fails
    counters.flush

    client.uploads.size.should eq 1 # the first batch went out
    client.uploads.first.size.should eq 1000
    counters.pending.size.should eq 500                  # only the second batch is kept
    counters.pending.has_key?("metric0").should be_false # sent counters are not re-added
    counters.pending.has_key?("metric1499").should be_true

    counters.flush
    client.uploads.size.should eq 2
    client.uploads.last.size.should eq 500
    counters.pending.should be_empty
  end

  it "uploads exactly once on close" do
    client = FlakyClient.new
    counters = EasyAwscr::CloudWatch::Counters.new("TestApp", client: client, flush_interval: nil)
    counters.count("requests.total")
    counters.close
    counters.close # ignored
    counters.flush # ignored
    counters.count("after.close")
    counters.flush # ignored

    client.uploads.size.should eq 1
    client.uploads.first.map(&.metric_name).should eq ["requests.total"]
    counters.pending.should eq({"after.close" => 1})
  end

  it "aggregates counts per metric and uploads them on flush" do
    with_cloudwatch_client do |client|
      with_temp_namespace do |namespace|
        counters = EasyAwscr::CloudWatch::Counters.new(namespace, client: client, flush_interval: 1.hour)
        3.times { counters.count("requests.total") }
        2.times { counters.count("requests.errors") }
        counters.count("requests.bytes_processed", 512)
        counters.flush

        wait_for_counter(client, namespace, "requests.total", 3)
        wait_for_counter(client, namespace, "requests.errors", 2)
        wait_for_counter(client, namespace, "requests.bytes_processed", 512)
        counters.pending.should be_empty
      end
    end
  end

  it "attaches the configured dimensions" do
    with_cloudwatch_client do |client|
      with_temp_namespace do |namespace|
        dimensions = {"Environment" => "Dev"}
        counters = EasyAwscr::CloudWatch::Counters.new(namespace, client: client,
          flush_interval: 1.hour, dimensions: dimensions)
        counters.count("requests.total")
        counters.flush

        wait_for_counter(client, namespace, "requests.total", 1, dimensions)
        counter_sum(client, namespace, "requests.total", {"Environment" => "Prod"}).should eq 0.0
      end
    end
  end

  it "uploads more than 1000 metrics" do
    with_cloudwatch_client do |client|
      with_temp_namespace do |namespace|
        counters = EasyAwscr::CloudWatch::Counters.new(namespace, client: client, flush_interval: 1.hour)
        1500.times { |i| counters.count("metric#{i}") }
        counters.flush

        # One request takes at most 1000 metrics, so check one metric of each batch.
        wait_for_counter(client, namespace, "metric0", 1)
        wait_for_counter(client, namespace, "metric1499", 1)
      end
    end
  end

  it "flushes in the background" do
    with_cloudwatch_client do |client|
      with_temp_namespace do |namespace|
        counters = EasyAwscr::CloudWatch::Counters.new(namespace, client: client, flush_interval: 20.milliseconds)
        counters.count("requests.total")
        wait_for_counter(client, namespace, "requests.total", 1) # no flush call needed
        counters.close
      end
    end
  end

  it "flushes what is left on close" do
    with_cloudwatch_client do |client|
      with_temp_namespace do |namespace|
        counters = EasyAwscr::CloudWatch::Counters.new(namespace, client: client, flush_interval: 1.hour)
        counters.count("requests.total")
        counters.close
        wait_for_counter(client, namespace, "requests.total", 1)
      end
    end
  end

  it "uploads only on flush when no flush loop is configured" do
    with_cloudwatch_client do |client|
      with_temp_namespace do |namespace|
        counters = EasyAwscr::CloudWatch::Counters.new(namespace, client: client, flush_interval: nil)
        counters.count("requests.total")
        100.times { Fiber.yield } # a wrongly started loop would have flushed here
        counters.pending.should eq({"requests.total" => 1})

        counters.flush
        wait_for_counter(client, namespace, "requests.total", 1)
      end
    end
  end

  it "reports closed after close, with and without a flush loop" do
    with_cloudwatch_client do |client|
      with_temp_namespace do |namespace|
        with_loop = EasyAwscr::CloudWatch::Counters.new(namespace, client: client, flush_interval: 1.hour)
        with_loop.closed?.should be_false
        with_loop.close
        with_loop.closed?.should be_true

        without_loop = EasyAwscr::CloudWatch::Counters.new(namespace, client: client, flush_interval: nil)
        without_loop.closed?.should be_false
        without_loop.close
        without_loop.closed?.should be_true # even without a loop, close sets the flag
      end
    end
  end

  it "ignores flush and close after close" do
    with_cloudwatch_client do |client|
      with_temp_namespace do |namespace|
        counters = EasyAwscr::CloudWatch::Counters.new(namespace, client: client, flush_interval: nil)
        counters.count("requests.total")
        counters.close
        wait_for_counter(client, namespace, "requests.total", 1) # exactly this flush happened

        counters.count("after.close")
        counters.flush # ignored
        counters.close # ignored
        counters.pending.should eq({"after.close" => 1})
        counter_sum(client, namespace, "after.close").should eq 0.0 # never uploaded
      end
    end
  end

  it "stops the background flush on close" do
    with_cloudwatch_client do |client|
      with_temp_namespace do |namespace|
        # A zero interval makes the flush loop runnable immediately, so plain
        # yields (instead of sleeps) drive it deterministically.
        counters = EasyAwscr::CloudWatch::Counters.new(namespace, client: client, flush_interval: 0.seconds)
        counters.count("requests.total")
        wait_for_counter(client, namespace, "requests.total", 1) # the loop is up and has flushed

        counters.close
        counters.count("after.close")
        100.times { Fiber.yield } # a still-running loop would have flushed again
        counters.pending.should eq({"after.close" => 1})
      end
    end
  end
end
