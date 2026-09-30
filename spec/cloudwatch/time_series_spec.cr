require "../spec_helper"
require "../support/cloudwatch_api"

# Answers get_metric_data with canned pages, in order, and records the requests.
class PagingClient < EasyAwscr::CloudWatch::Client
  record Request, queries : Array(Awscr::CloudWatch::MetricDataQuery), scan_by : String?, next_token : String?

  getter requests = [] of Request
  @pages : Deque(Awscr::CloudWatch::Response::GetMetricDataOutput)

  def initialize(pages : Array(Awscr::CloudWatch::Response::GetMetricDataOutput))
    super(region: "us-east-1", credential_provider: test_provider, lazy_init: true)
    @pages = Deque.new(pages)
  end

  def get_metric_data(queries : Array(Awscr::CloudWatch::MetricDataQuery), **options)
    @requests << Request.new(queries, options[:scan_by]?, options[:next_token]?)
    @pages.shift
  end
end

private alias Results = Array({String, Array(Time), Array(Float64)})

# One page of results (id, timestamps, values), with *next_token* when more pages follow.
private def page(results : Results, next_token : String? = nil) : Awscr::CloudWatch::Response::GetMetricDataOutput
  data = results.map do |id, timestamps, values|
    Awscr::CloudWatch::Response::MetricDataResult.new(
      id: id, label: nil, timestamps: timestamps, values: values,
      status_code: next_token ? "PartialData" : "Complete",
      messages: [] of Awscr::CloudWatch::Response::MessageData)
  end
  Awscr::CloudWatch::Response::GetMetricDataOutput.new(data, [] of Awscr::CloudWatch::Response::MessageData, next_token, HTTP::Client::Response.new(200))
end

private def hour(n : Int) : Time
  Time.utc(2026, 9, 1) + n.hours
end

describe EasyAwscr::CloudWatch::Client do
  describe "#each_metric_data" do
    it "follows the next token and yields every result of every page" do
      client = PagingClient.new([
        page([{"a", [hour(0)], [1.0]}, {"b", [hour(0)], [2.0]}], next_token: "page2"),
        page([{"a", [hour(1)], [3.0]}, {"b", [] of Time, [] of Float64}]),
      ])
      query = Awscr::CloudWatch::MetricDataQuery.new("a", expression: "SUM(METRICS())")

      seen = [] of {String, Array(Float64)}
      client.each_metric_data([query], start_time: hour(0), end_time: hour(2)) { |result| seen << {result.id, result.values} }

      seen.should eq [{"a", [1.0]}, {"b", [2.0]}, {"a", [3.0]}, {"b", [] of Float64}]
      client.requests.map(&.next_token).should eq [nil, "page2"]
      client.requests.map(&.queries).should eq [[query], [query]]
      client.requests.map(&.scan_by).should eq ["TimestampAscending", "TimestampAscending"]
    end
  end

  describe "#each_datapoint" do
    it "yields the points of every metric in time order, across pages" do
      requests = Awscr::CloudWatch::Metric.new("MyApp", "requests", {"Env" => "test"})
      errors = Awscr::CloudWatch::Metric.new("MyApp", "errors", {"Env" => "test"})
      client = PagingClient.new([
        page([{"m0", [hour(0), hour(1)], [10.0, 11.0]}, {"m1", [hour(0)], [1.0]}], next_token: "page2"),
        page([{"m0", [hour(2)], [12.0]}, {"m1", [hour(1), hour(2)], [2.0, 3.0]}]),
      ])

      seen = [] of {String, Time, Float64}
      client.each_datapoint([requests, errors], stat: "Sum", period: 1.hour, start_time: hour(0), end_time: hour(3)) do |metric, time, value|
        seen << {metric.metric_name, time, value}
      end

      seen.select { |name, _, _| name == "requests" }.should eq [{"requests", hour(0), 10.0}, {"requests", hour(1), 11.0}, {"requests", hour(2), 12.0}]
      seen.select { |name, _, _| name == "errors" }.should eq [{"errors", hour(0), 1.0}, {"errors", hour(1), 2.0}, {"errors", hour(2), 3.0}]

      query = client.requests.first.queries.first
      query.id.should eq "m0"
      query.metric_stat.should eq Awscr::CloudWatch::MetricStat.new(requests, 1.hour, "Sum")
    end

    it "sends at most 500 metrics per request" do
      metrics = Array.new(501) { |i| Awscr::CloudWatch::Metric.new("MyApp", "metric#{i}") }
      client = PagingClient.new([
        page([{"m499", [hour(0)], [499.0]}]),
        page([{"m0", [hour(0)], [500.0]}]), # the second batch starts with fresh ids
      ])

      seen = [] of {String, Float64}
      client.each_datapoint(metrics, stat: "Sum", period: 1.minute, start_time: hour(0), end_time: hour(1)) do |metric, _, value|
        seen << {metric.metric_name, value}
      end

      seen.should eq [{"metric499", 499.0}, {"metric500", 500.0}]
      client.requests.map(&.queries.size).should eq [500, 1]
    end

    it "rejects periods that CloudWatch rejects, before any request" do
      client = PagingClient.new([] of Awscr::CloudWatch::Response::GetMetricDataOutput)
      metric = Awscr::CloudWatch::Metric.new("MyApp", "requests")
      [0.seconds, -1.minute, 15.seconds, 90.seconds, 1.5.seconds].each do |period|
        expect_raises(ArgumentError, /multiple of 60 seconds/) do
          client.each_datapoint([metric], stat: "Sum", period: period, start_time: hour(0), end_time: hour(1)) { }
        end
      end
      client.requests.should be_empty
    end
  end
end
