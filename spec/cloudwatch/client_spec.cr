require "../spec_helper"
require "../support/cloudwatch_api"

describe EasyAwscr::CloudWatch::Client do
  it "publishes counters and reads them back" do
    with_cloudwatch_client do |client|
      with_temp_namespace do |namespace|
        client.put_counter(namespace, "requests.total", 3)
        wait_for_counter(client, namespace, "requests.total", 3)
      end
    end
  end

  it "reads a time series with each_datapoint" do
    with_cloudwatch_client do |client|
      with_temp_namespace do |namespace|
        client.put_counter(namespace, "requests.total", 3)
        wait_for_counter(client, namespace, "requests.total", 3)

        metric = Awscr::CloudWatch::Metric.new(namespace, "requests.total")
        values = [] of Float64
        client.each_datapoint([metric], stat: "Sum", period: 1.minute,
          start_time: Time.utc - 5.minutes, end_time: Time.utc + 5.minutes) { |_, _, value| values << value }
        values.sum.should eq 3.0
      end
    end
  end

  it "creates, lists and deletes alarms" do
    with_cloudwatch_client do |client|
      with_temp_namespace do |namespace|
        alarm_name = "test-easy-awscr-tmp-alarm-#{UUID.random}"
        client.put_metric_alarm(alarm_name,
          namespace: namespace, metric_name: "requests.errors", statistic: "Sum", period: 1.minute,
          evaluation_periods: 3, threshold: 100, comparison_operator: "GreaterThanThreshold",
          actions_enabled: false)
        begin
          client.describe_alarms(alarm_names: [alarm_name]).metric_alarms.map(&.alarm_name).should eq [alarm_name]
        ensure
          client.delete_alarms([alarm_name])
        end
        wait_until(cloudwatch_read_timeout) { client.describe_alarms(alarm_names: [alarm_name]).metric_alarms.empty? }
      end
    end
  end

  it "reaches everything else through the native client" do
    with_cloudwatch_client do |client|
      with_temp_namespace do |namespace|
        client.with_native_client do |native_client|
          native_client.metrics.put_counter(namespace, "requests.total", 2)
        end
        wait_for_counter(client, namespace, "requests.total", 2)
      end
    end
  end
end
