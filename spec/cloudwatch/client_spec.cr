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

  it "creates, lists and deletes alarms" do
    with_cloudwatch_client do |client|
      with_temp_namespace do |namespace|
        alarm_name = "test-easy-awscr-tmp-alarm-#{UUID.random}"
        client.put_metric_alarm(alarm_name,
          namespace: namespace, metric_name: "requests.errors", statistic: "Sum", period: 60,
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
