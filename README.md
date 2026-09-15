# easy-awscr

A Crystal shard intended to provide basic AWS functionality:
* S3 (based on [awscr-s3](https://github.com/taylorfinnell/awscr-s3))
* CloudWatch metrics and alarms (based on [awscr-cloudwatch](https://github.com/philipp-classen/awscr-cloudwatch))
* Credentials (based on [aws-credentials](https://github.com/y2k2mt/aws-credentials.cr))

The idea is to simplify the setup:
* It should work out of the box
* The library should take care of acquiring and refreshing AWS credentials

It is not intended to be feature-rich, but rather to put the existing pieces together.
Currently, it is expected to work on an EC2 instance (using IAM roles) or in a local setup
where you provide credentials either through environment variables or through `~/.aws/config`.

Apart from the low-level API, there are some higher-level APIs provided:
* S3
  - support for streaming uploads
* CloudWatch
  - period uploads of aggregated counters

## Installation

1. Add the dependency to your `shard.yml`:

   ```yaml
   dependencies:
     easy-awscr:
       github: philipp-classen/easy-awscr
   ```

2. Run `shards install`

## Usage

`require "easy-awscr"` loads all sub-projects. If you want to keep control over
what gets loaded, select the sub-project you need explicitly, e.g.
`require "easy-awscr/s3"` or `require "easy-awscr/cloudwatch"`.

```crystal
require "easy-awscr/s3"

client = EasyAwscr::S3::Client.new

# create a test bucket
test_bucket = "some-test-bucket-#{rand(10000000)}"
client.put_bucket(test_bucket)
if client.list_buckets.buckets.includes?(test_bucket)
  puts "Found!"
else
  raise "Something went wrong"
end

# upload text and read the content again
# Note: Crystal uses Int32 for strings and arrays. It will work up to around 2gb.
client.put_object(test_bucket, "some_file", "Yes, it worked!")
content = client.get_object(test_bucket, "some_file").body
puts "Did it work? #{content}"

# Otherwise, can upload a large file like this ...
File.open("/path/some/big/file_over_4gb.txt") do |io|
  success = client.upload_file("bucket1", "obj", io)
  p success # => true
end

# ... and download it again by streaming into a file again.
File.open("/path/some/big/file-downloaded.txt", "w") do |io|
  client.get_object(test_bucket, "some_file") do |resp|
	IO.copy(resp.body_io, io)
  end
end

# If you do not know the size in advance, you can use the streaming API:
client.stream_to_s3(test_bucket, "some_file") do |io|
  io.puts "Some content"
end

# Or like this if you need more flexibility over the lifecycle:
io = client.stream_to_s3(test_bucket, "some_file", auto_close: false) { |io| io }
io.puts "Some content"
io.close

# list all files (optionally you can filter with `prefix` and limit with `max_keys`)
all_files = [] of String
client.list_objects(test_bucket).each do |batch|
  all_files.concat(batch.contents.map &.key)
end
p! all_files

# delete the file
client.delete_object(test_bucket, "some_file")

# delete the test bucket
client.delete_bucket(test_bucket)
```

### CloudWatch

```crystal
require "easy-awscr/cloudwatch"

client = EasyAwscr::CloudWatch::Client.new

stats = client.get_metric_statistics("MyApp", "requests.total",
  start_time: Time.utc - 1.hour, end_time: Time.utc, period: 300, statistics: ["Sum"])
stats.datapoints.each { |dp| puts "#{dp.timestamp}: #{dp.sum}" }

client.put_metric_alarm("MyApp-HighErrorRate",
  namespace: "MyApp", metric_name: "requests.errors", statistic: "Sum", period: 60,
  evaluation_periods: 3, threshold: 100, comparison_operator: "GreaterThanThreshold")

# On top of that, Counters provides a higher-level API:
# - events are aggregated in memory and uploaded once a minute (one aggregated data point per metric)
# - configure once, then only count (safe from any fiber)
metrics = EasyAwscr::CloudWatch::Counters.new("MyApp", dimensions: {"Environment" => "Dev"})
metrics.count("requests.total")
metrics.count("requests.bytes_processed", 512)
metrics.close # uploads what is left, e.g. at shutdown

# Note: If needed, there is also low-level access to the native client of the
# `awscr-cloudwatch` library:
client.with_native_client do |native_client|
  # ...
end
```

See `Awscr::CloudWatch::MetricClient` and `Awscr::CloudWatch::AlarmClient` for
all parameters.

## Development

The bulk of the work is done by the libraries `aws-credentials.cr`, `awscr-s3`
and `awscr-cloudwatch`.

### How to run tests

Run tests:

```
crystal spec --verbose --tag '~slow'
```

Run all tests, including slow, memory-intensive test:

```
crystal spec --verbose
```

Warning: The slow tests upload multi-GB files. They are meant for real AWS.
Be aware that they might overwhelm a resourced-constrained test environment.

Note: alias for these commands can also be found in the `Makefile` (e.g. `make test`).

The specs run against an AWS-compatible service provided by the test
environment. Without one, the affected tests are skipped:

* `EASY_AWSCR_SPEC_USE_S3_MOCK=1` runs the S3 specs, and
  `EASY_AWSCR_SPEC_USE_CLOUDWATCH_MOCK=1` the CloudWatch specs, against a local
  mock (e.g. started with `make start-moto`). `AWS_ENDPOINT_URL` points the
  specs at a different mock (default: `http://127.0.0.1:4566`).
* `EASY_AWSCR_SPEC_USE_NATIVE_AWS__I_KNOW_THE_RISK__=1` runs them against a real
  AWS account. Be careful: that publishes metrics, which cannot be deleted
  again.

The tests write only into buckets, namespaces and alarms with a
`test-easy-awscr-tmp` prefix. Buckets, objects and alarms are deleted again.
Published metrics stay behind in the account.

## Contributing

1. Fork it (<https://github.com/philipp-classen/easy-awscr/fork>)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## Contributors

- [Philipp Claßen](https://github.com/philipp-classen) - creator and maintainer
