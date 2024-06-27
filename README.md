# easy-awscr

A Crystal shard intended to provide basic AWS functionality:
* S3 (based on [awscr-s3](https://github.com/taylorfinnell/awscr-s3))
* Credentials (based on [aws-credentials](https://github.com/y2k2mt/aws-credentials.cr))

The idea is to simplify the setup:
* It should work out of the box
* The library should take care of acquiring and refreshing AWS credentials
* (currently needed) include open pull requests that provide missing features and bug fixes

It is not intended to be feature-rich, but rather to put the existing pieces together.
Currently, it is expected to work on an EC2 instance (using IAM roles); or in a local setup
where you provide credentials either through environment variables or through `~/.aws/config`.

## Installation

1. Add the dependency to your `shard.yml`:

   ```yaml
   dependencies:
     easy-awscr:
       github: philipp-classen/easy-awscr
   ```

2. Run `shards install`

## Usage

```crystal
require "easy-awscr"

client = EasyAwscr::S3::Client.new

# create a test bucket
test_bucket = "some-test-bucket-#{rand(10000000)}"
client.put_bucket(test_bucket)
if client.list_buckets.buckets.includes?(test_bucket)
  puts "Found!"
else
  puts "Something went wrong"
end

# upload text and read the content again
# Note: Crystal uses Int32 for buffer (works under 2gb)
client.put_object(test_bucket, "some_file", "Yes, it worked!")
content = client.get_object(test_bucket, "some_file").body
puts "Did it work? #{content}"

# Otherwise, do this to upload a large file ...
File.open("/path/some/big/file_over_4gb.txt") do |io|
  success = client.upload_file("bucket1", "obj", io)
  p success # => true
end

# ... and download it again
File.open("/path/some/big/file-downloaded.txt", "w") do |io|
  client.get_object(test_bucket, "some_file") do |resp|
	IO.copy(resp.body_io, io)
  end
end

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

## Development

The bulk of the work is done by the libraries `aws-credentials.cr` and `awscr-s3`.

I would prefer to adopt the original library. But until then, it is easier to get
a local repository since it allows to merge fixes immediately instead of waiting:

* https://github.com/philipp-classen/aws-credentials.cr/tree/fixes
* https://github.com/philipp-classen/awscr-s3/tree/fixes

## Contributing

1. Fork it (<https://github.com/philipp-classen/easy-awscr/fork>)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## Contributors

- [Philipp Claßen](https://github.com/philipp-classen) - creator and maintainer
