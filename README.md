# easy-awscr

A Crystal shared intendend to provide basic AWS functionality:
* S3 (based on [awscr-s3](https://github.com/taylorfinnell/awscr-s3))
* Credentials (based on [aws-credentials](https://github.com/y2k2mt/aws-credentials.cr))

The idea is to simply the setup:
* It should work out of the box
* The library should take care of acquiring AWS credentials and refreshing if they expire
* Includes pull requests from forks providing missing features and bug fixes

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

# upload a file and read its content again
client.put_object(test_bucket, "some_file", "Yes, it worked!")
content = client.get_object(test_bucket, "some_file").body
puts "Did it work? #{content}"

all_files = client.list_objects(test_bucket).each &.contents.map(&.key)
puts "Files: #{all_files}"

# delete the file
client.delete_object(test_bucket, "some_file")

# delete the test bucket
client.delete_bucket(test_bucket)
```

## Contributing

1. Fork it (<https://github.com/your-github-user/easy-awscr/fork>)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## Contributors

- [Philipp Claßen](https://github.com/philipp-classen) - creator and maintainer
