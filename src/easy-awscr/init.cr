# Shared by every entry point of this shard. The sub-projects
# ("easy-awscr/s3" and "easy-awscr/cloudwatch") require this file
# themselves, so each of them can be required on its own.
require "log"

module EasyAwscr
  VERSION = {{ `shards version "#{__DIR__}"`.chomp.stringify }}
  Log     = ::Log.for("easy-awscr")
end
