require "log"

module EasyAwscr
  VERSION = {{ `shards version "#{__DIR__}"`.chomp.stringify }}
  Log     = ::Log.for("easy-awscr")
end

require "./easy-awscr/*"
