require "spec"
require "../src/easy-awscr"

def env_set?(name)
  {"yes", "true", "1"}.includes?(ENV[name]?.try &.downcase)
end
