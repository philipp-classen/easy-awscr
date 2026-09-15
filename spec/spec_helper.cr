require "spec"
require "../src/easy-awscr"

def env_set?(name)
  {"yes", "true", "1"}.includes?(ENV[name]?.try &.downcase)
end

def test_provider(access_key = "admin", secret_access_key = "password") : EasyAwscr::Config::Provider
  EasyAwscr::Config::Provider.new(
    Aws::Credentials::Providers.new([
      Aws::Credentials::SimpleCredentials.new(access_key, secret_access_key).as(Aws::Credentials::Provider),
    ])
  )
end

# Waits until the block returns true. Conditions can take a while to hold
# (background fibers, AWS propagation), so poll instead of a fixed sleep.
def wait_until(timeout : Time::Span = 5.seconds, & : -> Bool) : Nil
  deadline = Time.utc + timeout
  until yield
    raise "condition not met within #{timeout}" if Time.utc > deadline
    sleep 5.milliseconds
  end
end
