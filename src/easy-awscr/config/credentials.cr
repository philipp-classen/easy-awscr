require "aws-credentials"

module EasyAwscr::Config
  class Provider
    def initialize(@provider = Aws::Credentials::Providers.new([
                     Aws::Credentials::EnvProvider.new,
                     Aws::Credentials::SharedCredentialFileProvider.new,
                     Aws::Credentials::InstanceMetadataProvider.new,
                   ] of Aws::Credentials::Provider))
      @mutex = Mutex.new
    end

    def credentials
      @mutex.synchronize { @provider.credentials }
    end
  end

  def self.default_credential_provider
    @@default_provider ||= Provider.new
  end
end
