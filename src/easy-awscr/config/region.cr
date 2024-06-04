module EasyAwscr::Config
  def self.default_region : String?
    ENV["AWS_REGION"]? || ENV["AWS_DEFAULT_REGION"]?
  end

  def self.default_region! : String
    default_region || raise "either specify a region, or define the environment variable AWS_REGION"
  end
end
