require "awscr-cloudwatch"
require "./internals/connection_pool"

module EasyAwscr::CloudWatch
  # CloudWatch with credentials and connections taken care of.
  #
  # The everyday calls are wrapped below and documented in `Awscr::CloudWatch`.
  # Everything else is reachable through `#with_native_client`.
  #
  # ```
  # client = EasyAwscr::CloudWatch::Client.new
  # client.put_counter("MyApp", "Requests")
  # client.describe_alarms(alarm_name_prefix: "MyApp").metric_alarms.each { |a| puts a.alarm_name }
  # ```
  class Client
    @native_client : Awscr::CloudWatch::Client?
    @client_factory : Internals::ConnectionPool?

    # An optional hook for managing the native CloudWatch client ("awscr-cloudwatch"
    # library) yourself, instead of letting "easy-awscr" handle it internally.
    #
    # The first parameter is a hint whether the connections or credentials should
    # be refreshed. It is only a hint, and implementations are allowed to ignore it.
    alias ClientProvider = Proc(Bool, Awscr::CloudWatch::Client)

    # From time to time, we should recreated the connection pool, so it will reload
    # the SSL context (see https://github.com/crystal-lang/crystal/issues/15419).
    DEFAULT_POOL_REFRESH_INTERVAL = 24.hours

    def initialize(*,
                   @region = EasyAwscr::Config.default_region!,
                   @credential_provider = EasyAwscr::Config.default_credential_provider,
                   @client_provider : ClientProvider? = nil,
                   @endpoint : String? = nil,
                   lazy_init = false)
      @mutex = Mutex.new(:unchecked)
      client unless lazy_init
    end

    # Converts an existing client from "awscr-cloudwatch" to use the "easy-awscr" client interface.
    def self.from_native_client(native_client : Awscr::CloudWatch::Client) : self
      new(client_provider: ->(_force_new : Bool) { native_client }, region: "us-east-1")
    end

    # Closes this client. If used again, a new connection will be opened.
    def close
      @mutex.synchronize do
        @client_factory.try &.close
      ensure
        @native_client = nil
        @client_factory = nil
      end
    end

    private def create_connection_pool
      # TODO: this uses the defaults, but it would make sense to
      # let the user overwrite them.
      Internals::ConnectionPool.new
    end

    # --- Metrics (see `Awscr::CloudWatch::MetricClient`) ---

    # Publishes up to 1000 data points (1 MB) in one request.
    def put_metric_data(namespace : String, metric_data : Array(Awscr::CloudWatch::MetricDatum)) : Nil
      try_with_refresh &.metrics.put_metric_data(namespace, metric_data)
    end

    # Publishes a single `Count` value.
    def put_counter(namespace : String, metric_name : String, value : Number = 1, **options) : Nil
      try_with_refresh &.metrics.put_counter(namespace, metric_name, value, **options)
    end

    def list_metrics(namespace : String? = nil, **options)
      try_with_refresh &.metrics.list_metrics(namespace, **options)
    end

    def get_metric_statistics(namespace : String, metric_name : String, **options)
      try_with_refresh &.metrics.get_metric_statistics(namespace, metric_name, **options)
    end

    def get_metric_data(queries : Array(Awscr::CloudWatch::MetricDataQuery), **options)
      try_with_refresh &.metrics.get_metric_data(queries, **options)
    end

    def get_metric_widget_image(metric_widget : String)
      try_with_refresh &.metrics.get_metric_widget_image(metric_widget)
    end

    def put_dashboard(dashboard_name : String, dashboard_body : String) : Nil
      try_with_refresh &.metrics.put_dashboard(dashboard_name, dashboard_body)
    end

    def get_dashboard(dashboard_name : String)
      try_with_refresh &.metrics.get_dashboard(dashboard_name)
    end

    def delete_dashboards(dashboard_names : Array(String)) : Nil
      try_with_refresh &.metrics.delete_dashboards(dashboard_names)
    end

    def list_dashboards(**options)
      try_with_refresh &.metrics.list_dashboards(**options)
    end

    def put_anomaly_detector(namespace : String, metric_name : String, stat : String, **options) : Nil
      try_with_refresh &.metrics.put_anomaly_detector(namespace, metric_name, stat, **options)
    end

    def describe_anomaly_detectors(**options)
      try_with_refresh &.metrics.describe_anomaly_detectors(**options)
    end

    def delete_anomaly_detector(namespace : String, metric_name : String, stat : String, **options) : Nil
      try_with_refresh &.metrics.delete_anomaly_detector(namespace, metric_name, stat, **options)
    end

    # --- Alarms (see `Awscr::CloudWatch::AlarmClient`) ---

    def put_metric_alarm(alarm_name : String, **options) : Nil
      try_with_refresh &.alarms.put_metric_alarm(alarm_name, **options)
    end

    def put_composite_alarm(alarm_name : String, alarm_rule : String, **options) : Nil
      try_with_refresh &.alarms.put_composite_alarm(alarm_name, alarm_rule, **options)
    end

    def describe_alarms(**options)
      try_with_refresh &.alarms.describe_alarms(**options)
    end

    def describe_alarms_for_metric(namespace : String, metric_name : String, **options)
      try_with_refresh &.alarms.describe_alarms_for_metric(namespace, metric_name, **options)
    end

    def describe_alarm_history(**options)
      try_with_refresh &.alarms.describe_alarm_history(**options)
    end

    def delete_alarms(alarm_names : Array(String)) : Nil
      try_with_refresh &.alarms.delete_alarms(alarm_names)
    end

    def set_alarm_state(alarm_name : String, state_value : String, state_reason : String, state_reason_data : String? = nil) : Nil
      try_with_refresh &.alarms.set_alarm_state(alarm_name, state_value, state_reason, state_reason_data)
    end

    def enable_alarm_actions(alarm_names : Array(String)) : Nil
      try_with_refresh &.alarms.enable_alarm_actions(alarm_names)
    end

    def disable_alarm_actions(alarm_names : Array(String)) : Nil
      try_with_refresh &.alarms.disable_alarm_actions(alarm_names)
    end

    # Runs the block with the native client, for everything that is not wrapped
    # above. Expired credentials are refreshed, like for the wrapped calls.
    #
    # ```
    # client.with_native_client &.metrics.put_metric_stream("Stream", "arn:aws:firehose:...", "arn:aws:iam::...", "JSON")
    # client.with_native_client &.alarms.tag_resource(alarm_arn, {"Env" => "prod"})
    # ```
    def with_native_client(& : Awscr::CloudWatch::Client -> T) : T forall T
      try_with_refresh do |native_client|
        yield native_client
      end
    end

    private def try_with_refresh(&)
      yield client
    rescue Awscr::CloudWatch::ExpiredTokenException
      yield client(force_new: true)
    end

    private def client(*, force_new = false) : Awscr::CloudWatch::Client
      @client_provider.try { |provider| return provider.call(force_new) }

      dead_client_factory = nil
      @mutex.synchronize do
        native_client = @native_client
        if native_client && !force_new && !client_factory_needs_refresh?
          native_client
        else
          cred = @credential_provider.credentials

          # refresh the connection pool (updates also the SSL context)
          client_factory = create_connection_pool
          dead_client_factory = @client_factory
          @client_factory = client_factory

          @native_client = Awscr::CloudWatch::Client.new(
            @region,
            cred.access_key_id,
            cred.secret_access_key,
            cred.session_token,
            endpoint: @endpoint,
            client_factory: client_factory
          )
        end
      end
    ensure
      dead_client_factory.try &.close
    end

    private def client_factory_needs_refresh? : Bool
      if cf = @client_factory
        Time.utc - cf.created_at > DEFAULT_POOL_REFRESH_INTERVAL
      else
        false
      end
    end
  end
end
