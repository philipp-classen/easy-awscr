require "./client"

module EasyAwscr::CloudWatch
  # Counts events in memory and uploads them to CloudWatch: one data point per
  # metric and flush interval, so a busy application sends one request per
  # minute instead of one per event.
  #
  # By default, a background loop uploads the counters periodically. With
  # `flush_interval: nil` there is no loop and `flush` uploads on demand.
  #
  # Configure once, afterwards it is only `count`:
  #
  # ```
  # metrics = EasyAwscr::CloudWatch::Counters.new("MyApp")
  # metrics.count("Requests")
  # ```
  class Counters
    # AWS allows at most 1000 data points and 1 MB per request. The size
    # grows with the length of the metric names and dimensions, but stays
    # well below 1 MB for typical counters (a full batch is around 300 KB).
    MAX_METRICS_PER_REQUEST = 1000

    # *flush_interval* starts a background loop that uploads the counters
    # periodically (every minute by default). nil starts no loop: call `flush`
    # to upload instead.
    # *dimensions* are attached to every metric, e.g. {"Environment" => "Dev"}.
    def initialize(@namespace : String, *,
                   @client : Client = Client.new,
                   @flush_interval : Time::Span? = 1.minute,
                   @dimensions : Hash(String, String)? = nil)
      @counts = Hash(String, Int64).new(0_i64)
      @mutex = Mutex.new(:unchecked)
      @closed = Atomic(Bool).new(false)

      if flush_interval = @flush_interval
        Log.debug { "Flush loop started (namespace #{@namespace}, flush_interval #{flush_interval}, dimensions=#{@dimensions.inspect})" }
        spawn { flush_loop(flush_interval) }
      end
    end

    def count(name : String, by : Int = 1) : Nil
      add(name, by.to_i64)
    end

    # Counts not uploaded yet, by metric name. For debugging and tests.
    def pending : Hash(String, Int64)
      @mutex.synchronize { @counts.dup }
    end

    # Uploads everything counted so far. What could not be uploaded stays and
    # goes out with the next flush. Ignored after `close`.
    def flush : Nil
      upload
    end

    # Drains the counted events and uploads them. Checking `closed?` under the
    # same mutex as the drain closes the race with a concurrent `close`:
    # a flush can never claim counts anymore once close was called.
    # *final* is the one upload that `close` itself sends despite being closed.
    private def upload(*, final : Bool = false) : Nil
      counts = @mutex.synchronize do
        return if @closed.get && !final
        @counts.tap { @counts = Hash(String, Int64).new(0_i64) }
      end
      return if counts.empty?

      timestamp = Time.utc
      sent = 0
      counts.each_slice(MAX_METRICS_PER_REQUEST) do |batch|
        metric_data = batch.map { |name, count| Awscr::CloudWatch::MetricDatum.counter(name, count, @dimensions, timestamp) }
        begin
          @client.put_metric_data(@namespace, metric_data)
          sent += batch.size
        rescue ex
          Log.warn(exception: ex) { "Uploading counters to CloudWatch failed, pushing them back to pending" }
          counts.each_with_index do |(name, count), index|
            add(name, count) if index >= sent
          end
          return
        end
      end

      Log.debug do
        String.build do |io|
          io << "Uploaded counters:"
          counts.each do |name, count|
            io << "\n* " << name << ": " << count
          end
        end
      end
    end

    # Sends what is left and stops the background upload, e.g. at shutdown.
    # Later calls to `close` or `flush` are ignored.
    def close : Nil
      if !@closed.swap(true)
        Log.debug { "Flush loop stopped" } if @flush_interval
        upload(final: true)
      end
    end

    # Whether `close` was called. Once closed, nothing is uploaded anymore:
    # counters counted afterwards stay in `pending` (`flush` is a no-op).
    def closed? : Bool
      @closed.get
    end

    private def add(name : String, count : Int64) : Nil
      @mutex.synchronize { @counts[name] += count }
    end

    private def flush_loop(flush_interval : Time::Span)
      until closed?
        sleep flush_interval
        flush
      end
    end
  end
end
