require "./chunked_io"

module EasyAwscr::S3
  module Internals
    # This is an experimental implementation. Be warned that error handling is
    # non-existant. The performance should be comparable to the AWS cli tool.
    class AsyncChunkUploader < ChunkedIO::Handler
      private record JobResult,
        resp : Awscr::S3::Response::UploadPartOutput,
        buffer : IO::Memory

      @upload_id = ""
      @parts_started = 0
      @uploaded_parts = Array(Awscr::S3::Response::UploadPartOutput).new

      @jobs_scheduled = Atomic(Int32).new(0)
      @jobs_finished = Atomic(Int32).new(0)
      @job_results = Channel(JobResult).new

      def initialize(@client : Client, @bucket : String, @object : String,
                     @headers = Hash(String, String).new, *, @max_workers = 8)
        raise "max_workers must be greater than zero, but got #{@max_workers}" unless @max_workers > 0
      end

      def open : Nil
        resp = @client.start_multipart_upload(@bucket, @object, @headers)
        @upload_id = resp.upload_id
      end

      def write(buffer : IO::Memory) : IO::Memory?
        part_number = (@parts_started += 1) # Note: AWS counts parts starting from 1
        pending_jobs = @jobs_scheduled.add(1)
        buffer_to_recycle = nil

        # 1) If there jobs ready, we should always collect them, just to reuse the buffer.
        # 2) If we maxed out already the number of jobs, then we also need to collect one job.
        #
        # In the first case, it will not block. Only the second case will block (as intended to throttle).
        max_workers_reached = (pending_jobs + 1) == @max_workers
        if max_workers_reached || @jobs_finished.get > @uploaded_parts.size
          job_result = @job_results.receive
          @jobs_scheduled.add(-1)
          @uploaded_parts << job_result.resp
          buffer_to_recycle = job_result.buffer
        end

        spawn do
          begin
            resp = @client.upload_part(@bucket, @object, @upload_id, part_number, buffer)
            @jobs_finished.add(1)
            @job_results.send(JobResult.new(resp, buffer))
          rescue e
            if @job_results.close
              Log.warn(exception: e) { "Unable to upload object s3://#{@bucket}/@{object}" }
            end
          end
        end

        buffer_to_recycle
      end

      def close : Nil
        while @uploaded_parts.size < @parts_started
          job_result = @job_results.receive
          @uploaded_parts << job_result.resp
        end
        @job_results.close

        # The upload of individual parts can finish out of order, but the AWS API
        # expects it in the correct order. We could do a full sort, but here it is
        # easier because we know already the indices.
        # Additional pitfall: AWS counts parts starting from 1, not 0!
        0.upto(@uploaded_parts.size - 2) do |idx|
          while @uploaded_parts[idx].part_number != idx + 1
            correct_idx = @uploaded_parts[idx].part_number - 1
            @uploaded_parts[idx], @uploaded_parts[correct_idx] = @uploaded_parts[correct_idx], @uploaded_parts[idx]
          end
        end

        @client.complete_multipart_upload(@bucket, @object, @upload_id, @uploaded_parts)
      ensure
        @job_results.close
      end
    end
  end
end
