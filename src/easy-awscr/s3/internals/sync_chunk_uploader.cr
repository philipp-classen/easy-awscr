require "./chunked_io"

module EasyAwscr::S3
  module Internals
    # If performance is relevant, `AsyncChunkUploader` should be much better. However,
    # this implementation is straightforward; benefits are blocking behavior and minimal
    # memory overhead. It can also be useful for debugging purposes, since it avoids
    # a lot of complexity of the other multi-worker implementation.
    class SyncChunkUploader < ChunkedIO::Handler
      @parts = Array(Awscr::S3::Response::UploadPartOutput).new
      @upload_id = ""

      def initialize(@client : Client, @bucket : String, @object : String, @headers = Hash(String, String).new)
      end

      def open : Nil
        resp = @client.start_multipart_upload(@bucket, @object, @headers)
        @upload_id = resp.upload_id
      end

      def write(buffer : IO::Memory) : IO::Memory?
        part_number = @parts.size + 1 # counting starts at 1
        resp = @client.upload_part(@bucket, @object, @upload_id, part_number, buffer)
        @parts << resp
        buffer
      end

      def close : Nil
        @client.complete_multipart_upload(@bucket, @object, @upload_id, @parts)
      end
    end
  end
end
