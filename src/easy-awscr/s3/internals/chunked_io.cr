module EasyAwscr::S3::Internals
  # Write-only IO that will build configurable chunks (e.g. 5 MB blocks) and
  # forward them to a customizable Handler that will handle the block.
  class ChunkedIO < IO
    abstract class Handler
      # Will be called once before the first write operation.
      def open : Nil
      end

      # Can be called multiple times. The input is the current chunk.
      # The implementer can either process it synchronously or asynchronously.
      # Optionally, the function can return a buffer (either the input buffer or
      # another buffer of equal size) that the `ChunkedIO` class can reuse.
      # However, it is always safe to return `nil` and let the class
      # reallocate a new buffer instead.
      abstract def write(buffer : IO::Memory) : IO::Memory?

      # Will be called once after the last write operation.
      def close : Nil
      end
    end

    def initialize(@chunk_size : Int32, @handler : Handler)
      @buffer = IO::Memory.new(@chunk_size)
      @closed = false
      @opened = false
    end

    def read(slice : Bytes)
      raise IO::Error.new("Write-only stream")
    end

    def write(slice : Bytes) : Nil
      check_open

      while !slice.empty?
        remaining_capacity = @chunk_size - @buffer.size
        if remaining_capacity > 0
          write_size = {slice.size, remaining_capacity}.min
          @buffer.write(slice[0, write_size])
          slice = slice[write_size..]
        end
        flush if @buffer.size == @chunk_size
      end
    end

    def flush
      return if @buffer.empty?

      if !@opened
        @handler.open
        @opened = true
      end

      # wait until blocks are filled (unless for the final block)
      return if @buffer.size < @chunk_size && !@closed

      @buffer.rewind
      @buffer = @handler.write(@buffer) || IO::Memory.new(@chunk_size)
      @buffer.clear
    end

    def close
      return if closed?

      @closed = true
      flush
      @handler.close
    end

    def closed?
      @closed
    end
  end
end
