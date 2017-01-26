package org.albaross.io;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * A framework class which supports easy implementation of high-throughput parsers utilizing {@link ReadableByteChannel} and {@link ByteBuffer}.
 *
 * @param <OUT> output is meant to contain the result of the parsing process. It is usually some data structure (e.g. {@link
 *              java.util.Collection}) gathering parsed items or an analysis model class but may be also a data receiver.
 * @param <CTX> context is meant to temporarily store some kind of state during the parsing process. It may be an enum class representing simple
 *              states, a model class managing attributes or {@link Void} if no state is needed what so ever.
 * @author Manuel Barbi
 * @since 1.0.1
 */
public interface ChannelBasedParser<CTX, OUT> {

	default OUT read(File path) throws FileNotFoundException, IOException {
		try (InputStream in = new FileInputStream(path)) {
			return read(in);
		}
	}

	default OUT read(InputStream in) throws IOException {
		try (ReadableByteChannel src = (in instanceof FileInputStream) ? ((FileInputStream) in).getChannel() : Channels.newChannel(in)) {
			return read(src);
		}
	}

	/**
	 * Automatically fills the buffer and internally invokes the {@link ChannelBasedParser#process(ByteBuffer, Object, Object)} method repeatedly
	 * where the actual parsing takes place. Between two calls the buffer keeps unprocessed bytes and refills the remaining space.
	 *
	 * @param src the channel to ne parsed
	 * @return some kind of output containing the result of the parsing process
	 */
	default OUT read(ReadableByteChannel src) throws IOException {
		OUT output = initOutput();
		CTX context = initContext();
		ByteBuffer buf = ByteBuffer.allocate(getBufferSize());

		while (src.read(buf) > 0) {
			buf.flip();
			boolean hasChanged = true;

			while (buf.hasRemaining() && hasChanged) {
				int pos = buf.position();
				context = process(buf, context, output);
				hasChanged = buf.position() > pos;
			}

			buf.compact();
		}

		return output;
	}

	/**
	 * Parses the content of the buffer partial or complete. The {@link ChannelBasedParser#process(ByteBuffer, Object, Object)} method is called
	 * repeatedly as long as bytes are available in the buffer and some bytes were processed during the last call. Therefore it is legitimate to
	 * return due to a state change, even though some bytes are still unprocessed. Do not bother about the number of remaining bytes withing the
	 * buffer, {@link java.nio.BufferUnderflowException} is part of the control flow to refill the buffer. Just make sure to memorize the
	 * position after each executed step and if necessary reset the buffer to the last unprocessed position. A typical implementation may look
	 * something like this:
	 * <pre>
	 *  {@code
	 *      public SomeState process(ByteBuffer buf, SomeState context, Collection<SomeItem> output) {
	 *          int processed = buf.position();
	 *
	 *          try {
	 *              switch (context) {
	 *              case HEADER:
	 *                  boolean skip = parseHeader(buf);
	 *                  processed = buf.position();
	 *
	 *                  if (skip)
	 *                      return SomeState.SKIP;
	 *                  else
	 *                      return SomeState.CONTENT;
	 *              case CONTENT:
	 *                  for (; remaining > 0; remaining--) {
	 *                      output.add(parseItem(buf));
	 *                      processed = buf.position();
	 *                  }
	 *
	 *                  return SomeState.HEADER;
	 *              case SKIP:
	 *                  for (; remaining > 0; remaining--) {
	 *                      // skip until end of line
	 *                      detect(buf, '\n');
	 *                      processed = buf.position();
	 *                  }
	 *
	 *                  return SomeState.HEADER;
	 *              }
	 *          } catch (BufferUnderflowException bu) {
	 *              // reset buffer to last unprocessed position
	 *              buf.position(processed);
	 *          }
	 *
	 *          return context;
	 *      }
	 *  }
	 * </pre>
	 *
	 * @param buf     the buffer to be processed
	 * @param context the state the parser has at the beginning of this call
	 * @param output  the structure gathering parsed items
	 * @return the state the parser has at the end of this call respectivly will have at the beginning of the next
	 */
	CTX process(ByteBuffer buf, CTX context, OUT output);

	/**
	 * @return the initial parser state
	 */
	CTX initContext();

	/**
	 * @return an "empty" instance of the structure gathering parsed items
	 */
	OUT initOutput();

	default int getBufferSize() {
		return 64 * 1024;
	}

	/**
	 * @return the encoding for string conversion
	 */
	default Charset getEncoding() {
		return StandardCharsets.UTF_8;
	}

	/**
	 * Skips the buffer until char c was found.
	 */
	default void detect(ByteBuffer buf, char c) {
		while (buf.get() != c) ;
	}

	/**
	 * Encodes the bytes within the buffer from start to end (exclusive) as {@link String}.
	 *
	 * @param buf   the buffer to be encoded
	 * @param start the start position
	 * @param end   the end position
	 * @return the content within the buffer encoded as {@link String}
	 */
	default String parseString(ByteBuffer buf, int start, int end) {
		byte[] dst = new byte[end - start];
		for (int pos = start, i = 0; pos < end; pos++, i++) dst[i] = buf.get(pos);
		return new String(dst, getEncoding());
	}

	/**
	 * Encodes the bytes within the buffer from start to end (exclusive) as {@link String} without whitespaces at start or end.
	 *
	 * @param buf   the buffer to be encoded
	 * @param start the start position
	 * @param end   the end position
	 * @return the content within the buffer encoded as {@link String}
	 */
	default String trimmedString(ByteBuffer buf, int start, int end) {
		int s = start, e = end;

		while ((s < e) && (buf.get(s) <= ' ')) s++;
		while ((s < e) && (buf.get(e - 1) <= ' ')) e--;

		return (s != e) ? parseString(buf, s, e) : "";
	}

}
