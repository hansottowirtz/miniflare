import assert from "assert";
import { webcrypto } from "crypto";
import { PathLike, promises as fs } from "fs";
import {
  ReadableByteStreamController,
  ReadableStream,
  ReadableStreamBYOBRequest,
  WritableStream,
} from "stream/web";
import { InclusiveRange, Ranges, _parseRanges } from "./range";

const DEFAULT_CHUNK_SIZE = 1024;
const MULTIPART_BOUNDARY_PREFIX = "miniflare-boundary-";

const encoder = new TextEncoder();

export type ValidReadableStreamBYOBRequest = Omit<
  ReadableStreamBYOBRequest,
  "view"
> & { readonly view: Uint8Array };
export function unwrapBYOBRequest(
  controller: ReadableByteStreamController
): ValidReadableStreamBYOBRequest {
  // `controller.byobRequest` is typed as `undefined` in `@types/node`, but
  // should actually be `ReadableStreamBYOBRequest | undefined`. Unfortunately,
  // annotating `byobRequest` as `ReadableStreamBYOBRequest | undefined` doesn't
  // help here. Because of TypeScript's data flow analysis, it thinks
  // `controller.view` is `never`.
  const byobRequest = controller.byobRequest as
    | ReadableStreamBYOBRequest
    | undefined;
  assert(byobRequest !== undefined);

  // Specifying `autoAllocateChunkSize` means we'll always have a view,
  // even when using a default reader
  assert(byobRequest.view !== null);
  // Just asserted `view` is non-null, so this cast is safe
  return byobRequest as ValidReadableStreamBYOBRequest;
}

interface RangeSource {
  // Gets the total number of bytes in this source
  size(): Promise<number>;
  // Reads from the source at `position` for up-to `length` bytes into `view`,
  // returning the number of bytes read. `length <= view.byteLength` will hold.
  readInto(view: Uint8Array, position: number, length: number): Promise<number>;
  // Cleans up the source
  close?(): Promise<void>;
}
function createArrayRangeSource(array: Uint8Array): RangeSource {
  return {
    async size() {
      return array.byteLength;
    },
    async readInto(view, position, length) {
      assert(length <= view.byteLength);
      // `subarray()` will clamp `position + length` to `array.byteLength`
      const subarray = array.subarray(position, position + length);
      view.set(subarray);
      return subarray.length;
    },
  };
}
async function createFileRangeSource(filePath: PathLike): Promise<RangeSource> {
  // Open handle before creating stream, so not founds throws outside of stream
  const handle = await fs.open(filePath, "r");
  return {
    async size() {
      return (await handle.stat()).size;
    },
    async readInto(view, position, length) {
      assert(length <= view.byteLength);
      const { bytesRead } = await handle.read(view, 0, length, position);
      return bytesRead;
    },
    close() {
      return handle.close();
    },
  };
}

export type UnipartPartialReadableStream = ReadableStream<Uint8Array> & {
  size: number; // Total size in bytes of source
  range?: InclusiveRange;
};
function createUnipartReadableStream(
  source: RangeSource,
  size: number,
  range?: InclusiveRange
): UnipartPartialReadableStream {
  // Based off https://streams.spec.whatwg.org/#example-rbs-pull
  let position = range?.start ?? 0;
  const stream = new ReadableStream({
    type: "bytes",
    autoAllocateChunkSize: DEFAULT_CHUNK_SIZE,
    async pull(controller) {
      const byobRequest = unwrapBYOBRequest(controller);
      const v = byobRequest.view;

      let length = v.byteLength;
      if (range !== undefined) {
        // `+ 1` is because `range.end` is inclusive
        length = Math.min(length, range.end - position + 1);
      }

      const bytesRead = await source.readInto(v, position, length);
      if (bytesRead === 0) {
        await source.close?.();
        controller.close();
      } else {
        position += bytesRead;
      }
      byobRequest.respond(bytesRead);
    },
    cancel() {
      return source.close?.();
    },
  }) as UnipartPartialReadableStream;
  stream.size = size;
  if (range !== undefined) stream.range = range;
  return stream;
}

// Reading multiple ranges and generating a multipart response is a lot more
// complicated than single ranges. To keep the common case fast and simple,
// split this out into a separate function.

export interface MultipartOptions {
  contentType?: string;
}
export type MultipartPartialReadableStream = ReadableStream<Uint8Array> & {
  size: number; // Total size in bytes of source
  range: InclusiveRange[];
  multipartContentType: string;
};
export function createMultipartReadableStream(
  source: RangeSource,
  size: number,
  ranges: InclusiveRange[],
  opts?: MultipartOptions
): MultipartPartialReadableStream {
  // See https://developer.mozilla.org/en-US/docs/Web/HTTP/Range_requests#multipart_ranges
  // for details on `multipart/byteranges` responses
  const boundary = MULTIPART_BOUNDARY_PREFIX + webcrypto.randomUUID();
  const multipartContentType = `multipart/byteranges; boundary=${boundary}`;

  // At any point, we'll either be:
  //   (1) Writing boundary/headers data (followed by writing the range)
  //   (2) Writing trailer data (followed by closing the stream)
  //   (3) Writing a range of the file
  //
  // (1) and (2) are both writing dynamically generated bytes to the stream, so
  // treat them as one state. Define a type-level constraint for the states.
  //
  // Note `state` will be `undefined` on the first call to `nextState()`,
  // transitioning to either (1) or (2).
  let state:
    | { array: Uint8Array /* (1), (2) */; nextRange?: InclusiveRange /* (1) */ }
    | { range: InclusiveRange /* (3) */ }
    | undefined;
  let position = 0;

  // Shallow clone `ranges`, so we don't affect caller by `shift()`ing elements
  ranges = ranges.slice();

  function nextState(): boolean /* done */ {
    if (state === undefined || "range" in state) {
      let toWrite: string;
      const nextRange = ranges.shift();
      if (nextRange === undefined) {
        // Finished writing all ranges, now write the trailer [-->(2)]
        toWrite = `--${boundary}--`;
      } else {
        // Write boundary and headers, then the range [-->(1)]
        toWrite = `--${boundary}\r\n`;
        if (opts?.contentType !== undefined) {
          toWrite += `Content-Type: ${opts.contentType}\r\n`;
        }
        const start = nextRange.start;
        const end = Math.min(nextRange.end, size - 1);
        toWrite += `Content-Range: bytes ${start}-${end}/${size}\r\n\r\n`;
      }
      // If this isn't the first thing we've written, we'll need to prepend CRLF
      if (state !== undefined) toWrite = `\r\n${toWrite}`;

      position = 0;
      state = { array: encoder.encode(toWrite), nextRange };
    } else {
      if (state.nextRange === undefined) {
        // Finished writing trailer, so we're done [-->*]
        return true;
      } else {
        // Finished writing boundary and headers, now write the range [-->(3)]
        position = state.nextRange.start;
        state = { range: state.nextRange };
      }
    }
    return false;
  }

  // We should never be done after the initial transition. Even if we have
  // no ranges, we'll still write the trailer.
  assert(!nextState());

  const body = new ReadableStream({
    type: "bytes",
    autoAllocateChunkSize: DEFAULT_CHUNK_SIZE,
    async pull(controller) {
      // We call `nextState()` in `start()`, which either sets a defined `state`
      // or `returns true` indicating we're finished. If we were finished, we
      // would've closed the stream, and `pull()` shouldn't be called again.
      assert(state !== undefined);

      // `pull` is optional in `ReadableByteStreamControllerCallback`, but we've
      // definitely defined it, because this is the function we're in right now
      assert(this.pull !== undefined);

      const byobRequest = unwrapBYOBRequest(controller);
      const v = byobRequest.view;

      if ("range" in state) {
        // We're writing a range of the file
        const range = state.range;
        // `+ 1` is because `range.end` is inclusive
        const length = Math.min(v.byteLength, range.end - position + 1);
        const bytesRead = await source.readInto(v, position, length);
        if (bytesRead === 0) {
          // We've finished writing this range, move onto the next state. We
          // should never be done after this. Even if we have no more ranges,
          // we'll still write the trailer.
          assert(!nextState());
          // For BYOB, we're required to write bytes or close the stream
          return this.pull(controller);
        } else {
          position += bytesRead;
          byobRequest.respond(bytesRead);
        }
      } else {
        // We're copying an array
        const array = state.array;
        const length = Math.min(v.byteLength, array.byteLength - position);
        if (length === 0) {
          // We've finished writing this array, move onto the next state.
          if (nextState()) {
            // If we're done, cleanup
            await source.close?.();
            controller.close();
            byobRequest.respond(0);
          } else {
            // For BYOB, we're required to write bytes or close the stream
            return this.pull(controller);
          }
        } else {
          // `subarray()` returns a view over the same underlying buffer
          v.set(array.subarray(position, position + length));
          position += length;
          byobRequest.respond(length);
        }
      }
    },
    cancel() {
      return source.close?.();
    },
  }) as MultipartPartialReadableStream;

  body.size = size;
  body.range = ranges;
  body.multipartContentType = multipartContentType;
  return body;
}

export type PartialReadableStream =
  | UnipartPartialReadableStream
  | MultipartPartialReadableStream;
async function createReadableStream(
  source: RangeSource,
  range?: Ranges,
  opts?: MultipartOptions
): Promise<PartialReadableStream> {
  const size = await source.size();
  const ranges = _parseRanges(size, range);
  if (ranges.length <= 1) {
    // If ranges is empty, return the whole source.
    // If ranges contains a single range, just return that.
    return createUnipartReadableStream(source, size, ranges[0]);
  } else {
    // Otherwise, return a multipart response.
    return createMultipartReadableStream(source, size, ranges, opts);
  }
}

export function createArrayReadableStream(
  array: Uint8Array,
  range?: Ranges,
  opts?: MultipartOptions
): Promise<PartialReadableStream> {
  // TODO(perf): small array optimisation: if `array.byteLength < DEFAULT_CHUNK_SIZE`,
  //  as will likely be the case in local testing, enqueue range of array directly,
  //  without allocating `DEFAULT_CHUNK_SIZE` buffer to copy into
  // We could use `new Blob([array]).stream()` for this, but the returned value
  // isn't a byte-stream, so doesn't allow BYOB reads
  const source = createArrayRangeSource(array);
  return createReadableStream(source, range, opts);
}

export async function createFileReadableStream(
  filePath: PathLike,
  range?: Ranges,
  opts?: MultipartOptions
): Promise<PartialReadableStream> {
  const source = await createFileRangeSource(filePath);
  return createReadableStream(source, range, opts);
}

export async function createFileWritableStream(
  filePath: PathLike,
  exclusive = false
): Promise<WritableStream<Uint8Array>> {
  // Based off https://streams.spec.whatwg.org/#example-ws-backpressure
  // Open handle before returning, so file errors throws outside of stream.
  // If exclusive flag is set, throw if `filePath` already exists.
  const handle = await fs.open(filePath, exclusive ? "wx" : "w");
  return new WritableStream({
    write(chunk) {
      const promise = handle.write(chunk, 0, chunk.length);
      // `write()`s return is typed as `Promise<void>` in `@types/node`, but
      // should probably be `Promise<unknown>`, as any `Promise` is valid.
      return promise as Promise<unknown> as Promise<void>;
    },
    close() {
      return handle.close();
    },
    abort() {
      return handle.close();
    },
  });
}
