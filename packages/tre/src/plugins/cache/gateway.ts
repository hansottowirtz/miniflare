import assert from "assert";
import crypto from "crypto";
import http from "http";
import { ReadableStream } from "stream/web";
import CachePolicy from "http-cache-semantics";
import { Headers, HeadersInit, Request, Response, fetch } from "../../http";
import { Clock, Log } from "../../shared";
import { Storage } from "../../storage";
import {
  KeyValueEntry,
  KeyValueStorage,
  PartialReadableStream,
  RangeNotSatisfiableError,
} from "../../storage2";
import { isSitesRequest } from "../kv";
import { CacheMiss, PurgeFailure, StorageFailure } from "./errors";

interface CacheMetadata {
  headers: string[][];
  status: number;
}

function getExpiration(clock: Clock, req: Request, res: Response) {
  // Cloudflare ignores request Cache-Control
  const reqHeaders = normaliseHeaders(req.headers);
  delete reqHeaders["cache-control"];

  // Cloudflare never caches responses with Set-Cookie headers
  // If Cache-Control contains private=set-cookie, Cloudflare will remove
  // the Set-Cookie header automatically
  const resHeaders = normaliseHeaders(res.headers);
  if (
    resHeaders["cache-control"]?.toLowerCase().includes("private=set-cookie")
  ) {
    resHeaders["cache-control"] = resHeaders["cache-control"]
      ?.toLowerCase()
      .replace(/private=set-cookie;?/i, "");
    delete resHeaders["set-cookie"];
  }

  // Build request and responses suitable for CachePolicy
  const cacheReq: CachePolicy.Request = {
    url: req.url,
    // If a request gets to the Cache service, it's method will be GET. See README.md for details
    method: "GET",
    headers: reqHeaders,
  };
  const cacheRes: CachePolicy.Response = {
    status: res.status,
    headers: resHeaders,
  };

  // @ts-expect-error `now` isn't included in CachePolicy's type definitions
  const originalNow = CachePolicy.prototype.now;
  // @ts-expect-error `now` isn't included in CachePolicy's type definitions
  CachePolicy.prototype.now = clock;
  try {
    const policy = new CachePolicy(cacheReq, cacheRes, { shared: true });

    return {
      // Check if the request & response is cacheable
      storable: policy.storable() && !("set-cookie" in resHeaders),
      expiration: policy.timeToLive(),
      // Cache Policy Headers is typed as [header: string]: string | string[] | undefined
      // It's safe to ignore the undefined here, which is what casting to HeadersInit does
      headers: policy.responseHeaders() as HeadersInit,
    };
  } finally {
    // @ts-expect-error `now` isn't included in CachePolicy's type definitions
    CachePolicy.prototype.now = originalNow;
  }
}

// Normalises headers to object mapping lower-case names to single values.
// Single values are OK here as the headers we care about for determining
// cache-ability are all single-valued, and we store the raw, multi-valued
// headers in KV once this has been determined.
function normaliseHeaders(headers: Headers): Record<string, string> {
  const result: Record<string, string> = {};
  for (const [key, value] of headers) result[key.toLowerCase()] = value;
  return result;
}

// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/ETag#syntax
const etagRegexp = /^(W\/)?"(.+)"$/;
function parseETag(value: string): string | undefined {
  // As we only use this for `If-None-Match` handling, which always uses the
  // weak comparison algorithm, ignore "W/" directives:
  // https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/If-None-Match
  return etagRegexp.exec(value.trim())?.[2] ?? undefined;
}

// https://datatracker.ietf.org/doc/html/rfc7231#section-7.1.1.1
const utcDateRegexp =
  /^(Mon|Tue|Wed|Thu|Fri|Sat|Sun), \d\d (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d\d\d\d \d\d:\d\d:\d\d GMT$/;
function parseUTCDate(value: string): number {
  return utcDateRegexp.test(value) ? Date.parse(value) : NaN;
}

function getMatchResponse(
  reqHeaders: Headers,
  resStatus: number,
  resHeaders: Headers,
  resBody: PartialReadableStream
): Response {
  // If `If-None-Match` is set, perform a conditional request:
  // https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/If-None-Match
  const reqIfNoneMatchHeader = reqHeaders.get("If-None-Match");
  const resETagHeader = resHeaders.get("ETag");
  if (reqIfNoneMatchHeader !== null && resETagHeader !== null) {
    const resETag = parseETag(resETagHeader);
    if (resETag !== undefined) {
      if (reqIfNoneMatchHeader.trim() === "*") {
        return new Response(null, { status: 304, headers: resHeaders });
      }
      for (const reqIfNoneMatch of reqIfNoneMatchHeader.split(",")) {
        if (resETag === parseETag(reqIfNoneMatch)) {
          return new Response(null, { status: 304, headers: resHeaders });
        }
      }
    }
  }

  // If `If-Modified-Since` is set, perform a conditional request:
  // https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/If-Modified-Since
  const reqIfModifiedSinceHeader = reqHeaders.get("If-Modified-Since");
  const resLastModifiedHeader = resHeaders.get("Last-Modified");
  if (reqIfModifiedSinceHeader !== null && resLastModifiedHeader !== null) {
    const reqIfModifiedSince = parseUTCDate(reqIfModifiedSinceHeader);
    const resLastModified = parseUTCDate(resLastModifiedHeader);
    // Comparison of NaN's (invalid dates), will always result in `false`
    if (resLastModified <= reqIfModifiedSince) {
      return new Response(null, { status: 304, headers: resHeaders });
    }
  }

  // If `Range` was set, return a partial response:
  // https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Range
  if (resBody.range !== undefined) {
    resStatus = 206; // Partial Content
    if (Array.isArray(resBody.range)) {
      // TypeScript can't seem to narrow to `MultipartPartialReadableStream`
      // from `isArray` check
      assert("multipartContentType" in resBody);
      resHeaders.set("Content-Type", resBody.multipartContentType);
    } else {
      const { start, end } = resBody.range;
      resHeaders.set("Content-Range", `bytes ${start}-${end}/${resBody.size}`);
      resHeaders.set("Content-Length", `${end - start + 1}`);
    }
  }

  return new Response(resBody, { status: resStatus, headers: resHeaders });
}

class HttpParser {
  private static INSTANCE: HttpParser;
  static get(): HttpParser {
    HttpParser.INSTANCE ??= new HttpParser();
    return HttpParser.INSTANCE;
  }

  readonly #responses: Map<string, ReadableStream<Uint8Array>> = new Map();
  readonly #ready: Promise<URL>;

  private constructor() {
    const server = http.createServer(this.#listen).unref();
    this.#ready = new Promise((resolve) => {
      server.listen(0, "localhost", () => {
        const address = server.address();
        assert(address !== null && typeof address === "object");
        resolve(new URL(`http://localhost:${address.port}`));
      });
    });
  }

  #listen: http.RequestListener = async (req, res) => {
    assert(req.url !== undefined);
    assert(res.socket !== null);
    const stream = this.#responses.get(req.url);
    assert(stream !== undefined);
    // Write response to parse directly to underlying socket
    for await (const chunk of stream) res.socket.write(chunk);
    res.socket.end();
  };

  async parse(response: ReadableStream<Uint8Array>): Promise<Response> {
    const baseURL = await this.#ready;
    // Since multiple parses can be in-flight at once, an identifier is needed
    const id = `/${crypto.randomBytes(16).toString("hex")}`;
    this.#responses.set(id, response);
    try {
      return await fetch(new URL(id, baseURL));
    } finally {
      this.#responses.delete(id);
    }
  }
}

export class CacheGateway {
  private readonly storage: KeyValueStorage<CacheMetadata>;

  constructor(
    private readonly log: Log,
    legacyStorage: Storage,
    private readonly clock: Clock
  ) {
    const storage = legacyStorage.getNewStorage();
    this.storage = new KeyValueStorage(storage, clock);
  }

  async match(request: Request, cacheKey?: string): Promise<Response> {
    // Never cache Workers Sites requests, so we always return on-disk files
    if (isSitesRequest(request)) throw new CacheMiss();
    cacheKey ??= request.url;

    const range = request.headers.get("Range") ?? undefined;
    let resHeaders: Headers | undefined;
    let cached: KeyValueEntry<CacheMetadata> | null;
    try {
      cached = await this.storage.get(cacheKey, range, (metadata) => {
        resHeaders = new Headers(metadata.headers);
        const contentType = resHeaders.get("Content-Type");
        return { contentType: contentType ?? undefined };
      });
    } catch (e) {
      if (e instanceof RangeNotSatisfiableError) {
        return new Response(null, {
          status: 416,
          headers: {
            "Content-Range": `bytes */${e.totalSize}`,
            "CF-Cache-Status": "HIT",
          },
        });
      }
      throw e;
    }
    if (cached?.metadata === undefined) throw new CacheMiss();

    // Avoid re-constructing headers if we already extracted multipart options
    resHeaders ??= new Headers(cached.metadata.headers);
    resHeaders.set("CF-Cache-Status", "HIT");

    return getMatchResponse(
      request.headers,
      cached.metadata.status,
      resHeaders,
      cached.value
    );
  }

  async put(
    request: Request,
    value: ReadableStream<Uint8Array>,
    cacheKey?: string
  ): Promise<Response> {
    // Never cache Workers Sites requests, so we always return on-disk files.
    if (isSitesRequest(request)) return new Response(null, { status: 204 });

    const response = await HttpParser.get().parse(value);
    assert(response.body !== null);

    const { storable, expiration, headers } = getExpiration(
      this.clock,
      request,
      response
    );
    if (!storable) throw new StorageFailure();

    cacheKey ??= request.url;
    await this.storage.put({
      key: cacheKey,
      value: response.body,
      expiration: this.clock() + expiration,
      metadata: {
        headers: Object.entries(headers),
        status: response.status,
      },
    });
    return new Response(null, { status: 204 });
  }

  async delete(request: Request, cacheKey?: string): Promise<Response> {
    cacheKey ??= request.url;
    const deleted = await this.storage.delete(cacheKey);
    // This is an extremely vague error, but it fits with what the cache API in workerd expects
    if (!deleted) throw new PurgeFailure();
    return new Response(null);
  }
}
