import { _parseRangeHeader } from "@miniflare/tre";
import test from "ava";

test('_parseRanges: case-insensitive unit must be "bytes"', (t) => {
  // Check case-insensitive and ignores whitespace
  t.not(_parseRangeHeader(2, "bytes=0-1"), undefined);
  t.not(_parseRangeHeader(2, "BYTES    =0-1"), undefined);
  t.not(_parseRangeHeader(4, "     bYtEs=0-1"), undefined);
  t.not(_parseRangeHeader(2, "    Bytes        =0-1"), undefined);
  // Check fails with other units
  t.is(_parseRangeHeader(2, "nibbles=0-1"), undefined);
});

test("_parseRanges: matches range with start and end", (t) => {
  // Check valid ranges accepted
  t.deepEqual(_parseRangeHeader(8, "bytes=0-1"), [{ start: 0, end: 1 }]);
  t.deepEqual(_parseRangeHeader(8, "bytes=2-7"), [{ start: 2, end: 7 }]);
  t.deepEqual(_parseRangeHeader(8, "bytes=5-5"), [{ start: 5, end: 5 }]);
  // Check start after end rejected
  t.deepEqual(_parseRangeHeader(2, "bytes=1-0"), undefined);
  // Check start after content rejected
  t.deepEqual(_parseRangeHeader(2, "bytes=2-3"), undefined);
  t.deepEqual(_parseRangeHeader(2, "bytes=5-7"), undefined);
  // Check end after content truncated
  t.deepEqual(_parseRangeHeader(2, "bytes=0-2"), [{ start: 0, end: 1 }]);
  t.deepEqual(_parseRangeHeader(3, "bytes=1-5"), [{ start: 1, end: 2 }]);
  // Check multiple valid ranges accepted
  t.deepEqual(_parseRangeHeader(12, "bytes=  1-3  , 6-7,10-11"), [
    { start: 1, end: 3 },
    { start: 6, end: 7 },
    { start: 10, end: 11 },
  ]);
  // Check overlapping ranges accepted
  t.deepEqual(_parseRangeHeader(5, "bytes=0-2,1-3"), [
    { start: 0, end: 2 },
    { start: 1, end: 3 },
  ]);
});

test("_parseRanges: matches range with just start", (t) => {
  // Check valid ranges accepted
  t.deepEqual(_parseRangeHeader(8, "bytes=2-"), [{ start: 2, end: 7 }]);
  t.deepEqual(_parseRangeHeader(6, "bytes=5-"), [{ start: 5, end: 5 }]);
  // Check start after content rejected
  t.deepEqual(_parseRangeHeader(2, "bytes=2-"), undefined);
  t.deepEqual(_parseRangeHeader(2, "bytes=5-"), undefined);
  // Check multiple valid ranges accepted
  t.deepEqual(_parseRangeHeader(12, "bytes=  1-  ,6- ,  10-11   "), [
    { start: 1, end: 11 },
    { start: 6, end: 11 },
    { start: 10, end: 11 },
  ]);
});

test("_parseRanges: matches range with just end", (t) => {
  // Check valid ranges accepted
  t.deepEqual(_parseRangeHeader(8, "bytes=-2"), [{ start: 6, end: 7 }]);
  t.deepEqual(_parseRangeHeader(7, "bytes=-6"), [{ start: 1, end: 6 }]);
  // Check start before content truncated and entire response returned
  t.deepEqual(_parseRangeHeader(7, "bytes=-7"), []);
  t.deepEqual(_parseRangeHeader(5, "bytes=-10"), []);
  // Check if any range returns entire response, other ranges ignored
  t.deepEqual(_parseRangeHeader(5, "bytes=0-1,-5,2-3"), []);
  // Check empty range ignored
  t.deepEqual(_parseRangeHeader(2, "bytes=-0"), []);
  t.deepEqual(_parseRangeHeader(4, "bytes=0-1,-0,2-3"), [
    { start: 0, end: 1 },
    { start: 2, end: 3 },
  ]);
});

test("_parseRanges: range requires at least start or end", (t) => {
  // Check range with no start or end rejected
  t.is(_parseRangeHeader(2, "bytes=-"), undefined);
  // Check range with no dash rejected
  t.is(_parseRangeHeader(2, "bytes=0"), undefined);
  // Check empty range rejected
  t.is(_parseRangeHeader(2, "bytes=0-1,"), undefined);
  // Check no ranges accepted
  t.deepEqual(_parseRangeHeader(2, "bytes="), []);
});
