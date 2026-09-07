// k6 equivalent of bench/bench.luau: one GET per iteration, keep-alive on
// (k6's default), pointed at the same `moonson serve-bench` target.
//
//   k6 run bench/bench.js
//
// Keep --vus and --duration identical to the moonson run you compare against.
import http from 'k6/http';

export const options = {
  vus: 50,
  duration: '20s',
};

export default function () {
  http.get('http://127.0.0.1:8080/');
}
