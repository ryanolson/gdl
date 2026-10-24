#!/usr/bin/env node
import { createServer } from 'node:http';
import { createReadStream } from 'node:fs';
import { realpath, stat } from 'node:fs/promises';
import { dirname, extname, resolve, sep } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { pipeline } from 'node:stream/promises';

const videoRoot = dirname(fileURLToPath(import.meta.url));
const repository = resolve(videoRoot, '../..');
const mounts = [
  ['/assets/', resolve(repository, 'agent-docs/video/assets')],
  ['/media/', resolve(videoRoot, 'work/media')],
];
const types = {
  '.html': 'text/html; charset=utf-8', '.mjs': 'text/javascript; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8', '.md': 'text/markdown; charset=utf-8',
  '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.svg': 'image/svg+xml',
  '.mp4': 'video/mp4', '.webm': 'video/webm', '.wav': 'audio/wav', '.mp3': 'audio/mpeg',
  '.woff2': 'font/woff2', '.ico': 'image/x-icon',
};

export function createFilmServer() {
  return createServer((request, response) => {
    serve(request, response).catch(() => {
      if (!response.headersSent) send(response, request.method, 500, 'Unable to serve file.\n');
      else response.destroy();
    });
  });
}

async function serve(request, response) {
  response.setHeader('Cache-Control', 'no-store');
  response.setHeader('X-Content-Type-Options', 'nosniff');
  if (!['GET', 'HEAD'].includes(request.method)) {
    response.setHeader('Allow', 'GET, HEAD');
    request.resume();
    return send(response, request.method, 405, 'Only GET and HEAD are supported.\n');
  }
  let pathname;
  try { pathname = decodeURIComponent((request.url || '/').split('?')[0]); }
  catch { return send(response, request.method, 400, 'Malformed request path.\n'); }
  if (!pathname.startsWith('/') || /[\\\0]/.test(pathname)) return send(response, request.method, 400, 'Invalid request path.\n');
  if (pathname.split('/').includes('..')) return send(response, request.method, 403, 'Parent paths are not served.\n');
  if (pathname === '/health') return send(response, request.method, 200, JSON.stringify({ ok: true }), 'application/json; charset=utf-8');
  if (pathname === '/composer') {
    response.setHeader('Location', 'http://127.0.0.1:8089/');
    return send(response, request.method, 302, 'Open the local GDL composer.\n');
  }
  let root = resolve(videoRoot, 'dist');
  let relative = pathname === '/' ? 'index.html' : pathname.slice(1);
  if (pathname === '/script.md') {
    root = resolve(repository, 'agent-docs/video'); relative = 'script.md';
  } else {
    const mount = mounts.find(([prefix]) => pathname.startsWith(prefix));
    if (mount) { root = mount[1]; relative = pathname.slice(mount[0].length); }
  }
  const candidate = resolve(root, relative);
  if (!within(candidate, root)) return send(response, request.method, 403, 'Path is outside its mount.\n');
  let file;
  let info;
  try {
    const [actualRoot, actualFile] = await Promise.all([realpath(root), realpath(candidate)]);
    if (!within(actualFile, actualRoot)) return send(response, request.method, 403, 'Path is outside its mount.\n');
    file = actualFile; info = await stat(file);
  } catch (error) {
    if (['ENOENT', 'ENOTDIR', 'EACCES', 'ELOOP'].includes(error.code)) return send(response, request.method, 404, 'Not found.\n');
    throw error;
  }
  if (!info.isFile()) return send(response, request.method, 404, 'Not found.\n');
  response.setHeader('Content-Type', types[extname(file).toLowerCase()] || 'application/octet-stream');
  response.setHeader('Accept-Ranges', 'bytes');
  let start = 0;
  let end = info.size - 1;
  if (request.headers.range !== undefined) {
    const range = byteRange(request.headers.range, info.size);
    if (!range) {
      response.setHeader('Content-Range', `bytes */${info.size}`);
      return send(response, request.method, 416, 'Requested byte range is unavailable.\n');
    }
    ({ start, end } = range);
    response.statusCode = 206;
    response.setHeader('Content-Range', `bytes ${start}-${end}/${info.size}`);
  }
  response.setHeader('Content-Length', Math.max(0, end - start + 1));
  if (request.method === 'HEAD' || info.size === 0) return response.end();
  await pipeline(createReadStream(file, { start, end }), response);
}

function byteRange(header, size) {
  const match = /^bytes=(\d*)-(\d*)$/.exec(header);
  if (!match || (!match[1] && !match[2]) || size === 0) return null;
  let start = match[1] ? Number(match[1]) : undefined;
  let end = match[2] ? Number(match[2]) : size - 1;
  if (start === undefined) {
    if (!Number.isSafeInteger(end) || end <= 0) return null;
    start = Math.max(0, size - end); end = size - 1;
  }
  if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end) || start >= size || start > end) return null;
  return { start, end: Math.min(end, size - 1) };
}

function within(path, root) { return path === root || path.startsWith(root + sep); }
function send(response, method, status, body, type = 'text/plain; charset=utf-8') {
  response.statusCode = status;
  response.setHeader('Content-Type', type);
  response.setHeader('Content-Length', Buffer.byteLength(body));
  response.end(method === 'HEAD' ? undefined : body);
}

if (process.argv[1] && pathToFileURL(resolve(process.argv[1])).href === import.meta.url) {
  const args = process.argv.slice(2);
  if (args.includes('--help') || args.includes('-h')) {
    console.log('Usage: node tools/video/server.mjs [--port 8088]\n\nServe the film stage on 127.0.0.1 only. GET and HEAD; no uploads.\nLocal composer redirect: http://127.0.0.1:8089/');
  } else {
    let port = 8088;
    if (args.length === 2 && args[0] === '--port') port = Number(args[1]);
    else if (args.length === 1 && args[0].startsWith('--port=')) port = Number(args[0].slice(7));
    else if (args.length) { console.error('Unknown arguments. Use --help.'); process.exitCode = 1; port = NaN; }
    if (!Number.isInteger(port) || port < 0 || port > 65535) {
      if (!process.exitCode) console.error('Port must be an integer from 0 to 65535.');
      process.exitCode = 1;
    } else {
      const server = createFilmServer();
      server.on('error', error => { console.error(`Film server: ${error.message}`); process.exitCode = 1; });
      server.listen(port, '127.0.0.1', () => console.log(`Film stage: http://127.0.0.1:${server.address().port}/`));
    }
  }
}
