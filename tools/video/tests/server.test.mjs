import assert from 'node:assert/strict';
import { after, before, test } from 'node:test';
import { readFile } from 'node:fs/promises';
import { request as httpRequest } from 'node:http';
import { createFilmServer } from '../server.mjs';

const portrait = await readFile(new URL('../../../agent-docs/video/assets/ishan/neutral.png', import.meta.url));
const script = await readFile(new URL('../../../agent-docs/video/script.md', import.meta.url));
let server;
let port;

before(async () => {
  server = createFilmServer();
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  port = server.address().port;
});
after(async () => { await new Promise(resolve => server.close(resolve)); });

function request(path, { method = 'GET', headers = {} } = {}) {
  return new Promise((resolve, reject) => {
    const req = httpRequest({ hostname: '127.0.0.1', port, path, method, headers }, response => {
      const chunks = [];
      response.on('data', chunk => chunks.push(chunk));
      response.on('end', () => resolve({ status: response.statusCode, headers: response.headers, body: Buffer.concat(chunks) }));
    });
    req.on('error', reject); req.end();
  });
}

test('serves the canonical script, portrait assets, and stage modules from their mounts', async () => {
  const [markdown, image, module] = await Promise.all([request('/script.md'), request('/assets/ishan/neutral.png'), request('/scene.mjs')]);
  assert.equal(markdown.status, 200); assert.deepEqual(markdown.body, script);
  assert.match(markdown.headers['content-type'], /^text\/markdown/);
  assert.equal(image.status, 200); assert.deepEqual(image.body, portrait);
  assert.equal(image.headers['content-type'], 'image/png');
  assert.equal(module.status, 200); assert.match(module.headers['content-type'], /javascript/);
  assert.match(module.body.toString(), /export function createRenderer/);
});

test('health is JSON and the composer endpoint redirects to its local server', async () => {
  const health = await request('/health');
  assert.equal(health.status, 200); assert.equal(JSON.parse(health.body).ok, true);
  assert.match(health.headers['content-type'], /^application\/json/);
  const composer = await request('/composer');
  assert.equal(composer.status, 302); assert.equal(composer.headers.location, 'http://127.0.0.1:8089/');
});

test('does not expose repository parents, encoded traversal, or malformed paths', async () => {
  for (const path of ['/AGENTS.md', '/server.mjs', '/assets/../script.md', '/assets/%2e%2e/%2e%2e/AGENTS.md', '/assets/%2E%2E%2Fscript.md', '/%2e%2e/AGENTS.md', '/media/../../AGENTS.md', '/assets/%5c..%5cAGENTS.md', '/assets/%00.png', '/assets/%ZZ']) {
    const response = await request(path);
    assert.ok([400, 403, 404].includes(response.status), `${path}: ${response.status}`);
    assert.ok(!response.body.includes(Buffer.from('# dynamo-gdl — Agent Guide')));
  }
});

test('rejects mutations without accepting uploads', async () => {
  for (const method of ['POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS']) {
    const response = await request('/assets/ishan/neutral.png', { method });
    assert.equal(response.status, 405); assert.equal(response.headers.allow, 'GET, HEAD');
  }
});

test('HEAD returns file metadata without a body', async () => {
  const response = await request('/assets/ishan/neutral.png', { method: 'HEAD' });
  assert.equal(response.status, 200); assert.equal(response.body.length, 0);
  assert.equal(Number(response.headers['content-length']), portrait.length);
  assert.equal(response.headers['accept-ranges'], 'bytes');
});

test('serves closed, open-ended, and suffix byte ranges', async () => {
  const cases = [['bytes=4-11', 4, 11], [`bytes=${portrait.length - 7}-`, portrait.length - 7, portrait.length - 1], ['bytes=-9', portrait.length - 9, portrait.length - 1], [`bytes=${portrait.length - 3}-${portrait.length + 100}`, portrait.length - 3, portrait.length - 1]];
  for (const [range, start, end] of cases) {
    const response = await request('/assets/ishan/neutral.png', { headers: { range } });
    assert.equal(response.status, 206);
    assert.equal(response.headers['content-range'], `bytes ${start}-${end}/${portrait.length}`);
    assert.equal(Number(response.headers['content-length']), end - start + 1);
    assert.deepEqual(response.body, portrait.subarray(start, end + 1));
  }
  const head = await request('/assets/ishan/neutral.png', { method: 'HEAD', headers: { range: 'bytes=0-7' } });
  assert.equal(head.status, 206); assert.equal(head.body.length, 0); assert.equal(head.headers['content-length'], '8');
});

test('rejects unsatisfiable, malformed, and multipart ranges with the file size', async () => {
  for (const range of [`bytes=${portrait.length}-`, 'bytes=10-2', 'bytes=-0', 'bytes=-', 'bytes=x-y', 'bytes=0-2,7-9']) {
    const response = await request('/assets/ishan/neutral.png', { headers: { range } });
    assert.equal(response.status, 416, range);
    assert.equal(response.headers['content-range'], `bytes */${portrait.length}`);
  }
});
