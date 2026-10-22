#!/usr/bin/env node
import { mkdir, rename, rm } from 'node:fs/promises';
import { basename, dirname, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { randomUUID } from 'node:crypto';

const videoRoot = dirname(fileURLToPath(import.meta.url));

export function parseArgs(args) {
  const options = { url: 'http://127.0.0.1:8089/', out: resolve(videoRoot, 'work/composer.mp4'), dryRun: false, fixture: 'api' };
  const seen = new Set();
  for (let i = 0; i < args.length; i++) {
    const token = args[i] === '-h' ? '--help' : args[i];
    const equal = token.indexOf('=');
    const key = equal < 0 ? token : token.slice(0, equal);
    if (!['--url', '--out', '--fixture', '--help', '--dry-run'].includes(key)) throw new Error(`Unknown option: ${token}. Use --help.`);
    if (seen.has(key)) throw new Error(`Duplicate option: ${key}.`);
    seen.add(key);
    if (['--help', '--dry-run'].includes(key)) {
      if (equal >= 0) throw new Error(`${key} takes no value.`);
      options[key === '--help' ? 'help' : 'dryRun'] = true; continue;
    }
    const value = equal < 0 ? args[++i] : token.slice(equal + 1);
    if (!value || value.startsWith('--')) throw new Error(`${key} needs a value.`);
    options[key.slice(2)] = value;
  }
  if (options.help) return options;
  const url = new URL(options.url);
  if (!['http:', 'https:'].includes(url.protocol) || !['localhost', '127.0.0.1', '[::1]'].includes(url.hostname)) throw new Error('--url must use HTTP(S) on localhost or a loopback address.');
  if (url.username || url.password) throw new Error('--url must not contain credentials.');
  if (!options.out.toLowerCase().endsWith('.mp4')) throw new Error('--out must name an MP4 file.');
  if (!['api', 'film'].includes(options.fixture)) throw new Error('--fixture must be api or film.');
  options.url = url.href;
  return options;
}

export function conversionArgs(source, destination) {
  return ['-hide_banner', '-loglevel', 'error', '-y', '-i', source, '-vf', 'scale=1920:1080:flags=lanczos', '-an', '-c:v', 'libx264', '-preset', 'medium', '-crf', '18', '-pix_fmt', 'yuv420p', '-movflags', '+faststart', '-f', 'mp4', destination];
}

async function run(options) {
  let chromium;
  let ffmpeg;
  try {
    const modules = await Promise.all([import('playwright-core'), import('ffmpeg-static')]);
    chromium = modules[0].chromium; ffmpeg = modules[1].default;
  } catch { throw new Error('Local dependencies are missing. Run npm --prefix tools/video ci.'); }
  const { stdout } = await promisify(execFile)(ffmpeg, ['-version'], { timeout: 10000, maxBuffer: 131072 });
  const output = resolve(options.out);
  const recordings = resolve(videoRoot, 'work/recordings');
  let browser;
  let context;
  let partial;
  try {
    browser = await chromium.launch({ channel: 'chrome', headless: true, timeout: 20000, slowMo: options.dryRun ? 0 : 120 });
    if (!options.dryRun) await mkdir(recordings, { recursive: true });
    context = await browser.newContext({
      viewport: { width: 1920, height: 1080 }, deviceScaleFactor: 1,
      ...(options.dryRun ? {} : { recordVideo: { dir: recordings, size: { width: 1920, height: 1080 } } }),
    });
    const page = await context.newPage(); page.setDefaultTimeout(10000);
    const response = await page.goto(options.url, { waitUntil: 'domcontentloaded', timeout: 20000 });
    if (!response?.ok()) throw new Error(`Composer returned HTTP ${response?.status() ?? 'no response'}. Start the composer on ${options.url}.`);
    const frontend = page.locator('#source-frontend option').filter({ hasText: options.fixture === 'film' ? /^HttpFrontend \(/ : /^ApiFrontend \(/ });
    await frontend.waitFor({ state: 'attached' });
    if (await page.locator('#node-count').textContent() !== '1 node') throw new Error('An isolated composer context must start with one source node.');
    console.log(`Prerequisites OK: Chrome ${browser.version()}; ${stdout.split('\n')[0]}; ${options.fixture} catalog ready.`);
    if (options.dryRun) { console.log('Dry run: no graph actions and no recording.'); return; }
    const video = page.video();
    if (!video) throw new Error('Playwright did not create a context recording.');
    if (options.fixture === 'film') await filmTour(page, frontend); else await tour(page, frontend);
    await context.close(); context = null;
    const webm = await video.path();
    await mkdir(dirname(output), { recursive: true });
    partial = resolve(dirname(output), `.${basename(output)}.${randomUUID()}.partial.mp4`);
    await promisify(execFile)(ffmpeg, conversionArgs(webm, partial), { timeout: 60000, maxBuffer: 1048576 });
    await rename(partial, output); partial = null;
    console.log(`Saved ${options.fixture} composer rehearsal: ${output}`);
    console.log(`Original browser video: ${webm}`);
    console.log(options.fixture === 'film' ? 'This tour shows real reference and ACME demo nodes in the composer. Native runtime and accounting receipts require the separate process demonstration.' : 'This tour shows the real API example and host state. It is not ACME, NIM, or Stargate proof footage.');
  } finally {
    await context?.close().catch(() => {});
    await browser?.close().catch(() => {});
    if (partial) await rm(partial, { force: true });
  }
}

async function tour(page, frontend) {
  // These holds give viewers time to read after an observed UI state is ready.
  const hold = async (label, milliseconds = 1500) => { console.log(label); await page.waitForTimeout(milliseconds); };
  const choose = async (slot, name) => {
    await page.getByRole('button', { name: slot, exact: true }).click();
    const dialog = page.locator('#node-dialog'); await dialog.waitFor({ state: 'visible' });
    await page.locator('#node-options .node-choice').filter({ hasText: new RegExp(`^${name}`) }).click();
    await dialog.waitFor({ state: 'hidden' });
  };
  await hold('Catalog: real API example nodes.');
  await page.getByRole('button', { name: 'Source settings', exact: true }).click();
  await page.locator('#source-dialog').waitFor({ state: 'visible' });
  await page.locator('#source-frontend').selectOption(await frontend.getAttribute('value'));
  await page.getByRole('checkbox', { name: 'Responses', exact: true }).uncheck();
  await page.getByRole('checkbox', { name: 'Messages', exact: true }).uncheck();
  await hold('Frontend: Chat Completions enabled; Responses and Messages disabled.', 2000);
  await page.locator('#source-dialog').getByRole('button', { name: 'Done', exact: true }).click();
  await choose('Add request node', 'ExtractChat');
  await page.locator('#graph-board').getByRole('button', { name: 'Remove ExtractChat', exact: true }).waitFor();
  await hold('Request adapter: ExtractChat.');
  await choose('Add response node', 'FormatChat');
  await page.locator('#graph-board').getByRole('button', { name: 'Remove FormatChat', exact: true }).waitFor();
  await hold('Response adapter: FormatChat.');
  await page.getByRole('button', { name: 'Add node or connect existing', exact: true }).click();
  await page.locator('#pipeline-connect-dialog').waitFor({ state: 'visible' });
  await page.locator('#pipeline-choices .pipeline-choice').filter({ hasText: /^Echo/ }).click();
  await page.locator('#flow-status').filter({ hasText: /^Flow complete/ }).waitFor();
  await page.locator('#node-count').filter({ hasText: /^4 nodes$/ }).waitFor();
  await page.getByRole('button', { name: 'Fit', exact: true }).click();
  await hold('Observed: four-node graph is valid and flow is complete.', 3000);
  await page.getByRole('tab', { name: 'Segments', exact: true }).click();
  await page.locator('.deployment-view').waitFor({ state: 'visible' });
  await hold('Placement: the same graph in one segment.', 2000);
  await page.getByRole('button', { name: 'Move Echo to segment', exact: true }).click();
  await page.locator('#boundary-dialog').waitFor({ state: 'visible' });
  await page.locator('#boundary-scope').selectOption('node');
  await page.locator('#boundary-placement').selectOption('');
  await page.locator('#boundary-name').fill('engine');
  await hold('Move only Echo into the engine segment.');
  await page.getByRole('button', { name: 'Apply placement', exact: true }).click();
  await page.locator('[aria-label="Segment engine"] [data-deployment-member="Echo"]').waitFor({ state: 'visible' });
  await page.locator('#flow-status').filter({ hasText: /^Placement valid/ }).waitFor();
  await page.getByRole('button', { name: 'Fit', exact: true }).click();
  await hold('Observed: Echo runs in its own segment; placement is valid.', 3000);
  const targets = page.waitForResponse(response => new URL(response.url()).pathname === '/api/targets', { timeout: 15000 });
  await page.getByRole('tab', { name: 'Deploy', exact: true }).click();
  await page.locator('#deploy-panel').waitFor({ state: 'visible' });
  await targets;
  await page.locator('#host-status').filter({ hasText: /^(Docker Compose|Could not ask)/ }).waitFor();
  await page.locator('#deploy-status').filter({ hasText: /^(?!Updating preview|Generating files).+/ }).waitFor();
  console.log(`Deploy preview: ${await page.locator('#deploy-status').textContent()}`);
  await hold('Deploy: actual preview and environment.', 2500);
  await page.locator('#apply-deployment').scrollIntoViewIfNeeded();
  if (!await page.locator('#apply-deployment').isDisabled()) throw new Error('Build and apply is enabled. This rehearsal requires a composer without apply authorization.');
  console.log(`Host: ${await page.locator('#host-status').textContent()}`);
  await hold('Observed: Build and apply is disabled. No deployment action is invoked.', 3500);
}

async function filmTour(page, frontend) {
  const hold = async (label, milliseconds = 2000) => { console.log(label); await page.waitForTimeout(milliseconds); };
  const choose = async (slot, name) => {
    await page.getByRole('button', { name: slot, exact: true }).click();
    await page.locator('#node-dialog').waitFor({ state: 'visible' });
    await page.locator('#node-options .node-choice').filter({ hasText: new RegExp(`^${name}`) }).click();
    await page.locator('#node-dialog').waitFor({ state: 'hidden' });
  };
  const move = async (node, segment) => {
    await page.getByRole('button', { name: `Move ${node} to segment`, exact: true }).click();
    await page.locator('#boundary-dialog').waitFor({ state: 'visible' });
    await page.locator('#boundary-name').fill(segment);
    await hold(`Place only ${node} in segment ${segment}.`, 1500);
    await page.getByRole('button', { name: 'Apply placement', exact: true }).click();
    await page.locator(`[aria-label="Segment ${segment}"] [data-deployment-member="${node}"]`).waitFor();
    await page.locator('#flow-status').filter({ hasText: /^Placement valid/ }).waitFor();
  };
  await page.locator('#catalog-compatible').uncheck();
  await page.locator('#catalog-grouping').selectOption('packages');
  for (const name of ['acme-gdl-nodes', 'gdl-demo-nodes']) await page.locator(`.package[data-package="${name}@0.1.0"] > summary`).click();
  await hold('Catalog: reference and ACME packages, composed from their actual Rust sources.', 2500);
  await page.getByRole('button', { name: 'Message types', exact: true }).click();
  const reexport = page.locator('.package[data-package="acme-gdl-nodes@0.1.0"] .resource.message').filter({ hasText: /Chunk/ });
  if (await reexport.count()) {
    await reexport.locator('summary').click();
    await reexport.getByText('acme_gdl_nodes::Chunk', { exact: true }).waitFor();
    await hold('ACME exposes the shared Chunk type through a marked public re-export.', 3000);
  } else console.log('Composer Message types does not display marked re-exports; no alias shot is captured.');
  await page.getByRole('button', { name: 'Nodes', exact: true }).click();
  await page.locator('#catalog-grouping').selectOption('flat');
  await page.getByRole('button', { name: 'Source settings', exact: true }).click();
  await page.locator('#source-dialog').waitFor();
  await page.locator('#source-frontend').selectOption(await frontend.getAttribute('value'));
  await page.locator('#source-name').fill('Http');
  await hold('HttpFrontend supplies the typed Prompt → stream Chunk contract.');
  await page.locator('#source-dialog').getByRole('button', { name: 'Done', exact: true }).click();
  await choose('Add pipeline node', 'MockEngine');
  await page.getByRole('button', { name: 'Settings for MockEngine', exact: true }).click();
  await page.locator('#settings-dialog').getByRole('textbox', { name: 'Model label', exact: true }).fill('film-engine');
  await page.locator('#settings-dialog').getByRole('checkbox', { name: 'Uppercase response', exact: true }).check();
  await hold('Engine graph settings: model label film-engine; uppercase enabled.', 3000);
  await page.locator('#settings-dialog').getByRole('button', { name: 'Done', exact: true }).click();
  await choose('add a backward node', 'UsageTap');
  await page.getByRole('button', { name: 'Settings for UsageTap', exact: true }).click();
  await page.locator('#settings-dialog').getByRole('textbox', { name: 'Tenant', exact: true }).fill('acme-film');
  await page.locator('#settings-dialog').getByRole('checkbox', { name: 'Accounting enabled', exact: true }).check();
  await hold('Vendor graph settings: tenant acme-film; accounting enabled. Accounting URL is runtime-only.', 3000);
  await page.locator('#settings-dialog').getByRole('button', { name: 'Done', exact: true }).click();
  await page.locator('#flow-status').filter({ hasText: /^Flow complete/ }).waitFor();
  await page.locator('#node-count').filter({ hasText: /^3 nodes$/ }).waitFor();
  await page.getByRole('button', { name: 'Fit', exact: true }).click();
  await hold('Observed: source, engine, and response-only vendor tap form a valid three-node graph.', 3000);
  await page.getByRole('tab', { name: 'Segments', exact: true }).click();
  await page.locator('.deployment-view').waitFor();
  await move('MockEngine', 'engine'); await move('UsageTap', 'accounting');
  await page.locator('#workspace-layout').selectOption('horizontal');
  await page.getByRole('button', { name: 'Fit', exact: true }).click();
  await hold('Observed: three independently placed segments, with the same typed graph.', 3500);
  const targets = page.waitForResponse(response => new URL(response.url()).pathname === '/api/targets', { timeout: 15000 });
  await page.getByRole('tab', { name: 'Deploy', exact: true }).click();
  await page.locator('#deploy-panel').waitFor(); await targets;
  await page.getByRole('textbox', { name: 'accounting: Runtime config', exact: true }).fill('[node.UsageTap]\naccounting_url = "http://127.0.0.1:8091/usage"\n');
  await hold('Runtime-only settings are explicit TOML. This accounting URL is for the native local demonstration.', 3000);
  await page.locator('#host-status').filter({ hasText: /^(Docker Compose|Could not ask)/ }).waitFor();
  await page.locator('#deploy-status').filter({ hasText: /^(?!Updating preview|Generating files).+/ }).waitFor();
  console.log(`Deploy preview: ${await page.locator('#deploy-status').textContent()}`);
  await page.locator('#apply-deployment').scrollIntoViewIfNeeded();
  if (!await page.locator('#apply-deployment').isDisabled()) throw new Error('Build and apply must remain disabled for this rehearsal.');
  console.log(`Host: ${await page.locator('#host-status').textContent()}`);
  await hold('Observed: actual host capabilities and disabled Build and apply. Nothing is deployed.', 3500);
}

const help = `Record a genuine local GDL composer rehearsal.

Usage: node tools/video/record.mjs [--fixture api|film] [--url http://127.0.0.1:8089/] [--out tools/video/work/composer.mp4] [--dry-run]

Requires local playwright-core, ffmpeg-static, installed Chrome, and a running
composer with the selected fixture catalog. The API fixture is the default.
The film fixture uses the reference and ACME demo crates. --dry-run checks prerequisites without
graph actions or a video. The actual recording uses an isolated browser context,
clicks only real UI controls, and never invokes Build and apply. The WebM browser
recording remains under tools/video/work/recordings. The MP4 is silent 1920x1080.
Composer recordings do not establish native runtime or accounting receipts.
`;
if (process.argv[1] && pathToFileURL(resolve(process.argv[1])).href === import.meta.url) {
  try { const options = parseArgs(process.argv.slice(2)); if (options.help) console.log(help); else await run(options); }
  catch (error) { console.error(error.message); process.exitCode = 1; }
}
