#!/usr/bin/env node

/**
 * Shopify Media Sync Script
 *
 * Downloads all media files (product images, collection images, uploaded files)
 * from Shopify's CDN to a local directory with incremental caching.
 * Designed to pair with rclone for Google Drive sync.
 *
 * Usage:
 *   node shopify-media-sync.mjs
 *
 * Environment variables:
 *   SHOPIFY_STORE        – mystore.myshopify.com
 *   SHOPIFY_ACCESS_TOKEN – Admin API access token (legacy admin-managed custom
 *                          apps). Optional if CLIENT_ID/SECRET are set.
 *   SHOPIFY_CLIENT_ID    – Dev Dashboard app Client ID (Klant-ID).
 *   SHOPIFY_CLIENT_SECRET – Dev Dashboard app Client Secret (Geheim).
 *   SHOPIFY_API_VERSION  – API version (default: 2025-01)
 *   SHOPIFY_MIN_REQUEST_INTERVAL_MS – Min delay between API requests in ms
 *                          (default: 560). Raise to ~N × 560 when running N
 *                          jobs concurrently against the same token.
 *   MEDIA_OUTPUT_DIR     – Output directory (default: /tmp/shopify-media)
 *   MEDIA_CONCURRENCY    – Parallel downloads (default: 6)
 *
 * Output:
 *   A structured folder of media files + manifest.json.
 *   Prints the output directory path to stdout for n8n.
 *
 * Incremental behavior:
 *   - Existing files are kept and overwritten only when the remote size differs
 *   - New files are downloaded
 *   - Deleted files on Shopify are NOT removed locally (safe retention)
 *
 * @version 1.0.0
 * @license GPL-3.0-or-later
 */

import { writeFileSync, mkdirSync, existsSync, statSync } from 'node:fs';
import { join, extname, basename } from 'node:path';
import { pipeline } from 'node:stream/promises';
import { createWriteStream } from 'node:fs';

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const STORE = process.env.SHOPIFY_STORE;
let TOKEN = process.env.SHOPIFY_ACCESS_TOKEN;
const CLIENT_ID = process.env.SHOPIFY_CLIENT_ID;
const CLIENT_SECRET = process.env.SHOPIFY_CLIENT_SECRET;
const API_VERSION = process.env.SHOPIFY_API_VERSION || '2025-01';
const OUTPUT_DIR = process.env.MEDIA_OUTPUT_DIR || '/tmp/shopify-media';
const CONCURRENCY = parseInt(process.env.MEDIA_CONCURRENCY || '6', 10);

if (!STORE || (!TOKEN && !(CLIENT_ID && CLIENT_SECRET))) {
  console.error(
    'ERROR: SHOPIFY_STORE is required, plus either SHOPIFY_ACCESS_TOKEN ' +
      '(legacy admin-managed custom apps) or both SHOPIFY_CLIENT_ID and ' +
      'SHOPIFY_CLIENT_SECRET (Dev Dashboard apps).'
  );
  process.exit(1);
}

const BASE_URL = `https://${STORE}/admin/api/${API_VERSION}`;
const GQL_URL = `https://${STORE}/admin/api/${API_VERSION}/graphql.json`;

// API rate limiting (same as backup script). Shopify's REST limit (2 req/s) is
// shared per store/access token across all processes, so raise this via
// SHOPIFY_MIN_REQUEST_INTERVAL_MS (~N × 560 ms) when this runs alongside other
// backup/sync jobs on the same token.
const MIN_REQUEST_INTERVAL_MS = Math.max(
  0,
  parseInt(process.env.SHOPIFY_MIN_REQUEST_INTERVAL_MS || '560', 10) || 560
);
let lastApiRequestTime = 0;

// ---------------------------------------------------------------------------
// Logging & stats
// ---------------------------------------------------------------------------

const stats = {
  downloaded: 0,
  skipped: 0,
  failed: 0,
  totalBytes: 0,
  errors: [],
};

function log(msg) {
  process.stderr.write(`[${new Date().toISOString()}] ${msg}\n`);
}

// ---------------------------------------------------------------------------
// API helpers (for fetching file/product lists)
// ---------------------------------------------------------------------------

async function apiThrottle() {
  const now = Date.now();
  const elapsed = now - lastApiRequestTime;
  if (elapsed < MIN_REQUEST_INTERVAL_MS) {
    await new Promise((r) => setTimeout(r, MIN_REQUEST_INTERVAL_MS - elapsed));
  }
  lastApiRequestTime = Date.now();
}

const FETCH_RETRIES = 5;

async function shopifyApiFetch(url, options = {}, retries = FETCH_RETRIES) {
  await apiThrottle();

  let response;
  try {
    response = await fetch(url, {
      ...options,
      headers: {
        'X-Shopify-Access-Token': TOKEN,
        'Content-Type': 'application/json',
        Accept: 'application/json',
        ...options.headers,
      },
    });
  } catch (err) {
    if (retries > 0) {
      const wait = 2000 * 2 ** (FETCH_RETRIES - retries);
      log(`  ⏳ Network error (${err.message}), retrying in ${wait}ms...`);
      await new Promise((r) => setTimeout(r, wait));
      return shopifyApiFetch(url, options, retries - 1);
    }
    throw new Error(`Network failure after retries: ${err.message} (${url})`);
  }

  if (response.status === 429 || response.status >= 500) {
    if (retries > 0) {
      const retryAfter =
        Math.max(1000, (parseFloat(response.headers.get('Retry-After')) || 2) * 1000) +
        Math.floor(Math.random() * 1000);
      log(`  ⏳ API rate limited (${response.status}), retrying...`);
      await new Promise((r) => setTimeout(r, retryAfter));
      return shopifyApiFetch(url, options, retries - 1);
    }
    throw new Error(`Shopify API ${response.status} after retries: ${url}`);
  }

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Shopify API ${response.status}: ${body}`);
  }

  return response;
}

function getNextPageUrl(linkHeader) {
  if (!linkHeader) return null;
  const match = linkHeader.match(/<([^>]+)>;\s*rel="next"/);
  return match ? match[1] : null;
}

async function fetchAllRestPages(endpoint, rootKey, params = {}) {
  const allItems = [];
  const query = new URLSearchParams({ limit: '250', ...params });
  let url = `${BASE_URL}${endpoint}?${query}`;

  while (url) {
    const response = await shopifyApiFetch(url);
    const data = await response.json();
    allItems.push(...(data[rootKey] || []));
    url = getNextPageUrl(response.headers.get('link'));
  }

  return allItems;
}

async function graphqlRequest(query, variables = {}) {
  await apiThrottle();

  const response = await shopifyApiFetch(GQL_URL, {
    method: 'POST',
    body: JSON.stringify({ query, variables }),
  });

  const result = await response.json();
  if (result.errors) {
    throw new Error(`GraphQL errors: ${JSON.stringify(result.errors)}`);
  }
  return result.data;
}

// ---------------------------------------------------------------------------
// File download helpers
// ---------------------------------------------------------------------------

/**
 * Sanitize a string for use as a filename/directory name.
 */
function sanitize(str) {
  return str
    .replace(/[<>:"/\\|?*\x00-\x1f]/g, '-')
    .replace(/\s+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '')
    .slice(0, 200);
}

/**
 * Extract a clean filename from a Shopify CDN URL.
 * Strips query parameters and version suffixes.
 */
function filenameFromUrl(url) {
  try {
    const parsed = new URL(url);
    const path = parsed.pathname;
    let name = basename(path);
    // Remove Shopify's version query param artifacts if baked into the name
    name = name.replace(/\?.*$/, '');
    return sanitize(name) || 'unnamed';
  } catch {
    return 'unnamed';
  }
}

/**
 * Get file extension from URL or content-type.
 */
function getExtension(url, contentType) {
  const fromUrl = extname(filenameFromUrl(url)).toLowerCase();
  if (fromUrl && fromUrl !== '.') return fromUrl;

  // Fallback to content-type
  const typeMap = {
    'image/jpeg': '.jpg',
    'image/png': '.png',
    'image/gif': '.gif',
    'image/webp': '.webp',
    'image/svg+xml': '.svg',
    'video/mp4': '.mp4',
    'video/quicktime': '.mov',
    'application/pdf': '.pdf',
    'model/gltf-binary': '.glb',
    'model/gltf+json': '.gltf',
  };

  return typeMap[contentType] || '';
}

/**
 * Download a file from a URL to a local path.
 * Skips download if the local file exists and has the same size.
 *
 * @param {string} url – Source URL (CDN, publicly accessible)
 * @param {string} destPath – Local destination path
 * @returns {Promise<{action: string, bytes: number}>}
 */
async function downloadFile(url, destPath, retries = 2) {
  try {
    // HEAD request to get file size for comparison
    const headResponse = await fetch(url, { method: 'HEAD' });

    if (!headResponse.ok) {
      throw new Error(`HEAD ${headResponse.status} for ${url}`);
    }

    const remoteSize = parseInt(
      headResponse.headers.get('content-length') || '0',
      10
    );

    // Check if we already have this file with matching size
    if (existsSync(destPath)) {
      const localSize = statSync(destPath).size;
      if (remoteSize > 0 && localSize === remoteSize) {
        return { action: 'skipped', bytes: 0 };
      }
    }

    // Ensure directory exists
    const dir = join(destPath, '..');
    if (!existsSync(dir)) {
      mkdirSync(dir, { recursive: true });
    }

    // Download
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`GET ${response.status} for ${url}`);
    }

    const fileStream = createWriteStream(destPath);
    await pipeline(response.body, fileStream);

    const downloadedSize = statSync(destPath).size;
    return { action: 'downloaded', bytes: downloadedSize };
  } catch (err) {
    if (retries > 0) {
      await new Promise((r) => setTimeout(r, 1000));
      return downloadFile(url, destPath, retries - 1);
    }
    throw err;
  }
}

/**
 * Process a batch of download tasks with concurrency control.
 *
 * @param {Array<{url: string, destPath: string, label: string}>} tasks
 */
async function downloadBatch(tasks) {
  let completed = 0;
  const total = tasks.length;

  // Simple concurrency pool
  const pool = [];
  const results = [];

  for (const task of tasks) {
    const promise = (async () => {
      try {
        const result = await downloadFile(task.url, task.destPath);

        if (result.action === 'downloaded') {
          stats.downloaded++;
          stats.totalBytes += result.bytes;
        } else {
          stats.skipped++;
        }

        completed++;
        if (completed % 25 === 0 || completed === total) {
          log(
            `  📥 ${completed}/${total} files processed (${stats.downloaded} downloaded, ${stats.skipped} skipped)`
          );
        }

        return { ...task, ...result };
      } catch (err) {
        stats.failed++;
        stats.errors.push(`${task.label}: ${err.message}`);
        completed++;
        return { ...task, action: 'failed', error: err.message };
      }
    })();

    pool.push(promise);

    // Maintain concurrency limit
    if (pool.length >= CONCURRENCY) {
      const settled = await Promise.race(
        pool.map((p, i) => p.then((r) => ({ index: i, result: r })))
      );
      results.push(settled.result);
      pool.splice(settled.index, 1);
    }
  }

  // Wait for remaining
  const remaining = await Promise.all(pool);
  results.push(...remaining);

  return results;
}

// ---------------------------------------------------------------------------
// Source collectors: gather all download tasks
// ---------------------------------------------------------------------------

/**
 * Collect product image download tasks from the Shopify REST API.
 */
async function collectProductImages() {
  log('🛍️  Fetching product image list...');
  const products = await fetchAllRestPages('/products.json', 'products');
  const tasks = [];

  for (const product of products) {
    const handle = sanitize(product.handle || `product-${product.id}`);
    const images = product.images || [];

    for (const image of images) {
      if (!image.src) continue;

      const filename = `${image.position || 0}_${filenameFromUrl(image.src)}`;
      const destPath = join(OUTPUT_DIR, 'products', handle, filename);

      tasks.push({
        url: image.src,
        destPath,
        label: `product/${handle}/${filename}`,
        type: 'product_image',
        shopify_id: image.id,
        product_id: product.id,
        product_handle: product.handle,
      });
    }
  }

  log(`  → ${tasks.length} product images from ${products.length} products`);
  return tasks;
}

/**
 * Collect collection image download tasks.
 */
async function collectCollectionImages() {
  log('📂 Fetching collection image list...');
  const tasks = [];

  const customCollections = await fetchAllRestPages(
    '/custom_collections.json',
    'custom_collections'
  );
  const smartCollections = await fetchAllRestPages(
    '/smart_collections.json',
    'smart_collections'
  );

  const allCollections = [...customCollections, ...smartCollections];

  for (const col of allCollections) {
    const imageUrl = col.image?.src;
    if (!imageUrl) continue;

    const handle = sanitize(col.handle || `collection-${col.id}`);
    const filename = `${handle}${getExtension(imageUrl, '') || '.jpg'}`;
    const destPath = join(OUTPUT_DIR, 'collections', filename);

    tasks.push({
      url: imageUrl,
      destPath,
      label: `collection/${filename}`,
      type: 'collection_image',
      shopify_id: col.id,
    });
  }

  log(`  → ${tasks.length} collection images`);
  return tasks;
}

/**
 * Collect all uploaded files from Settings → Files via GraphQL.
 */
async function collectUploadedFiles() {
  log('📁 Fetching uploaded files list via GraphQL...');
  const tasks = [];

  const query = `
    query Files($cursor: String) {
      files(first: 50, after: $cursor, sortKey: CREATED_AT) {
        edges {
          cursor
          node {
            __typename
            createdAt
            updatedAt
            alt
            fileStatus
            ... on GenericFile {
              id
              url
              mimeType
              originalFileSize
            }
            ... on MediaImage {
              id
              mimeType
              originalFileSize
              image {
                url
                width
                height
              }
            }
            ... on Video {
              id
              filename
              originalSource {
                url
                mimeType
                fileSize
              }
            }
          }
        }
        pageInfo {
          hasNextPage
        }
      }
    }
  `;

  let cursor = null;
  let hasNextPage = true;
  let pageCount = 0;

  while (hasNextPage) {
    const data = await graphqlRequest(query, { cursor });
    const edges = data.files?.edges || [];

    for (const edge of edges) {
      const node = edge.node;
      if (!node || node.fileStatus !== 'READY') continue;

      let url = null;
      let subdir = 'other';
      let filename = null;

      switch (node.__typename) {
        case 'MediaImage':
          url = node.image?.url;
          subdir = 'images';
          filename = filenameFromUrl(url || '');
          break;

        case 'GenericFile':
          url = node.url;
          subdir = 'documents';
          filename = filenameFromUrl(url || '');
          break;

        case 'Video':
          url = node.originalSource?.url;
          subdir = 'videos';
          filename = sanitize(node.filename || filenameFromUrl(url || ''));
          break;

        default:
          continue;
      }

      if (!url || !filename) continue;

      // Deduplicate filenames by appending a short ID suffix
      const idSuffix = (node.id || '').split('/').pop()?.slice(-6) || '';
      const ext = extname(filename);
      const base = filename.slice(0, -ext.length || undefined);
      const uniqueFilename = `${base}_${idSuffix}${ext}`;

      const destPath = join(OUTPUT_DIR, 'files', subdir, uniqueFilename);

      tasks.push({
        url,
        destPath,
        label: `files/${subdir}/${uniqueFilename}`,
        type: 'uploaded_file',
        shopify_id: node.id,
        content_type: node.mimeType || node.originalSource?.mimeType || '',
        file_typename: node.__typename,
      });
    }

    hasNextPage = data.files?.pageInfo?.hasNextPage ?? false;
    if (edges.length > 0) {
      cursor = edges[edges.length - 1].cursor;
    } else {
      hasNextPage = false;
    }

    pageCount++;
    if (pageCount % 5 === 0) {
      log(`  ...fetched ${tasks.length} files so far (page ${pageCount})`);
    }
  }

  log(`  → ${tasks.length} uploaded files`);
  return tasks;
}

// ---------------------------------------------------------------------------
// Manifest
// ---------------------------------------------------------------------------

function writeManifest(allTasks) {
  const manifest = {
    _meta: {
      version: '1.0.0',
      store: STORE,
      created_at: new Date().toISOString(),
      total_files: allTasks.length,
      downloaded: stats.downloaded,
      skipped: stats.skipped,
      failed: stats.failed,
      total_bytes_downloaded: stats.totalBytes,
    },
    files: allTasks.map((t) => ({
      local_path: t.destPath.replace(OUTPUT_DIR + '/', ''),
      source_url: t.url,
      type: t.type,
      shopify_id: t.shopify_id,
      action: t.action || 'pending',
    })),
  };

  const manifestPath = join(OUTPUT_DIR, 'manifest.json');
  writeFileSync(manifestPath, JSON.stringify(manifest, null, 2), 'utf-8');
  log(`📋 Manifest written: ${manifestPath}`);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  const startTime = Date.now();
  log('🚀 Starting Shopify media sync...');
  log(`   Store: ${STORE}`);
  log(`   Output: ${OUTPUT_DIR}`);
  log(`   Concurrency: ${CONCURRENCY}`);
  log('');

  // Ensure base directories exist
  for (const dir of [
    OUTPUT_DIR,
    join(OUTPUT_DIR, 'products'),
    join(OUTPUT_DIR, 'collections'),
    join(OUTPUT_DIR, 'files', 'images'),
    join(OUTPUT_DIR, 'files', 'documents'),
    join(OUTPUT_DIR, 'files', 'videos'),
    join(OUTPUT_DIR, 'files', 'other'),
  ]) {
    if (!existsSync(dir)) {
      mkdirSync(dir, { recursive: true });
    }
  }

  // Collect all download tasks
  const productImageTasks = await collectProductImages();
  const collectionImageTasks = await collectCollectionImages();
  const uploadedFileTasks = await collectUploadedFiles();

  const allTasks = [
    ...productImageTasks,
    ...collectionImageTasks,
    ...uploadedFileTasks,
  ];

  log('');
  log(`📊 Total files to process: ${allTasks.length}`);
  log(`   Product images: ${productImageTasks.length}`);
  log(`   Collection images: ${collectionImageTasks.length}`);
  log(`   Uploaded files: ${uploadedFileTasks.length}`);
  log('');

  if (allTasks.length === 0) {
    log('ℹ️  No media files found. Nothing to do.');
    console.log(OUTPUT_DIR);
    return;
  }

  // Download all files
  log('📥 Starting downloads...');
  const results = await downloadBatch(allTasks);

  // Merge results back into tasks for the manifest
  for (let i = 0; i < allTasks.length; i++) {
    if (results[i]) {
      allTasks[i].action = results[i].action;
    }
  }

  // Write manifest
  writeManifest(allTasks);

  // Summary
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  const totalMB = (stats.totalBytes / 1024 / 1024).toFixed(2);

  log('');
  log('═══════════════════════════════════════');
  log(`✅ Media sync complete in ${elapsed}s`);
  log(`   Downloaded: ${stats.downloaded} files (${totalMB} MB)`);
  log(`   Skipped (unchanged): ${stats.skipped}`);
  log(`   Failed: ${stats.failed}`);

  if (stats.errors.length > 0) {
    log('');
    log('⚠️  Errors:');
    for (const err of stats.errors.slice(0, 20)) {
      log(`   - ${err}`);
    }
    if (stats.errors.length > 20) {
      log(`   ... and ${stats.errors.length - 20} more`);
    }
  }

  log('═══════════════════════════════════════');

  // Print output dir to stdout for n8n
  console.log(OUTPUT_DIR);
}

/**
 * Resolve the Admin API access token.
 *
 * If SHOPIFY_ACCESS_TOKEN is set (legacy admin-managed custom apps), it is used
 * directly. Otherwise the Client ID / Client Secret from a Dev Dashboard app
 * are exchanged for a short-lived (~24h) token via the client credentials
 * grant. A single sync run fits comfortably inside the token lifetime.
 *
 * @see https://shopify.dev/docs/apps/build/dev-dashboard/get-api-access-tokens
 */
async function resolveToken() {
  if (TOKEN) return;

  log('🔑 Exchanging client credentials for a short-lived access token...');
  const response = await fetch(`https://${STORE}/admin/oauth/access_token`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      grant_type: 'client_credentials',
      client_id: CLIENT_ID,
      client_secret: CLIENT_SECRET,
    }),
  });

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Token exchange failed (${response.status}): ${body}`);
  }

  const data = await response.json();
  if (!data.access_token) {
    throw new Error('Token exchange returned no access_token');
  }
  TOKEN = data.access_token;
}

resolveToken()
  .then(main)
  .catch((err) => {
    log(`❌ Media sync failed: ${err.message}`);
    log(err.stack);
    process.exit(1);
  });
