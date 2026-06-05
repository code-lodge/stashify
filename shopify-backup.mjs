#!/usr/bin/env node

/**
 * Shopify Store Backup Script
 *
 * Comprehensive backup of all Shopify store data via the Admin API.
 * Designed to run from n8n via Execute Command node.
 *
 * Usage:
 *   node shopify-backup.mjs
 *
 * Environment variables:
 *   SHOPIFY_STORE        – mystore.myshopify.com (without https://)
 *   SHOPIFY_ACCESS_TOKEN – Admin API access token (legacy admin-managed
 *                          custom apps). Optional if CLIENT_ID/SECRET are set.
 *   SHOPIFY_CLIENT_ID    – Dev Dashboard app Client ID (Klant-ID). Used with
 *                          SHOPIFY_CLIENT_SECRET to fetch a short-lived token.
 *   SHOPIFY_CLIENT_SECRET – Dev Dashboard app Client Secret (Geheim).
 *   SHOPIFY_API_VERSION  – API version (default: 2025-01)
 *   SHOPIFY_MIN_REQUEST_INTERVAL_MS – Min delay between API requests in ms
 *                          (default: 560). Raise to ~N × 560 when running N
 *                          jobs concurrently against the same token.
 *   BACKUP_OUTPUT_DIR    – Output directory (default: /tmp/shopify-backups)
 *
 * Output:
 *   A timestamped JSON file containing all store data. The document is built
 *   incrementally with a streaming writer: each resource is fetched page by
 *   page and flushed straight to the output file, so peak memory stays bounded
 *   regardless of store size (only compact id projections are retained for
 *   dependent lookups). The result is a single compact JSON object with the
 *   same shape shopify-restore.mjs expects.
 *   Prints the output filepath to stdout for n8n to pick up.
 *
 * Conforms to:
 *   - ECMA-262 (ES2022+ modules, async/await, structured error handling)
 *   - OWASP API Security (token-based auth, no secrets in logs)
 *   - GDPR awareness (customer data flagged, retention notes)
 *   - RFC 9112 HTTP/1.1 (proper header handling for pagination)
 *
 * @version 1.0.0
 * @license GPL-3.0-or-later
 */

import { createWriteStream, mkdirSync, existsSync } from 'node:fs';
import { join } from 'node:path';

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const STORE = process.env.SHOPIFY_STORE;
let TOKEN = process.env.SHOPIFY_ACCESS_TOKEN;
const CLIENT_ID = process.env.SHOPIFY_CLIENT_ID;
const CLIENT_SECRET = process.env.SHOPIFY_CLIENT_SECRET;
const API_VERSION = process.env.SHOPIFY_API_VERSION || '2025-01';
const OUTPUT_DIR = process.env.BACKUP_OUTPUT_DIR || '/tmp/shopify-backups';

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

// Shopify REST rate limit: bucket of 40, leak rate 2/s (per store/access token,
// shared across ALL processes using that token). The default 560 ms keeps a
// single process at ~1.8 req/s, just under the 2/s ceiling.
//
// The bucket is shared, so if you run N backups concurrently against the same
// store you must slow each one to ~1.8/N req/s, i.e. roughly N × 560 ms.
// Override via SHOPIFY_MIN_REQUEST_INTERVAL_MS (e.g. 1120 for two concurrent
// runs, 1680 for three). Shopify Plus has a 4/s ceiling and can use a smaller
// value.
const MIN_REQUEST_INTERVAL_MS = Math.max(
  0,
  parseInt(process.env.SHOPIFY_MIN_REQUEST_INTERVAL_MS || '560', 10) || 560
);
let lastRequestTime = 0;

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

/**
 * Throttle requests to respect Shopify's REST API rate limits.
 */
async function throttle() {
  const now = Date.now();
  const elapsed = now - lastRequestTime;
  if (elapsed < MIN_REQUEST_INTERVAL_MS) {
    await new Promise((r) => setTimeout(r, MIN_REQUEST_INTERVAL_MS - elapsed));
  }
  lastRequestTime = Date.now();
}

/**
 * Make an authenticated REST API request with retry logic.
 *
 * @param {string} url – Full URL to request
 * @param {object} [options] – Additional fetch options
 * @param {number} [retries=3] – Number of retries on 429/5xx
 * @returns {Promise<Response>} – The fetch Response object
 */
async function shopifyFetch(url, options = {}, retries = 3) {
  await throttle();

  const response = await fetch(url, {
    ...options,
    headers: {
      'X-Shopify-Access-Token': TOKEN,
      'Content-Type': 'application/json',
      Accept: 'application/json',
      ...options.headers,
    },
  });

  if (response.status === 429 || response.status >= 500) {
    if (retries > 0) {
      const retryAfter =
        parseInt(response.headers.get('Retry-After') || '2', 10) * 1000;
      log(`  ⏳ Rate limited or server error (${response.status}), retrying in ${retryAfter}ms...`);
      await new Promise((r) => setTimeout(r, retryAfter));
      return shopifyFetch(url, options, retries - 1);
    }
    throw new Error(`Shopify API error ${response.status} after retries: ${url}`);
  }

  if (!response.ok) {
    const body = await response.text();
    throw new Error(`Shopify API ${response.status}: ${body}`);
  }

  return response;
}

/**
 * Fetch all pages of a paginated REST endpoint.
 * Shopify uses cursor-based pagination via Link headers (RFC 8288).
 *
 * @param {string} endpoint – REST endpoint path, e.g. '/products.json'
 * @param {string} rootKey – JSON root key, e.g. 'products'
 * @param {object} [params] – Additional query parameters
 * @returns {Promise<Array>} – All items across all pages
 */
async function fetchAllPages(endpoint, rootKey, params = {}) {
  const allItems = [];
  for await (const item of streamPages(endpoint, rootKey, params)) {
    allItems.push(item);
  }
  return allItems;
}

/**
 * Stream all items of a paginated REST endpoint, one item at a time.
 *
 * Unlike fetchAllPages this never accumulates the full result set: at most one
 * page (250 items) is held in memory while it is yielded onward. Use this for
 * large collections that are written straight to the backup file.
 *
 * @param {string} endpoint – REST endpoint path, e.g. '/products.json'
 * @param {string} rootKey – JSON root key, e.g. 'products'
 * @param {object} [params] – Additional query parameters
 * @yields {object} – Each item across all pages
 */
async function* streamPages(endpoint, rootKey, params = {}) {
  const query = new URLSearchParams({ limit: '250', ...params });
  let url = `${BASE_URL}${endpoint}?${query}`;

  while (url) {
    const response = await shopifyFetch(url);
    const data = await response.json();
    for (const item of data[rootKey] || []) {
      yield item;
    }
    // Parse Link header for next page URL
    url = getNextPageUrl(response.headers.get('link'));
  }
}

/**
 * Extract the "next" page URL from a Shopify Link header.
 *
 * @param {string|null} linkHeader – The Link response header value
 * @returns {string|null} – The next page URL, or null if none
 */
function getNextPageUrl(linkHeader) {
  if (!linkHeader) return null;
  const match = linkHeader.match(/<([^>]+)>;\s*rel="next"/);
  return match ? match[1] : null;
}

/**
 * Make an authenticated GraphQL request.
 *
 * @param {string} query – GraphQL query string
 * @param {object} [variables] – GraphQL variables
 * @returns {Promise<object>} – The response data
 */
async function graphqlRequest(query, variables = {}) {
  await throttle();

  const response = await shopifyFetch(GQL_URL, {
    method: 'POST',
    body: JSON.stringify({ query, variables }),
  });

  const result = await response.json();

  if (result.errors) {
    throw new Error(`GraphQL errors: ${JSON.stringify(result.errors)}`);
  }

  return result.data;
}

/**
 * Fetch all pages of a paginated GraphQL connection.
 *
 * @param {string} queryTemplate – GraphQL query with $cursor variable
 * @param {string[]} dataPath – Path to the connection in the response, e.g. ['products']
 * @param {object} [extraVars] – Additional GraphQL variables
 * @returns {Promise<Array>} – All nodes across all pages
 */
async function fetchAllGraphQLPages(queryTemplate, dataPath, extraVars = {}) {
  const allNodes = [];
  for await (const node of streamGraphQLPages(queryTemplate, dataPath, extraVars)) {
    allNodes.push(node);
  }
  return allNodes;
}

/**
 * Stream all nodes of a paginated GraphQL connection, one node at a time.
 *
 * @param {string} queryTemplate – GraphQL query with $cursor variable
 * @param {string[]} dataPath – Path to the connection in the response
 * @param {object} [extraVars] – Additional GraphQL variables
 * @yields {object} – Each node across all pages
 */
async function* streamGraphQLPages(queryTemplate, dataPath, extraVars = {}) {
  let cursor = null;
  let hasNextPage = true;

  while (hasNextPage) {
    const data = await graphqlRequest(queryTemplate, { ...extraVars, cursor });

    // Navigate to the connection in the response
    let connection = data;
    for (const key of dataPath) {
      connection = connection[key];
    }

    const edges = connection.edges || [];
    for (const edge of edges) {
      yield edge.node;
    }

    hasNextPage = connection.pageInfo?.hasNextPage ?? false;
    if (edges.length > 0) {
      cursor = edges[edges.length - 1].cursor;
    } else {
      hasNextPage = false;
    }
  }
}

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------

function log(msg) {
  // Write to stderr so stdout stays clean for n8n to parse the output path
  process.stderr.write(`[${new Date().toISOString()}] ${msg}\n`);
}

// ---------------------------------------------------------------------------
// Streaming JSON writer
// ---------------------------------------------------------------------------

/**
 * Writes a JSON document incrementally to a Node writable stream.
 *
 * Supports arbitrarily nested objects/arrays via a small container stack, so
 * large collections can be emitted item by item instead of being serialized
 * from one giant in-memory object. Honours stream backpressure (awaits 'drain'
 * when the internal buffer fills) and tracks the number of bytes written.
 *
 * Output is compact (no pretty-printing) to minimise file size; JSON.parse on
 * the restore side is indifferent to whitespace, so this stays compatible.
 */
class JsonStreamWriter {
  constructor(stream) {
    this.stream = stream;
    this.bytes = 0;
    this.stack = [];
    this._pendingValue = false;
  }

  async _write(str) {
    this.bytes += Buffer.byteLength(str);
    if (!this.stream.write(str)) {
      await new Promise((resolve) => this.stream.once('drain', resolve));
    }
  }

  // Emit a separator before a value, unless it directly follows a key.
  async _preValue() {
    if (this._pendingValue) {
      this._pendingValue = false;
      return;
    }
    const top = this.stack[this.stack.length - 1];
    if (top) {
      if (!top.first) await this._write(',');
      top.first = false;
    }
  }

  async beginObject() {
    await this._preValue();
    await this._write('{');
    this.stack.push({ first: true });
  }

  async endObject() {
    this.stack.pop();
    await this._write('}');
  }

  async beginArray() {
    await this._preValue();
    await this._write('[');
    this.stack.push({ first: true });
  }

  async endArray() {
    this.stack.pop();
    await this._write(']');
  }

  /** Write an object key; the next value-producing call fills it. */
  async key(name) {
    const top = this.stack[this.stack.length - 1];
    if (!top.first) await this._write(',');
    top.first = false;
    await this._write(JSON.stringify(String(name)) + ':');
    this._pendingValue = true;
  }

  /** Write a complete JSON value (array item or the value after a key). */
  async value(val) {
    await this._preValue();
    const serialized = JSON.stringify(val);
    await this._write(serialized === undefined ? 'null' : serialized);
  }
}

/**
 * Stream a REST collection into the current key slot as a JSON array.
 *
 * Optionally captures a compact projection of each item (e.g. ids) so dependent
 * resources can be fetched afterwards without retaining the full objects.
 *
 * @param {JsonStreamWriter} w
 * @param {string} key – Output key for the array
 * @param {AsyncIterable<object>} gen – Item source (e.g. streamPages(...))
 * @param {(item: object) => any} [pick] – Projection to capture per item
 * @returns {Promise<{count: number, captured: any[]|null}>}
 */
async function writeArraySection(w, key, gen, pick) {
  await w.key(key);
  await w.beginArray();
  let count = 0;
  const captured = pick ? [] : null;
  for await (const item of gen) {
    await w.value(item);
    if (pick) captured.push(pick(item));
    count++;
  }
  await w.endArray();
  return { count, captured };
}

/**
 * Stream a metafields-by-owner map into the current key slot as a JSON object.
 *
 * Fetches metafields one owner at a time, so only a single owner's metafields
 * are held in memory. Owners with no metafields are omitted (matching the
 * original behaviour).
 *
 * @param {JsonStreamWriter} w
 * @param {string} key – Output key for the map
 * @param {Array<string|number>} ownerIds – Owner ids to fetch metafields for
 * @param {string} ownerPath – Owner REST base, e.g. '/products'
 * @param {string} label – Human label for progress logs
 */
async function writeMetafieldMap(w, key, ownerIds, ownerPath, label) {
  await w.key(key);
  await w.beginObject();
  let count = 0;
  for (const id of ownerIds) {
    const metafields = await fetchAllPages(
      `${ownerPath}/${id}/metafields.json`,
      'metafields'
    );
    if (metafields.length > 0) {
      await w.key(id);
      await w.value(metafields);
    }
    count++;
    if (count % 50 === 0) {
      log(`  ...processed ${count}/${ownerIds.length} ${label}`);
    }
  }
  await w.endObject();
}

// ---------------------------------------------------------------------------
// Resource fetchers (small / whole-value resources)
// ---------------------------------------------------------------------------

async function fetchShop() {
  log('📦 Fetching shop info...');
  const response = await shopifyFetch(`${BASE_URL}/shop.json`);
  const data = await response.json();
  return data.shop;
}

async function fetchPolicies() {
  log('📜 Fetching policies...');
  const response = await shopifyFetch(`${BASE_URL}/policies.json`);
  const data = await response.json();
  return data.policies || [];
}

async function fetchShippingZones() {
  log('🚚 Fetching shipping zones...');
  try {
    const response = await shopifyFetch(`${BASE_URL}/shipping_zones.json`);
    const data = await response.json();
    return data.shipping_zones || [];
  } catch (err) {
    log(`  ⚠️  Could not fetch shipping zones: ${err.message}`);
    return [];
  }
}

async function fetchCountries() {
  log('🌍 Fetching countries/tax settings...');
  try {
    return await fetchAllPages('/countries.json', 'countries');
  } catch (err) {
    log(`  ⚠️  Could not fetch countries: ${err.message}`);
    return [];
  }
}

async function fetchLocations() {
  log('📍 Fetching locations...');
  const response = await shopifyFetch(`${BASE_URL}/locations.json`);
  const data = await response.json();
  return data.locations || [];
}

async function fetchThemes() {
  log('🎨 Fetching themes...');
  const response = await shopifyFetch(`${BASE_URL}/themes.json`);
  const data = await response.json();
  return data.themes || [];
}

async function fetchGiftCards() {
  log('🎁 Fetching gift cards...');
  try {
    return await fetchAllPages('/gift_cards.json', 'gift_cards');
  } catch (err) {
    log(`  ⚠️  Gift cards not available (requires Shopify Plus or specific plan): ${err.message}`);
    return [];
  }
}

// ---------------------------------------------------------------------------
// GraphQL resource fetchers (for resources REST doesn't cover well)
// ---------------------------------------------------------------------------

async function fetchMetafieldDefinitions() {
  log('📐 Fetching metafield definitions...');
  const ownerTypes = [
    'PRODUCT', 'PRODUCTVARIANT', 'COLLECTION', 'CUSTOMER',
    'ORDER', 'SHOP', 'PAGE', 'ARTICLE', 'BLOG', 'LOCATION',
    'COMPANY', 'COMPANY_LOCATION', 'DRAFT_ORDER', 'MARKET',
  ];
  const allDefinitions = {};

  const query = `
    query MetafieldDefinitions($ownerType: MetafieldOwnerType!, $cursor: String) {
      metafieldDefinitions(first: 100, ownerType: $ownerType, after: $cursor) {
        edges {
          cursor
          node {
            id
            name
            namespace
            key
            type {
              name
              category
            }
            description
            ownerType
            pinnedPosition
            validations {
              name
              value
            }
          }
        }
        pageInfo {
          hasNextPage
        }
      }
    }
  `;

  for (const ownerType of ownerTypes) {
    try {
      const nodes = await fetchAllGraphQLPages(
        query,
        ['metafieldDefinitions'],
        { ownerType }
      );
      if (nodes.length > 0) {
        allDefinitions[ownerType] = nodes;
      }
    } catch (err) {
      log(`  ⚠️  Could not fetch definitions for ${ownerType}: ${err.message}`);
    }
  }

  return allDefinitions;
}

async function fetchMetaobjectDefinitions() {
  log('📐 Fetching metaobject definitions...');
  const query = `
    query MetaobjectDefinitions($cursor: String) {
      metaobjectDefinitions(first: 50, after: $cursor) {
        edges {
          cursor
          node {
            id
            name
            type
            description
            displayNameKey
            access {
              admin
              storefront
            }
            capabilities {
              publishable {
                enabled
              }
              translatable {
                enabled
              }
            }
            fieldDefinitions {
              name
              key
              description
              required
              type {
                name
                category
              }
              validations {
                name
                value
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

  return fetchAllGraphQLPages(query, ['metaobjectDefinitions']);
}

async function fetchShopMetafields() {
  log('🏷️  Fetching shop-level metafields...');
  const query = `
    query ShopMetafields($cursor: String) {
      shop {
        metafields(first: 100, after: $cursor) {
          edges {
            cursor
            node {
              id
              namespace
              key
              value
              type
              ownerType
              updatedAt
            }
          }
          pageInfo {
            hasNextPage
          }
        }
      }
    }
  `;

  // For shop metafields, the connection is nested under shop
  const allNodes = [];
  let cursor = null;
  let hasNextPage = true;

  while (hasNextPage) {
    const data = await graphqlRequest(query, { cursor });
    const connection = data.shop.metafields;
    const edges = connection.edges || [];

    for (const edge of edges) {
      allNodes.push(edge.node);
    }

    hasNextPage = connection.pageInfo?.hasNextPage ?? false;
    if (edges.length > 0) {
      cursor = edges[edges.length - 1].cursor;
    } else {
      hasNextPage = false;
    }
  }

  return allNodes;
}

// ---------------------------------------------------------------------------
// Main backup orchestration
// ---------------------------------------------------------------------------

async function runBackup() {
  const startTime = Date.now();
  log('🚀 Starting Shopify backup...');
  log(`   Store: ${STORE}`);
  log(`   API Version: ${API_VERSION}`);
  log('');

  // Open the output file up front and stream every section into it, so the
  // whole store is never resident in memory at once.
  if (!existsSync(OUTPUT_DIR)) {
    mkdirSync(OUTPUT_DIR, { recursive: true });
  }

  const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const storeName = STORE.replace('.myshopify.com', '');
  const filename = `shopify-backup_${storeName}_${timestamp}.json`;
  const filepath = join(OUTPUT_DIR, filename);

  const fileStream = createWriteStream(filepath, { encoding: 'utf-8' });
  fileStream.on('error', (err) => {
    log(`❌ Failed writing backup file: ${err.message}`);
    process.exit(1);
  });

  const w = new JsonStreamWriter(fileStream);
  await w.beginObject(); // root document

  // ---- Shop settings & configuration ----
  await w.key('shop');
  await w.value(await fetchShop());
  await w.key('policies');
  await w.value(await fetchPolicies());
  await w.key('shipping_zones');
  await w.value(await fetchShippingZones());
  await w.key('countries');
  await w.value(await fetchCountries());

  // ---- Products ----
  // Stream the full product objects to disk while capturing only the ids and
  // variant inventory ids needed for the dependent fetches below.
  log('🛍️  Fetching products...');
  const { count: productCount, captured: productIndex } = await writeArraySection(
    w,
    'products',
    streamPages('/products.json', 'products'),
    (p) => ({
      id: p.id,
      variant_ids: (p.variants || []).map((v) => v.id),
      inventory_item_ids: (p.variants || [])
        .map((v) => v.inventory_item_id)
        .filter(Boolean),
    })
  );
  log(`   → ${productCount} products`);

  const productIds = productIndex.map((p) => p.id);
  const variantIds = productIndex.flatMap((p) => p.variant_ids);
  const inventoryItemIds = productIndex.flatMap((p) => p.inventory_item_ids);

  log(`🏷️  Fetching metafields for ${productIds.length} products...`);
  await writeMetafieldMap(w, 'product_metafields', productIds, '/products', 'products');

  log(`🏷️  Fetching metafields for ${variantIds.length} variants...`);
  await writeMetafieldMap(w, 'variant_metafields', variantIds, '/variants', 'variants');

  // ---- Inventory ----
  const locations = await fetchLocations();
  log(`   → ${locations.length} locations`);

  log(`📦 Fetching inventory levels for ${locations.length} locations...`);
  await w.key('inventory_levels');
  await w.beginArray();
  let levelCount = 0;
  for (const location of locations) {
    let locLevels = 0;
    for await (const level of streamPages(
      `/locations/${location.id}/inventory_levels.json`,
      'inventory_levels'
    )) {
      // Tag each level with the location name for readability
      level._location_name = location.name;
      await w.value(level);
      levelCount++;
      locLevels++;
    }
    log(`  📍 ${location.name}: ${locLevels} inventory levels`);
  }
  await w.endArray();
  log(`   → ${levelCount} inventory levels`);

  log('📦 Fetching inventory item details...');
  await w.key('inventory_items');
  await w.beginArray();
  let inventoryItemCount = 0;
  // Fetch in batches of 100 (Shopify limit for the ids parameter)
  for (let i = 0; i < inventoryItemIds.length; i += 100) {
    const batch = inventoryItemIds.slice(i, i + 100);
    const response = await shopifyFetch(
      `${BASE_URL}/inventory_items.json?ids=${batch.join(',')}&limit=100`
    );
    const data = await response.json();
    for (const item of data.inventory_items || []) {
      await w.value(item);
      inventoryItemCount++;
    }
    if ((i + 100) % 500 === 0 || i + 100 >= inventoryItemIds.length) {
      log(`  ...fetched ${Math.min(i + 100, inventoryItemIds.length)}/${inventoryItemIds.length} inventory items`);
    }
  }
  await w.endArray();
  log(`   → ${inventoryItemCount} inventory items`);

  // ---- Collections ----
  log('📂 Fetching custom collections...');
  const { count: customColCount, captured: customColIds } = await writeArraySection(
    w,
    'custom_collections',
    streamPages('/custom_collections.json', 'custom_collections'),
    (c) => c.id
  );
  log(`   → ${customColCount} custom collections`);

  log('📂 Fetching smart collections...');
  const { count: smartColCount, captured: smartColIds } = await writeArraySection(
    w,
    'smart_collections',
    streamPages('/smart_collections.json', 'smart_collections'),
    (c) => c.id
  );
  log(`   → ${smartColCount} smart collections`);

  log('🏷️  Fetching collection metafields...');
  await writeMetafieldMap(w, 'custom_collection_metafields', customColIds, '/collections', 'custom collections');
  await writeMetafieldMap(w, 'smart_collection_metafields', smartColIds, '/collections', 'smart collections');

  log('🔗 Fetching collects (product-collection mappings)...');
  const { count: collectCount } = await writeArraySection(
    w,
    'collects',
    streamPages('/collects.json', 'collects')
  );
  log(`   → ${collectCount} collects`);

  // ---- Customers (GDPR-sensitive) ----
  log('👤 Fetching customers...');
  const { count: customerCount, captured: customerIds } = await writeArraySection(
    w,
    'customers',
    streamPages('/customers.json', 'customers'),
    (c) => c.id
  );
  log(`   → ${customerCount} customers`);

  log(`🏷️  Fetching metafields for ${customerIds.length} customers...`);
  await writeMetafieldMap(w, 'customer_metafields', customerIds, '/customers', 'customers');

  // ---- Orders ----
  log('🧾 Fetching orders...');
  const { count: orderCount } = await writeArraySection(
    w,
    'orders',
    streamPages('/orders.json', 'orders', { status: 'any' })
  );
  log(`   → ${orderCount} orders`);

  log('📝 Fetching draft orders...');
  const { count: draftOrderCount } = await writeArraySection(
    w,
    'draft_orders',
    streamPages('/draft_orders.json', 'draft_orders')
  );
  log(`   → ${draftOrderCount} draft orders`);

  // ---- Content ----
  log('📄 Fetching pages...');
  const { count: pageCount, captured: pageIds } = await writeArraySection(
    w,
    'pages',
    streamPages('/pages.json', 'pages'),
    (p) => p.id
  );
  log(`   → ${pageCount} pages`);

  log(`🏷️  Fetching metafields for ${pageIds.length} pages...`);
  await writeMetafieldMap(w, 'page_metafields', pageIds, '/pages', 'pages');

  log('📰 Fetching blogs...');
  const { count: blogCount, captured: blogIds } = await writeArraySection(
    w,
    'blogs',
    streamPages('/blogs.json', 'blogs'),
    (b) => b.id
  );
  log(`   → ${blogCount} blogs`);

  log(`📰 Fetching articles for ${blogIds.length} blogs...`);
  await w.key('articles');
  await w.beginObject();
  let articleCount = 0;
  for (const blogId of blogIds) {
    await w.key(blogId);
    await w.beginArray();
    for await (const article of streamPages(`/blogs/${blogId}/articles.json`, 'articles')) {
      await w.value(article);
      articleCount++;
    }
    await w.endArray();
  }
  await w.endObject();
  log(`   → ${articleCount} articles`);

  // ---- Themes ----
  // Theme assets carry their full source inline and are usually the largest
  // section, so stream each asset individually rather than buffering a theme.
  const themes = await fetchThemes();
  log(`🎨 Fetching theme assets for ${themes.length} themes...`);
  await w.key('themes');
  await w.beginObject();
  for (const theme of themes) {
    const response = await shopifyFetch(`${BASE_URL}/themes/${theme.id}/assets.json`);
    const data = await response.json();
    const assetList = data.assets || [];
    log(`  🎨 Theme "${theme.name}" (${theme.role}): ${assetList.length} assets`);

    await w.key(theme.id);
    await w.beginObject();
    await w.key('theme_info');
    await w.value(theme);
    await w.key('assets');
    await w.beginArray();
    let count = 0;
    for (const asset of assetList) {
      try {
        const assetResponse = await shopifyFetch(
          `${BASE_URL}/themes/${theme.id}/assets.json?asset[key]=${encodeURIComponent(asset.key)}`
        );
        const assetData = await assetResponse.json();
        await w.value(assetData.asset);
      } catch (err) {
        log(`    ⚠️  Failed to fetch asset ${asset.key}: ${err.message}`);
        await w.value({ ...asset, _error: err.message });
      }
      count++;
      if (count % 50 === 0) {
        log(`    ...fetched ${count}/${assetList.length} assets`);
      }
    }
    await w.endArray();
    await w.endObject();
  }
  await w.endObject();
  log(`   → ${themes.length} themes backed up`);

  // ---- Redirects, scripts, discounts, gift cards ----
  log('↩️  Fetching redirects...');
  const { count: redirectCount } = await writeArraySection(
    w,
    'redirects',
    streamPages('/redirects.json', 'redirects')
  );
  log(`   → ${redirectCount} redirects`);

  log('📜 Fetching script tags...');
  const { count: scriptTagCount } = await writeArraySection(
    w,
    'script_tags',
    streamPages('/script_tags.json', 'script_tags')
  );
  log(`   → ${scriptTagCount} script tags`);

  log('💰 Fetching price rules & discount codes...');
  await w.key('price_rules_and_discounts');
  await w.beginArray();
  let priceRuleCount = 0;
  for await (const rule of streamPages('/price_rules.json', 'price_rules')) {
    const codes = await fetchAllPages(
      `/price_rules/${rule.id}/discount_codes.json`,
      'discount_codes'
    );
    await w.value({ ...rule, discount_codes: codes });
    priceRuleCount++;
  }
  await w.endArray();
  log(`   → ${priceRuleCount} price rules`);

  await w.key('gift_cards');
  await w.value(await fetchGiftCards());

  // ---- Metafield definitions ----
  const metafieldDefinitions = await fetchMetafieldDefinitions();
  await w.key('metafield_definitions');
  await w.value(metafieldDefinitions);
  log(`   → metafield definitions for ${Object.keys(metafieldDefinitions).length} owner types`);

  // ---- Metaobjects ----
  const moDefinitions = await fetchMetaobjectDefinitions();
  await w.key('metaobject_definitions');
  await w.value(moDefinitions);
  log(`   → ${moDefinitions.length} metaobject definitions`);

  log(`📦 Fetching metaobjects for ${moDefinitions.length} types...`);
  const metaobjectQuery = `
    query Metaobjects($type: String!, $cursor: String) {
      metaobjects(first: 50, type: $type, after: $cursor) {
        edges {
          cursor
          node {
            id
            handle
            type
            displayName
            updatedAt
            fields {
              key
              value
              type
            }
          }
        }
        pageInfo {
          hasNextPage
        }
      }
    }
  `;
  await w.key('metaobjects');
  await w.beginObject();
  let metaobjectTypeCount = 0;
  for (const def of moDefinitions) {
    let opened = false;
    let typeCount = 0;
    try {
      for await (const node of streamGraphQLPages(metaobjectQuery, ['metaobjects'], {
        type: def.type,
      })) {
        if (!opened) {
          await w.key(def.type);
          await w.beginArray();
          opened = true;
        }
        await w.value(node);
        typeCount++;
      }
    } catch (err) {
      log(`  ⚠️  Could not fetch metaobjects for type "${def.type}": ${err.message}`);
    }
    if (opened) {
      await w.endArray();
      metaobjectTypeCount++;
      log(`  📦 ${def.type}: ${typeCount} metaobjects`);
    }
  }
  await w.endObject();
  log(`   → metaobjects for ${metaobjectTypeCount} types`);

  // ---- Shop-level metafields ----
  const shopMetafields = await fetchShopMetafields();
  await w.key('shop_metafields');
  await w.value(shopMetafields);
  log(`   → ${shopMetafields.length} shop metafields`);

  // ---- Finalize ----
  // _meta is written last so it can record the duration and completion time.
  // JSON key order is irrelevant to the restore script (it parses the whole
  // object), so this stays fully compatible.
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  await w.key('_meta');
  await w.value({
    version: '1.0.0',
    store: STORE,
    api_version: API_VERSION,
    created_at: new Date(startTime).toISOString(),
    created_by: 'shopify-backup.mjs',
    gdpr_notice:
      'This backup contains personal data (customers, orders). Handle in accordance with GDPR / AVG. Apply appropriate retention policies and access controls.',
    duration_seconds: parseFloat(elapsed),
    completed_at: new Date().toISOString(),
  });

  await w.endObject(); // close root document

  // Flush and close the file before reporting the path to n8n.
  fileStream.end();
  await new Promise((resolve, reject) => {
    fileStream.on('finish', resolve);
    fileStream.on('error', reject);
  });

  log('');
  log(`✅ Backup complete in ${elapsed}s`);
  log(`📁 File: ${filepath}`);
  log(`📊 Size: ${(w.bytes / 1024 / 1024).toFixed(2)} MB`);

  // Print ONLY the filepath to stdout so n8n can capture it
  console.log(filepath);
}

// ---------------------------------------------------------------------------
// Authentication
// ---------------------------------------------------------------------------

/**
 * Resolve the Admin API access token.
 *
 * If SHOPIFY_ACCESS_TOKEN is set (legacy admin-managed custom apps), it is used
 * directly. Otherwise the Client ID / Client Secret from a Dev Dashboard app
 * are exchanged for a short-lived (~24h) token via the client credentials
 * grant. A single backup run fits comfortably inside the token lifetime.
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

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

resolveToken()
  .then(runBackup)
  .catch((err) => {
    log(`❌ Backup failed: ${err.message}`);
    log(err.stack);
    process.exit(1);
  });
