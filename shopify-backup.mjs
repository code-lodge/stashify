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
 *   SHOPIFY_STORE       – mystore.myshopify.com (without https://)
 *   SHOPIFY_ACCESS_TOKEN – Admin API access token from your custom app
 *   SHOPIFY_API_VERSION  – API version (default: 2025-01)
 *   BACKUP_OUTPUT_DIR    – Output directory (default: /tmp/shopify-backups)
 *
 * Output:
 *   A timestamped JSON file containing all store data.
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

import { writeFileSync, mkdirSync, existsSync } from 'node:fs';
import { join } from 'node:path';

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const STORE = process.env.SHOPIFY_STORE;
const TOKEN = process.env.SHOPIFY_ACCESS_TOKEN;
const API_VERSION = process.env.SHOPIFY_API_VERSION || '2025-01';
const OUTPUT_DIR = process.env.BACKUP_OUTPUT_DIR || '/tmp/shopify-backups';

if (!STORE || !TOKEN) {
  console.error(
    'ERROR: SHOPIFY_STORE and SHOPIFY_ACCESS_TOKEN environment variables are required.'
  );
  process.exit(1);
}

const BASE_URL = `https://${STORE}/admin/api/${API_VERSION}`;
const GQL_URL = `https://${STORE}/admin/api/${API_VERSION}/graphql.json`;

// Shopify REST rate limit: bucket of 40, leak rate 2/s.
// We throttle to ~1.8 req/s to stay safely under the limit.
const MIN_REQUEST_INTERVAL_MS = 560;
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
  const query = new URLSearchParams({ limit: '250', ...params });
  let url = `${BASE_URL}${endpoint}?${query}`;

  while (url) {
    const response = await shopifyFetch(url);
    const data = await response.json();
    const items = data[rootKey] || [];
    allItems.push(...items);

    // Parse Link header for next page URL
    url = getNextPageUrl(response.headers.get('link'));
  }

  return allItems;
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
  let cursor = null;
  let hasNextPage = true;

  while (hasNextPage) {
    const variables = { ...extraVars, cursor };
    const data = await graphqlRequest(queryTemplate, variables);

    // Navigate to the connection in the response
    let connection = data;
    for (const key of dataPath) {
      connection = connection[key];
    }

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
// Logging
// ---------------------------------------------------------------------------

function log(msg) {
  // Write to stderr so stdout stays clean for n8n to parse the output path
  process.stderr.write(`[${new Date().toISOString()}] ${msg}\n`);
}

// ---------------------------------------------------------------------------
// Resource fetchers
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

async function fetchProducts() {
  log('🛍️  Fetching products...');
  return fetchAllPages('/products.json', 'products');
}

async function fetchProductMetafields(products) {
  log(`🏷️  Fetching metafields for ${products.length} products...`);
  const result = {};
  let count = 0;

  for (const product of products) {
    const metafields = await fetchAllPages(
      `/products/${product.id}/metafields.json`,
      'metafields'
    );
    if (metafields.length > 0) {
      result[product.id] = metafields;
    }
    count++;
    if (count % 50 === 0) {
      log(`  ...processed ${count}/${products.length} products`);
    }
  }

  return result;
}

async function fetchVariantMetafields(products) {
  log(`🏷️  Fetching variant metafields...`);
  const result = {};
  let count = 0;
  const totalVariants = products.reduce((sum, p) => sum + (p.variants?.length || 0), 0);

  for (const product of products) {
    for (const variant of product.variants || []) {
      const metafields = await fetchAllPages(
        `/variants/${variant.id}/metafields.json`,
        'metafields'
      );
      if (metafields.length > 0) {
        result[variant.id] = metafields;
      }
      count++;
      if (count % 100 === 0) {
        log(`  ...processed ${count}/${totalVariants} variants`);
      }
    }
  }

  return result;
}

async function fetchCustomCollections() {
  log('📂 Fetching custom collections...');
  return fetchAllPages('/custom_collections.json', 'custom_collections');
}

async function fetchSmartCollections() {
  log('📂 Fetching smart collections...');
  return fetchAllPages('/smart_collections.json', 'smart_collections');
}

async function fetchCollectionMetafields(collections, type) {
  log(`🏷️  Fetching metafields for ${collections.length} ${type} collections...`);
  const result = {};

  for (const col of collections) {
    const metafields = await fetchAllPages(
      `/collections/${col.id}/metafields.json`,
      'metafields'
    );
    if (metafields.length > 0) {
      result[col.id] = metafields;
    }
  }

  return result;
}

async function fetchCollects() {
  log('🔗 Fetching collects (product-collection mappings)...');
  return fetchAllPages('/collects.json', 'collects');
}

async function fetchCustomers() {
  log('👤 Fetching customers...');
  return fetchAllPages('/customers.json', 'customers');
}

async function fetchCustomerMetafields(customers) {
  log(`🏷️  Fetching metafields for ${customers.length} customers...`);
  const result = {};
  let count = 0;

  for (const customer of customers) {
    const metafields = await fetchAllPages(
      `/customers/${customer.id}/metafields.json`,
      'metafields'
    );
    if (metafields.length > 0) {
      result[customer.id] = metafields;
    }
    count++;
    if (count % 100 === 0) {
      log(`  ...processed ${count}/${customers.length} customers`);
    }
  }

  return result;
}

async function fetchOrders() {
  log('🧾 Fetching orders...');
  return fetchAllPages('/orders.json', 'orders', { status: 'any' });
}

async function fetchDraftOrders() {
  log('📝 Fetching draft orders...');
  return fetchAllPages('/draft_orders.json', 'draft_orders');
}

async function fetchPages() {
  log('📄 Fetching pages...');
  return fetchAllPages('/pages.json', 'pages');
}

async function fetchPageMetafields(pages) {
  log(`🏷️  Fetching metafields for ${pages.length} pages...`);
  const result = {};

  for (const page of pages) {
    const metafields = await fetchAllPages(
      `/pages/${page.id}/metafields.json`,
      'metafields'
    );
    if (metafields.length > 0) {
      result[page.id] = metafields;
    }
  }

  return result;
}

async function fetchBlogs() {
  log('📰 Fetching blogs...');
  return fetchAllPages('/blogs.json', 'blogs');
}

async function fetchArticles(blogs) {
  log(`📰 Fetching articles for ${blogs.length} blogs...`);
  const result = {};

  for (const blog of blogs) {
    const articles = await fetchAllPages(
      `/blogs/${blog.id}/articles.json`,
      'articles'
    );
    result[blog.id] = articles;
  }

  return result;
}

async function fetchLocations() {
  log('📍 Fetching locations...');
  const response = await shopifyFetch(`${BASE_URL}/locations.json`);
  const data = await response.json();
  return data.locations || [];
}

async function fetchInventoryLevels(locations) {
  log(`📦 Fetching inventory levels for ${locations.length} locations...`);
  const allLevels = [];

  for (const location of locations) {
    const levels = await fetchAllPages(
      `/locations/${location.id}/inventory_levels.json`,
      'inventory_levels'
    );
    // Tag each level with the location name for readability
    for (const level of levels) {
      level._location_name = location.name;
    }
    allLevels.push(...levels);
    log(`  📍 ${location.name}: ${levels.length} inventory levels`);
  }

  return allLevels;
}

async function fetchInventoryItems(products) {
  log('📦 Fetching inventory item details...');
  const allItems = [];

  // Collect all inventory_item_ids from product variants
  const inventoryItemIds = [];
  for (const product of products) {
    for (const variant of product.variants || []) {
      if (variant.inventory_item_id) {
        inventoryItemIds.push(variant.inventory_item_id);
      }
    }
  }

  // Fetch in batches of 100 (Shopify limit for ids parameter)
  for (let i = 0; i < inventoryItemIds.length; i += 100) {
    const batch = inventoryItemIds.slice(i, i + 100);
    const response = await shopifyFetch(
      `${BASE_URL}/inventory_items.json?ids=${batch.join(',')}&limit=100`
    );
    const data = await response.json();
    allItems.push(...(data.inventory_items || []));

    if ((i + 100) % 500 === 0 || i + 100 >= inventoryItemIds.length) {
      log(`  ...fetched ${Math.min(i + 100, inventoryItemIds.length)}/${inventoryItemIds.length} inventory items`);
    }
  }

  return allItems;
}

async function fetchRedirects() {
  log('↩️  Fetching redirects...');
  return fetchAllPages('/redirects.json', 'redirects');
}

async function fetchScriptTags() {
  log('📜 Fetching script tags...');
  return fetchAllPages('/script_tags.json', 'script_tags');
}

async function fetchThemes() {
  log('🎨 Fetching themes...');
  const response = await shopifyFetch(`${BASE_URL}/themes.json`);
  const data = await response.json();
  return data.themes || [];
}

async function fetchThemeAssets(themes) {
  log(`🎨 Fetching theme assets for ${themes.length} themes...`);
  const result = {};

  for (const theme of themes) {
    const response = await shopifyFetch(
      `${BASE_URL}/themes/${theme.id}/assets.json`
    );
    const data = await response.json();
    const assetList = data.assets || [];

    // Fetch each asset's content
    const assetsWithContent = [];
    log(`  🎨 Theme "${theme.name}" (${theme.role}): ${assetList.length} assets`);
    let count = 0;

    for (const asset of assetList) {
      try {
        const assetResponse = await shopifyFetch(
          `${BASE_URL}/themes/${theme.id}/assets.json?asset[key]=${encodeURIComponent(asset.key)}`
        );
        const assetData = await assetResponse.json();
        assetsWithContent.push(assetData.asset);
      } catch (err) {
        log(`    ⚠️  Failed to fetch asset ${asset.key}: ${err.message}`);
        assetsWithContent.push({ ...asset, _error: err.message });
      }
      count++;
      if (count % 50 === 0) {
        log(`    ...fetched ${count}/${assetList.length} assets`);
      }
    }

    result[theme.id] = {
      theme_info: theme,
      assets: assetsWithContent,
    };
  }

  return result;
}

async function fetchDiscountCodes() {
  log('💰 Fetching price rules & discount codes...');
  const priceRules = await fetchAllPages('/price_rules.json', 'price_rules');
  const result = [];

  for (const rule of priceRules) {
    const codes = await fetchAllPages(
      `/price_rules/${rule.id}/discount_codes.json`,
      'discount_codes'
    );
    result.push({ ...rule, discount_codes: codes });
  }

  return result;
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

async function fetchMetaobjects(definitions) {
  log(`📦 Fetching metaobjects for ${definitions.length} types...`);
  const allMetaobjects = {};

  const query = `
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

  for (const def of definitions) {
    try {
      const nodes = await fetchAllGraphQLPages(query, ['metaobjects'], {
        type: def.type,
      });
      if (nodes.length > 0) {
        allMetaobjects[def.type] = nodes;
        log(`  📦 ${def.type}: ${nodes.length} metaobjects`);
      }
    } catch (err) {
      log(`  ⚠️  Could not fetch metaobjects for type "${def.type}": ${err.message}`);
    }
  }

  return allMetaobjects;
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

// ---------------------------------------------------------------------------
// Main backup orchestration
// ---------------------------------------------------------------------------

async function runBackup() {
  const startTime = Date.now();
  log('🚀 Starting Shopify backup...');
  log(`   Store: ${STORE}`);
  log(`   API Version: ${API_VERSION}`);
  log('');

  const backup = {
    _meta: {
      version: '1.0.0',
      store: STORE,
      api_version: API_VERSION,
      created_at: new Date().toISOString(),
      created_by: 'shopify-backup.mjs',
      gdpr_notice:
        'This backup contains personal data (customers, orders). Handle in accordance with GDPR / AVG. Apply appropriate retention policies and access controls.',
    },
  };

  // ---- Shop settings & configuration ----
  backup.shop = await fetchShop();
  backup.policies = await fetchPolicies();
  backup.shipping_zones = await fetchShippingZones();
  backup.countries = await fetchCountries();

  // ---- Products ----
  backup.products = await fetchProducts();
  log(`   → ${backup.products.length} products`);

  backup.product_metafields = await fetchProductMetafields(backup.products);
  log(`   → metafields for ${Object.keys(backup.product_metafields).length} products`);

  backup.variant_metafields = await fetchVariantMetafields(backup.products);
  log(`   → metafields for ${Object.keys(backup.variant_metafields).length} variants`);

  // ---- Inventory ----
  backup.locations = await fetchLocations();
  log(`   → ${backup.locations.length} locations`);

  backup.inventory_levels = await fetchInventoryLevels(backup.locations);
  log(`   → ${backup.inventory_levels.length} inventory levels`);

  backup.inventory_items = await fetchInventoryItems(backup.products);
  log(`   → ${backup.inventory_items.length} inventory items`);

  // ---- Collections ----
  backup.custom_collections = await fetchCustomCollections();
  log(`   → ${backup.custom_collections.length} custom collections`);

  backup.smart_collections = await fetchSmartCollections();
  log(`   → ${backup.smart_collections.length} smart collections`);

  backup.custom_collection_metafields = await fetchCollectionMetafields(
    backup.custom_collections,
    'custom'
  );
  backup.smart_collection_metafields = await fetchCollectionMetafields(
    backup.smart_collections,
    'smart'
  );

  backup.collects = await fetchCollects();
  log(`   → ${backup.collects.length} collects`);

  // ---- Customers (GDPR-sensitive) ----
  backup.customers = await fetchCustomers();
  log(`   → ${backup.customers.length} customers`);

  backup.customer_metafields = await fetchCustomerMetafields(backup.customers);

  // ---- Orders ----
  backup.orders = await fetchOrders();
  log(`   → ${backup.orders.length} orders`);

  backup.draft_orders = await fetchDraftOrders();
  log(`   → ${backup.draft_orders.length} draft orders`);

  // ---- Content ----
  backup.pages = await fetchPages();
  log(`   → ${backup.pages.length} pages`);

  backup.page_metafields = await fetchPageMetafields(backup.pages);

  backup.blogs = await fetchBlogs();
  log(`   → ${backup.blogs.length} blogs`);

  backup.articles = await fetchArticles(backup.blogs);
  const totalArticles = Object.values(backup.articles).reduce(
    (sum, arr) => sum + arr.length,
    0
  );
  log(`   → ${totalArticles} articles`);

  // ---- Themes ----
  backup.themes = await fetchThemeAssets(await fetchThemes());
  log(`   → ${Object.keys(backup.themes).length} themes backed up`);

  // ---- Redirects, scripts, discounts, gift cards ----
  backup.redirects = await fetchRedirects();
  log(`   → ${backup.redirects.length} redirects`);

  backup.script_tags = await fetchScriptTags();
  log(`   → ${backup.script_tags.length} script tags`);

  backup.price_rules_and_discounts = await fetchDiscountCodes();
  log(`   → ${backup.price_rules_and_discounts.length} price rules`);

  backup.gift_cards = await fetchGiftCards();
  log(`   → ${backup.gift_cards.length} gift cards`);

  // ---- Metafield definitions ----
  backup.metafield_definitions = await fetchMetafieldDefinitions();
  log(
    `   → metafield definitions for ${Object.keys(backup.metafield_definitions).length} owner types`
  );

  // ---- Metaobjects ----
  const moDefinitions = await fetchMetaobjectDefinitions();
  backup.metaobject_definitions = moDefinitions;
  log(`   → ${moDefinitions.length} metaobject definitions`);

  backup.metaobjects = await fetchMetaobjects(moDefinitions);
  log(`   → metaobjects for ${Object.keys(backup.metaobjects).length} types`);

  // ---- Shop-level metafields ----
  backup.shop_metafields = await fetchShopMetafields();
  log(`   → ${backup.shop_metafields.length} shop metafields`);

  // ---- Finalize ----
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  backup._meta.duration_seconds = parseFloat(elapsed);
  backup._meta.completed_at = new Date().toISOString();

  // Write backup file
  if (!existsSync(OUTPUT_DIR)) {
    mkdirSync(OUTPUT_DIR, { recursive: true });
  }

  const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const storeName = STORE.replace('.myshopify.com', '');
  const filename = `shopify-backup_${storeName}_${timestamp}.json`;
  const filepath = join(OUTPUT_DIR, filename);

  writeFileSync(filepath, JSON.stringify(backup, null, 2), 'utf-8');

  log('');
  log(`✅ Backup complete in ${elapsed}s`);
  log(`📁 File: ${filepath}`);
  log(`📊 Size: ${(Buffer.byteLength(JSON.stringify(backup)) / 1024 / 1024).toFixed(2)} MB`);

  // Print ONLY the filepath to stdout so n8n can capture it
  console.log(filepath);
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

runBackup().catch((err) => {
  log(`❌ Backup failed: ${err.message}`);
  log(err.stack);
  process.exit(1);
});
