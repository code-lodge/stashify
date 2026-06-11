#!/usr/bin/env node

/**
 * Shopify Store Restore Script
 *
 * Restores Shopify store data from a backup JSON file created by shopify-backup.mjs.
 * Supports selective restore by resource type.
 *
 * Usage:
 *   node shopify-restore.mjs --file /path/to/backup.json [--resources products,inventory,collections,...]
 *
 * Environment variables:
 *   SHOPIFY_STORE        – mystore.myshopify.com (without https://)
 *   SHOPIFY_ACCESS_TOKEN – Admin API access token (legacy admin-managed custom
 *                          apps). Optional if CLIENT_ID/SECRET are set.
 *   SHOPIFY_CLIENT_ID    – Dev Dashboard app Client ID (Klant-ID).
 *   SHOPIFY_CLIENT_SECRET – Dev Dashboard app Client Secret (Geheim).
 *   SHOPIFY_API_VERSION  – API version (default: 2025-01)
 *   SHOPIFY_MIN_REQUEST_INTERVAL_MS – Min delay between API requests in ms
 *                          (default: 560). Raise to ~N × 560 when running N
 *                          jobs concurrently against the same token.
 *
 * Arguments:
 *   --file <path>        – Path to backup JSON file (required)
 *   --resources <list>   – Comma-separated list of resources to restore (default: all restorable)
 *   --dry-run            – Preview what would be restored without making changes
 *   --skip-existing      – Skip resources that already exist (don't update)
 *
 * Restorable resources:
 *   products, product_metafields, variant_metafields, inventory,
 *   custom_collections, smart_collections, collection_metafields,
 *   customers, customer_metafields, pages, page_metafields,
 *   blogs, articles, redirects, theme_assets,
 *   metafield_definitions, metaobject_definitions, metaobjects, shop_metafields,
 *   price_rules, script_tags
 *
 * NOT restorable via API:
 *   orders (read-only historical data), draft_orders (complex state),
 *   shop settings (mostly read-only), policies (managed in admin),
 *   shipping_zones (complex nested config), gift_cards (security restrictions)
 *
 * @version 1.0.0
 * @license GPL-3.0-or-later
 */

import { readFileSync } from 'node:fs';

// ---------------------------------------------------------------------------
// Argument parsing
// ---------------------------------------------------------------------------

const args = process.argv.slice(2);
const flags = {};
for (let i = 0; i < args.length; i++) {
  if (args[i] === '--file' && args[i + 1]) {
    flags.file = args[++i];
  } else if (args[i] === '--resources' && args[i + 1]) {
    flags.resources = args[++i].split(',').map((s) => s.trim());
  } else if (args[i] === '--dry-run') {
    flags.dryRun = true;
  } else if (args[i] === '--skip-existing') {
    flags.skipExisting = true;
  }
}

if (!flags.file) {
  console.error('ERROR: --file argument is required.');
  console.error(
    'Usage: node shopify-restore.mjs --file /path/to/backup.json [--resources products,inventory] [--dry-run]'
  );
  process.exit(1);
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const STORE = process.env.SHOPIFY_STORE;
let TOKEN = process.env.SHOPIFY_ACCESS_TOKEN;
const CLIENT_ID = process.env.SHOPIFY_CLIENT_ID;
const CLIENT_SECRET = process.env.SHOPIFY_CLIENT_SECRET;
const API_VERSION = process.env.SHOPIFY_API_VERSION || '2025-01';

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
const DRY_RUN = flags.dryRun ?? false;

const ALL_RESTORABLE = [
  'products',
  'product_metafields',
  'variant_metafields',
  'inventory',
  'custom_collections',
  'smart_collections',
  'collection_metafields',
  'customers',
  'customer_metafields',
  'pages',
  'page_metafields',
  'blogs',
  'articles',
  'redirects',
  'theme_assets',
  'metafield_definitions',
  'metaobject_definitions',
  'metaobjects',
  'shop_metafields',
  'price_rules',
  'script_tags',
];

const resourcesToRestore = flags.resources || ALL_RESTORABLE;

// Validate resource names
for (const r of resourcesToRestore) {
  if (!ALL_RESTORABLE.includes(r)) {
    console.error(`ERROR: Unknown resource "${r}".`);
    console.error(`Valid resources: ${ALL_RESTORABLE.join(', ')}`);
    process.exit(1);
  }
}

// ---------------------------------------------------------------------------
// Rate limiting & HTTP helpers (same as backup script)
// ---------------------------------------------------------------------------

// Per store/access token, shared across all processes. Override via
// SHOPIFY_MIN_REQUEST_INTERVAL_MS when running alongside other backup/restore
// jobs on the same token (use ~N × 560 ms for N concurrent jobs).
const MIN_REQUEST_INTERVAL_MS = Math.max(
  0,
  parseInt(process.env.SHOPIFY_MIN_REQUEST_INTERVAL_MS || '560', 10) || 560
);
let lastRequestTime = 0;

async function throttle() {
  const now = Date.now();
  const elapsed = now - lastRequestTime;
  if (elapsed < MIN_REQUEST_INTERVAL_MS) {
    await new Promise((r) => setTimeout(r, MIN_REQUEST_INTERVAL_MS - elapsed));
  }
  lastRequestTime = Date.now();
}

const FETCH_RETRIES = 5;

async function shopifyFetch(url, options = {}, retries = FETCH_RETRIES) {
  await throttle();

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
      return shopifyFetch(url, options, retries - 1);
    }
    throw new Error(`Network failure after retries: ${err.message} (${url})`);
  }

  if (response.status === 429 || response.status >= 500) {
    if (retries > 0) {
      const retryAfter =
        Math.max(1000, (parseFloat(response.headers.get('Retry-After')) || 4) * 1000) +
        Math.floor(Math.random() * 1000);
      log(
        `  ⏳ Rate limited (${response.status}), retrying in ${retryAfter}ms...`
      );
      await new Promise((r) => setTimeout(r, retryAfter));
      return shopifyFetch(url, options, retries - 1);
    }
    throw new Error(
      `Shopify API error ${response.status} after retries: ${url}`
    );
  }

  return response;
}

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

function log(msg) {
  process.stderr.write(`[${new Date().toISOString()}] ${msg}\n`);
}

// ---------------------------------------------------------------------------
// Restore statistics
// ---------------------------------------------------------------------------

const stats = {
  created: 0,
  updated: 0,
  skipped: 0,
  failed: 0,
  errors: [],
};

function recordError(resource, id, error) {
  stats.failed++;
  const msg = `${resource} #${id}: ${error.message || error}`;
  stats.errors.push(msg);
  log(`  ❌ ${msg}`);
}

// ---------------------------------------------------------------------------
// REST restore helpers
// ---------------------------------------------------------------------------

/**
 * Create or update a REST resource.
 *
 * Attempts to GET the resource by ID first. If it exists, PUTs to update.
 * If it doesn't exist, POSTs to create.
 *
 * @param {string} resourcePath – e.g. '/products'
 * @param {string} singularKey – e.g. 'product'
 * @param {object} item – The backup item data
 * @param {string[]} [excludeFields] – Fields to strip before sending
 * @returns {Promise<object|null>} – The created/updated resource or null
 */
async function upsertResource(resourcePath, singularKey, item, excludeFields = []) {
  if (DRY_RUN) {
    log(`  [DRY RUN] Would upsert ${singularKey} #${item.id}`);
    stats.skipped++;
    return null;
  }

  // Clean the item: remove read-only or server-generated fields
  const cleanItem = { ...item };
  const alwaysExclude = [
    'admin_graphql_api_id',
    'created_at',
    'updated_at',
    'published_at',
  ];
  for (const field of [...alwaysExclude, ...excludeFields]) {
    delete cleanItem[field];
  }

  // Try to check if exists
  try {
    const checkResponse = await shopifyFetch(
      `${BASE_URL}${resourcePath}/${item.id}.json`
    );

    if (checkResponse.ok) {
      if (flags.skipExisting) {
        stats.skipped++;
        return null;
      }

      // Update existing
      const updateResponse = await shopifyFetch(
        `${BASE_URL}${resourcePath}/${item.id}.json`,
        {
          method: 'PUT',
          body: JSON.stringify({ [singularKey]: cleanItem }),
        }
      );

      if (updateResponse.ok) {
        stats.updated++;
        const data = await updateResponse.json();
        return data[singularKey];
      } else {
        const errBody = await updateResponse.text();
        throw new Error(`PUT ${updateResponse.status}: ${errBody}`);
      }
    }
  } catch (err) {
    // Resource doesn't exist or error checking — try to create
    if (err.message && !err.message.includes('404')) {
      // If it's not a 404, it's a real error during update
      if (err.message.includes('PUT ')) {
        recordError(singularKey, item.id, err);
        return null;
      }
    }
  }

  // Create new
  try {
    // Remove the ID so Shopify generates a new one if needed
    const createItem = { ...cleanItem };
    // Keep the original ID for reference but Shopify may assign a new one
    delete createItem.id;

    const createResponse = await shopifyFetch(
      `${BASE_URL}${resourcePath}.json`,
      {
        method: 'POST',
        body: JSON.stringify({ [singularKey]: createItem }),
      }
    );

    if (createResponse.ok) {
      stats.created++;
      const data = await createResponse.json();
      return data[singularKey];
    } else {
      const errBody = await createResponse.text();
      throw new Error(`POST ${createResponse.status}: ${errBody}`);
    }
  } catch (err) {
    recordError(singularKey, item.id, err);
    return null;
  }
}

/**
 * Restore metafields for a resource.
 *
 * @param {string} ownerPath – e.g. '/products/123'
 * @param {object[]} metafields – Array of metafield objects
 */
async function restoreMetafields(ownerPath, metafields) {
  for (const mf of metafields) {
    if (DRY_RUN) {
      log(`  [DRY RUN] Would restore metafield ${mf.namespace}.${mf.key}`);
      stats.skipped++;
      continue;
    }

    try {
      const response = await shopifyFetch(
        `${BASE_URL}${ownerPath}/metafields.json`,
        {
          method: 'POST',
          body: JSON.stringify({
            metafield: {
              namespace: mf.namespace,
              key: mf.key,
              value: mf.value,
              type: mf.type,
            },
          }),
        }
      );

      if (response.ok) {
        stats.created++;
      } else if (response.status === 422) {
        // Likely already exists — try to update via GraphQL or skip
        stats.skipped++;
      } else {
        const errBody = await response.text();
        throw new Error(`${response.status}: ${errBody}`);
      }
    } catch (err) {
      recordError('metafield', `${mf.namespace}.${mf.key}`, err);
    }
  }
}

// ---------------------------------------------------------------------------
// Resource restorers
// ---------------------------------------------------------------------------

async function restoreProducts(backup) {
  const products = backup.products || [];
  log(`🛍️  Restoring ${products.length} products...`);

  for (let i = 0; i < products.length; i++) {
    const product = products[i];
    await upsertResource('/products', 'product', product, [
      'variants', // Variants are nested and managed separately by Shopify
      'image',
      'options',
    ]);

    if ((i + 1) % 25 === 0) {
      log(`  ...processed ${i + 1}/${products.length}`);
    }
  }
}

async function restoreProductMetafields(backup) {
  const metafieldsMap = backup.product_metafields || {};
  const productIds = Object.keys(metafieldsMap);
  log(`🏷️  Restoring metafields for ${productIds.length} products...`);

  for (const productId of productIds) {
    await restoreMetafields(
      `/products/${productId}`,
      metafieldsMap[productId]
    );
  }
}

async function restoreVariantMetafields(backup) {
  const metafieldsMap = backup.variant_metafields || {};
  const variantIds = Object.keys(metafieldsMap);
  log(`🏷️  Restoring metafields for ${variantIds.length} variants...`);

  for (const variantId of variantIds) {
    await restoreMetafields(
      `/variants/${variantId}`,
      metafieldsMap[variantId]
    );
  }
}

async function restoreInventory(backup) {
  const levels = backup.inventory_levels || [];
  log(`📦 Restoring ${levels.length} inventory levels...`);

  for (let i = 0; i < levels.length; i++) {
    const level = levels[i];

    if (DRY_RUN) {
      log(
        `  [DRY RUN] Would set inventory_item ${level.inventory_item_id} at location ${level.location_id} to ${level.available}`
      );
      stats.skipped++;
      continue;
    }

    // Use the inventory_levels/set endpoint to set absolute quantities
    try {
      const response = await shopifyFetch(
        `${BASE_URL}/inventory_levels/set.json`,
        {
          method: 'POST',
          body: JSON.stringify({
            location_id: level.location_id,
            inventory_item_id: level.inventory_item_id,
            available: level.available,
          }),
        }
      );

      if (response.ok) {
        stats.updated++;
      } else {
        const errBody = await response.text();
        throw new Error(`${response.status}: ${errBody}`);
      }
    } catch (err) {
      recordError(
        'inventory_level',
        `item:${level.inventory_item_id}@loc:${level.location_id}`,
        err
      );
    }

    if ((i + 1) % 100 === 0) {
      log(`  ...processed ${i + 1}/${levels.length}`);
    }
  }
}

async function restoreCustomCollections(backup) {
  const collections = backup.custom_collections || [];
  log(`📂 Restoring ${collections.length} custom collections...`);

  for (const col of collections) {
    await upsertResource('/custom_collections', 'custom_collection', col);
  }
}

async function restoreSmartCollections(backup) {
  const collections = backup.smart_collections || [];
  log(`📂 Restoring ${collections.length} smart collections...`);

  for (const col of collections) {
    await upsertResource('/smart_collections', 'smart_collection', col);
  }
}

async function restoreCollectionMetafields(backup) {
  const customMf = backup.custom_collection_metafields || {};
  const smartMf = backup.smart_collection_metafields || {};

  log(`🏷️  Restoring collection metafields...`);

  for (const colId of Object.keys(customMf)) {
    await restoreMetafields(`/collections/${colId}`, customMf[colId]);
  }
  for (const colId of Object.keys(smartMf)) {
    await restoreMetafields(`/collections/${colId}`, smartMf[colId]);
  }
}

async function restoreCustomers(backup) {
  const customers = backup.customers || [];
  log(`👤 Restoring ${customers.length} customers...`);
  log(
    '  ⚠️  Note: Customer passwords cannot be restored. Customers will need to reset passwords.'
  );

  for (let i = 0; i < customers.length; i++) {
    await upsertResource('/customers', 'customer', customers[i], [
      'orders_count',
      'total_spent',
      'last_order_id',
      'last_order_name',
      'multipass_identifier',
    ]);

    if ((i + 1) % 50 === 0) {
      log(`  ...processed ${i + 1}/${customers.length}`);
    }
  }
}

async function restoreCustomerMetafields(backup) {
  const metafieldsMap = backup.customer_metafields || {};
  const customerIds = Object.keys(metafieldsMap);
  log(`🏷️  Restoring metafields for ${customerIds.length} customers...`);

  for (const customerId of customerIds) {
    await restoreMetafields(
      `/customers/${customerId}`,
      metafieldsMap[customerId]
    );
  }
}

async function restorePages(backup) {
  const pages = backup.pages || [];
  log(`📄 Restoring ${pages.length} pages...`);

  for (const page of pages) {
    await upsertResource('/pages', 'page', page);
  }
}

async function restorePageMetafields(backup) {
  const metafieldsMap = backup.page_metafields || {};
  const pageIds = Object.keys(metafieldsMap);
  log(`🏷️  Restoring metafields for ${pageIds.length} pages...`);

  for (const pageId of pageIds) {
    await restoreMetafields(`/pages/${pageId}`, metafieldsMap[pageId]);
  }
}

async function restoreBlogs(backup) {
  const blogs = backup.blogs || [];
  log(`📰 Restoring ${blogs.length} blogs...`);

  for (const blog of blogs) {
    await upsertResource('/blogs', 'blog', blog);
  }
}

async function restoreArticles(backup) {
  const articlesMap = backup.articles || {};
  log(`📰 Restoring articles...`);

  for (const [blogId, articles] of Object.entries(articlesMap)) {
    log(`  Blog #${blogId}: ${articles.length} articles`);
    for (const article of articles) {
      await upsertResource(
        `/blogs/${blogId}/articles`,
        'article',
        article,
        ['blog_id']
      );
    }
  }
}

async function restoreRedirects(backup) {
  const redirects = backup.redirects || [];
  log(`↩️  Restoring ${redirects.length} redirects...`);

  for (const redirect of redirects) {
    await upsertResource('/redirects', 'redirect', redirect);
  }
}

async function restoreThemeAssets(backup) {
  const themesMap = backup.themes || {};
  log(`🎨 Restoring theme assets...`);

  // Find the published/main theme in the current store
  const currentThemesResponse = await shopifyFetch(`${BASE_URL}/themes.json`);
  const currentThemes = (await currentThemesResponse.json()).themes || [];
  const mainTheme = currentThemes.find((t) => t.role === 'main');

  if (!mainTheme) {
    log('  ⚠️  No main theme found on target store. Skipping theme restore.');
    return;
  }

  // Find the main theme in the backup
  let backupMainTheme = null;
  for (const [, themeData] of Object.entries(themesMap)) {
    if (themeData.theme_info?.role === 'main') {
      backupMainTheme = themeData;
      break;
    }
  }

  if (!backupMainTheme) {
    log('  ⚠️  No main theme found in backup. Skipping theme restore.');
    return;
  }

  const assets = backupMainTheme.assets || [];
  log(
    `  🎨 Restoring ${assets.length} assets to theme "${mainTheme.name}" (#${mainTheme.id})...`
  );

  for (let i = 0; i < assets.length; i++) {
    const asset = assets[i];
    if (!asset.key || asset._error) continue;

    if (DRY_RUN) {
      log(`  [DRY RUN] Would restore asset ${asset.key}`);
      stats.skipped++;
      continue;
    }

    try {
      const payload = { asset: { key: asset.key } };

      // Theme assets can have either value (text) or attachment (base64 binary)
      if (asset.value != null) {
        payload.asset.value = asset.value;
      } else if (asset.attachment != null) {
        payload.asset.attachment = asset.attachment;
      } else {
        stats.skipped++;
        continue;
      }

      const response = await shopifyFetch(
        `${BASE_URL}/themes/${mainTheme.id}/assets.json`,
        { method: 'PUT', body: JSON.stringify(payload) }
      );

      if (response.ok) {
        stats.updated++;
      } else {
        const errBody = await response.text();
        throw new Error(`${response.status}: ${errBody}`);
      }
    } catch (err) {
      recordError('theme_asset', asset.key, err);
    }

    if ((i + 1) % 50 === 0) {
      log(`    ...processed ${i + 1}/${assets.length}`);
    }
  }
}

async function restoreMetafieldDefinitions(backup) {
  const definitionsMap = backup.metafield_definitions || {};
  log(`📐 Restoring metafield definitions...`);

  const mutation = `
    mutation CreateMetafieldDefinition($definition: MetafieldDefinitionInput!) {
      metafieldDefinitionCreate(definition: $definition) {
        createdDefinition {
          id
        }
        userErrors {
          field
          message
        }
      }
    }
  `;

  for (const [ownerType, definitions] of Object.entries(definitionsMap)) {
    for (const def of definitions) {
      if (DRY_RUN) {
        log(`  [DRY RUN] Would create definition ${def.namespace}.${def.key} for ${ownerType}`);
        stats.skipped++;
        continue;
      }

      try {
        const result = await graphqlRequest(mutation, {
          definition: {
            name: def.name,
            namespace: def.namespace,
            key: def.key,
            type: def.type.name,
            description: def.description || '',
            ownerType,
            validations: def.validations || [],
          },
        });

        const errors =
          result.metafieldDefinitionCreate?.userErrors || [];
        if (errors.length > 0) {
          if (errors.some((e) => e.message.includes('already exists'))) {
            stats.skipped++;
          } else {
            throw new Error(errors.map((e) => e.message).join(', '));
          }
        } else {
          stats.created++;
        }
      } catch (err) {
        recordError('metafield_definition', `${def.namespace}.${def.key}`, err);
      }
    }
  }
}

async function restoreMetaobjectDefinitions(backup) {
  const definitions = backup.metaobject_definitions || [];
  log(`📐 Restoring ${definitions.length} metaobject definitions...`);

  const mutation = `
    mutation CreateMetaobjectDefinition($definition: MetaobjectDefinitionCreateInput!) {
      metaobjectDefinitionCreate(definition: $definition) {
        metaobjectDefinition {
          id
        }
        userErrors {
          field
          message
        }
      }
    }
  `;

  for (const def of definitions) {
    if (DRY_RUN) {
      log(`  [DRY RUN] Would create metaobject definition "${def.type}"`);
      stats.skipped++;
      continue;
    }

    try {
      const fieldDefs = (def.fieldDefinitions || []).map((fd) => ({
        name: fd.name,
        key: fd.key,
        description: fd.description || '',
        required: fd.required,
        type: fd.type.name,
        validations: fd.validations || [],
      }));

      const result = await graphqlRequest(mutation, {
        definition: {
          name: def.name,
          type: def.type,
          description: def.description || '',
          displayNameKey: def.displayNameKey,
          access: def.access || {},
          fieldDefinitions: fieldDefs,
        },
      });

      const errors =
        result.metaobjectDefinitionCreate?.userErrors || [];
      if (errors.length > 0) {
        if (errors.some((e) => e.message.includes('already exists') || e.message.includes('taken'))) {
          stats.skipped++;
        } else {
          throw new Error(errors.map((e) => e.message).join(', '));
        }
      } else {
        stats.created++;
      }
    } catch (err) {
      recordError('metaobject_definition', def.type, err);
    }
  }
}

async function restoreMetaobjects(backup) {
  const metaobjectsMap = backup.metaobjects || {};
  log(`📦 Restoring metaobjects...`);

  const mutation = `
    mutation UpsertMetaobject($handle: MetaobjectHandleInput!, $metaobject: MetaobjectUpsertInput!) {
      metaobjectUpsert(handle: $handle, metaobject: $metaobject) {
        metaobject {
          id
        }
        userErrors {
          field
          message
        }
      }
    }
  `;

  for (const [type, objects] of Object.entries(metaobjectsMap)) {
    log(`  📦 Type "${type}": ${objects.length} objects`);

    for (const obj of objects) {
      if (DRY_RUN) {
        log(`  [DRY RUN] Would upsert metaobject ${type}/${obj.handle}`);
        stats.skipped++;
        continue;
      }

      try {
        const fields = (obj.fields || []).map((f) => ({
          key: f.key,
          value: f.value,
        }));

        const result = await graphqlRequest(mutation, {
          handle: { type, handle: obj.handle },
          metaobject: { fields },
        });

        const errors = result.metaobjectUpsert?.userErrors || [];
        if (errors.length > 0) {
          throw new Error(errors.map((e) => e.message).join(', '));
        } else {
          stats.created++;
        }
      } catch (err) {
        recordError('metaobject', `${type}/${obj.handle}`, err);
      }
    }
  }
}

async function restoreShopMetafields(backup) {
  const metafields = backup.shop_metafields || [];
  log(`🏷️  Restoring ${metafields.length} shop metafields...`);

  const mutation = `
    mutation SetShopMetafield($metafields: [MetafieldsSetInput!]!) {
      metafieldsSet(metafields: $metafields) {
        metafields {
          id
        }
        userErrors {
          field
          message
        }
      }
    }
  `;

  // Process in batches of 25 (Shopify limit for metafieldsSet)
  for (let i = 0; i < metafields.length; i += 25) {
    const batch = metafields.slice(i, i + 25);

    if (DRY_RUN) {
      log(`  [DRY RUN] Would set ${batch.length} shop metafields`);
      stats.skipped += batch.length;
      continue;
    }

    try {
      const input = batch.map((mf) => ({
        namespace: mf.namespace,
        key: mf.key,
        value: mf.value,
        type: mf.type,
        ownerId: `gid://shopify/Shop`, // Shop-level
      }));

      const result = await graphqlRequest(mutation, { metafields: input });
      const errors = result.metafieldsSet?.userErrors || [];

      if (errors.length > 0) {
        for (const err of errors) {
          stats.failed++;
          stats.errors.push(`shop_metafield: ${err.message}`);
        }
      } else {
        stats.updated += batch.length;
      }
    } catch (err) {
      recordError('shop_metafields', `batch ${i}`, err);
    }
  }
}

async function restorePriceRules(backup) {
  const priceRules = backup.price_rules_and_discounts || [];
  log(`💰 Restoring ${priceRules.length} price rules & discount codes...`);

  for (const rule of priceRules) {
    const discountCodes = rule.discount_codes || [];
    const ruleClean = { ...rule };
    delete ruleClean.discount_codes;

    const created = await upsertResource('/price_rules', 'price_rule', ruleClean, [
      'usage_count',
    ]);

    if (created && discountCodes.length > 0) {
      for (const code of discountCodes) {
        await upsertResource(
          `/price_rules/${created.id}/discount_codes`,
          'discount_code',
          code,
          ['usage_count']
        );
      }
    }
  }
}

async function restoreScriptTags(backup) {
  const scriptTags = backup.script_tags || [];
  log(`📜 Restoring ${scriptTags.length} script tags...`);

  for (const tag of scriptTags) {
    await upsertResource('/script_tags', 'script_tag', tag);
  }
}

// ---------------------------------------------------------------------------
// Restore orchestration
// ---------------------------------------------------------------------------

const RESTORE_MAP = {
  products: restoreProducts,
  product_metafields: restoreProductMetafields,
  variant_metafields: restoreVariantMetafields,
  inventory: restoreInventory,
  custom_collections: restoreCustomCollections,
  smart_collections: restoreSmartCollections,
  collection_metafields: restoreCollectionMetafields,
  customers: restoreCustomers,
  customer_metafields: restoreCustomerMetafields,
  pages: restorePages,
  page_metafields: restorePageMetafields,
  blogs: restoreBlogs,
  articles: restoreArticles,
  redirects: restoreRedirects,
  theme_assets: restoreThemeAssets,
  metafield_definitions: restoreMetafieldDefinitions,
  metaobject_definitions: restoreMetaobjectDefinitions,
  metaobjects: restoreMetaobjects,
  shop_metafields: restoreShopMetafields,
  price_rules: restorePriceRules,
  script_tags: restoreScriptTags,
};

async function runRestore() {
  const startTime = Date.now();

  // Load backup file
  log(`📂 Loading backup from: ${flags.file}`);
  const raw = readFileSync(flags.file, 'utf-8');
  const backup = JSON.parse(raw);

  log(`   Backup created: ${backup._meta?.created_at}`);
  log(`   Backup store: ${backup._meta?.store}`);
  log(`   Target store: ${STORE}`);
  log(`   Resources to restore: ${resourcesToRestore.join(', ')}`);
  log(`   Dry run: ${DRY_RUN}`);
  log('');

  // Safety check: warn if restoring to a different store
  if (backup._meta?.store && backup._meta.store !== STORE) {
    log('⚠️  WARNING: Backup store does not match target store!');
    log(
      `   Backup: ${backup._meta.store} → Target: ${STORE}`
    );
    log('   Resource IDs may not match. Restore may create duplicates.');
    log('');
  }

  // Execute restore in dependency order
  const orderedResources = [
    // First: definitions (metafield + metaobject schemas must exist before values)
    'metafield_definitions',
    'metaobject_definitions',
    // Then: primary resources
    'products',
    'custom_collections',
    'smart_collections',
    'customers',
    'pages',
    'blogs',
    'articles',
    // Then: dependent data
    'product_metafields',
    'variant_metafields',
    'collection_metafields',
    'customer_metafields',
    'page_metafields',
    'inventory',
    'metaobjects',
    'shop_metafields',
    // Then: auxiliary
    'redirects',
    'theme_assets',
    'price_rules',
    'script_tags',
  ];

  for (const resource of orderedResources) {
    if (!resourcesToRestore.includes(resource)) continue;
    if (!RESTORE_MAP[resource]) continue;

    log('');
    try {
      await RESTORE_MAP[resource](backup);
    } catch (err) {
      log(`❌ Fatal error restoring ${resource}: ${err.message}`);
      stats.errors.push(`FATAL ${resource}: ${err.message}`);
    }
  }

  // Summary
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  log('');
  log('═══════════════════════════════════════');
  log(`✅ Restore complete in ${elapsed}s`);
  log(`   Created: ${stats.created}`);
  log(`   Updated: ${stats.updated}`);
  log(`   Skipped: ${stats.skipped}`);
  log(`   Failed:  ${stats.failed}`);

  if (stats.errors.length > 0) {
    log('');
    log('❌ Errors:');
    for (const err of stats.errors.slice(0, 50)) {
      log(`   - ${err}`);
    }
    if (stats.errors.length > 50) {
      log(`   ... and ${stats.errors.length - 50} more`);
    }
  }

  log('═══════════════════════════════════════');

  // Output summary as JSON to stdout for n8n
  console.log(
    JSON.stringify({
      success: stats.failed === 0,
      duration_seconds: parseFloat(elapsed),
      created: stats.created,
      updated: stats.updated,
      skipped: stats.skipped,
      failed: stats.failed,
      error_count: stats.errors.length,
    })
  );

  if (stats.failed > 0) {
    process.exit(1);
  }
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
 * grant. A single restore run fits comfortably inside the token lifetime.
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
  .then(runRestore)
  .catch((err) => {
    log(`❌ Restore failed: ${err.message}`);
    log(err.stack);
    process.exit(1);
  });
