# stashify

Automated backup of an entire Shopify store with full media download and
selective restore. Runs as n8n workflows with two independent backup strategies:

- **Local backup** — store data + media to local/NAS storage
- **Google Drive backup** — store data + media directly to Google Drive

Import one or both depending on your needs.

## Prerequisites

- **n8n** (self-hosted) with Docker CLI available in the container
- **[anvil](https://github.com/code-lodge/anvil)** — the recommended base image (includes Docker CLI + rclone)
- **Shopify Custom App** with Admin API access
- **rclone** (Google Drive workflow only — included in the base image above)
- **Google Drive OAuth2 credentials in n8n** (Google Drive workflow only)

## What Gets Backed Up

### Data (JSON)

| Resource                    | Backed Up | Restorable | Notes                              |
| --------------------------- | --------- | ---------- | ---------------------------------- |
| Shop info                   | ✅        | ❌         | Read-only reference                |
| Policies                    | ✅        | ❌         | Managed in admin                   |
| Shipping zones              | ✅        | ❌         | Reference only                     |
| Countries/taxes             | ✅        | ❌         | Reference only                     |
| **Products**                | ✅        | ✅         | With variants and images           |
| Product metafields          | ✅        | ✅         | Per-product                        |
| Variant metafields          | ✅        | ✅         | Per-variant                        |
| **Inventory levels**        | ✅        | ✅         | Per-location quantities            |
| Inventory items             | ✅        | ❌         | SKU/tracking info                  |
| **Custom collections**      | ✅        | ✅         |                                    |
| **Smart collections**       | ✅        | ✅         | Including rules                    |
| Collection metafields       | ✅        | ✅         |                                    |
| Collects                    | ✅        | ❌         | Product↔Collection maps            |
| **Customers**               | ✅        | ✅         | ⚠️ GDPR — passwords not restorable |
| Customer metafields         | ✅        | ✅         |                                    |
| **Orders**                  | ✅        | ❌         | Historical reference               |
| Draft orders                | ✅        | ❌         | Complex state                      |
| **Pages**                   | ✅        | ✅         |                                    |
| Page metafields             | ✅        | ✅         |                                    |
| **Blogs**                   | ✅        | ✅         |                                    |
| **Articles**                | ✅        | ✅         | Per-blog                           |
| **Themes + assets**         | ✅        | ✅         | Liquid, CSS, JS, images            |
| **Redirects**               | ✅        | ✅         | URL redirects                      |
| Script tags                 | ✅        | ✅         |                                    |
| **Price rules + discounts** | ✅        | ✅         |                                    |
| Gift cards                  | ✅        | ❌         | Security restrictions              |
| **Metafield definitions**   | ✅        | ✅         | All owner types (GraphQL)          |
| **Metaobject definitions**  | ✅        | ✅         | Schema + fields (GraphQL)          |
| **Metaobjects**             | ✅        | ✅         | All types, upsert (GraphQL)        |
| **Shop metafields**         | ✅        | ✅         | Shop-level custom data (GraphQL)   |

### Media (files)

All binary files from Shopify's CDN, stored in a structured folder:

```
media/
├── products/
│   ├── classic-blue-t-shirt/
│   │   ├── 1_front.jpg
│   │   └── 2_back.jpg
│   └── canvas-tote-bag/
│       └── 1_product-photo.png
├── collections/
│   ├── clothing.jpg
│   └── accessories.jpg
├── files/
│   ├── images/
│   ├── documents/
│   └── videos/
└── manifest.json
```

Media sync is **incremental** — only new or changed files are downloaded.

## Setup

### 1. Create Shopify Custom App

Create an app in the Shopify **Dev Dashboard** (the **Apps** section → develop/create an app) and assign it the Admin API scopes below.

> **Heads up — token model changed.** Apps created in the Dev Dashboard no
> longer expose a static `shpat_…` Admin API access token. Instead the app gives
> you a **Client ID** (Klant-ID) and **Client Secret** (Geheim), which these
> scripts exchange for a short-lived (~24h) token automatically via the
> [client credentials grant](https://shopify.dev/docs/apps/build/dev-dashboard/get-api-access-tokens).
> You provide `SHOPIFY_CLIENT_ID` + `SHOPIFY_CLIENT_SECRET` instead of a token.
>
> If you have an older **admin-managed** custom app that still issues a static
> `shpat_…` token, you can keep using it via `SHOPIFY_ACCESS_TOKEN` — the
> scripts accept either.

Required Admin API scopes:

```
read_products          write_products
read_customers         write_customers
read_orders
read_inventory         write_inventory
read_content           write_content
read_themes            write_themes
read_files
read_script_tags       write_script_tags
read_shipping          read_locations
read_price_rules       write_price_rules
read_discounts         write_discounts
read_gift_cards
read_metaobject_definitions    write_metaobject_definitions
read_metaobject_entries        write_metaobject_entries
```

Install the app, then copy its **Client ID** (Klant-ID) and **Client Secret**
(Geheim) from the app's credentials/settings screen. (For a legacy
admin-managed custom app, copy the **Admin API access token** instead.)

### 2. Copy Scripts Into the Container

Copy the backup scripts into your n8n container:

```bash
docker cp shopify-backup.mjs n8n:/opt/shopify-backup/shopify-backup.mjs
docker cp shopify-media-sync.mjs n8n:/opt/shopify-backup/shopify-media-sync.mjs
docker cp shopify-restore.mjs n8n:/opt/shopify-backup/shopify-restore.mjs
docker exec n8n chmod +x /opt/shopify-backup/*.mjs
```

> To make scripts survive container rebuilds, create a persistent copy:
> ```bash
> # Scripts on CasaOS host (persists across updates)
> mkdir -p /DATA/Data/shopify-backup
> cp shopify-backup.mjs shopify-media-sync.mjs shopify-restore.mjs /DATA/Data/shopify-backup/
> ```
> Then set these n8n environment variables:
> ```
> SHOPIFY_BACKUP_SCRIPT_PATH=/home/node/data/shopify-backup/shopify-backup.mjs
> SHOPIFY_MEDIA_SCRIPT_PATH=/home/node/data/shopify-backup/shopify-media-sync.mjs
> SHOPIFY_RESTORE_SCRIPT_PATH=/home/node/data/shopify-backup/shopify-restore.mjs
> ```

### 3. Set Environment Variables

Add these to your n8n instance (CasaOS app settings or container environment):

```env
SHOPIFY_STORE=your-store.myshopify.com
SHOPIFY_API_VERSION=2025-01

# Dev Dashboard app (recommended) — scripts fetch a short-lived token:
SHOPIFY_CLIENT_ID=your_client_id_here
SHOPIFY_CLIENT_SECRET=your_client_secret_here

# OR, for a legacy admin-managed custom app with a static token, use this
# instead of the CLIENT_ID/SECRET pair above:
# SHOPIFY_ACCESS_TOKEN=shpat_your_access_token_here
```

**Local workflow only:**

```env
BACKUP_DATA_DIR=/backups/data
BACKUP_MEDIA_DIR=/backups/media
```

**Google Drive workflow only:**

```env
GDRIVE_BACKUP_FOLDER_ID=your_google_drive_folder_id
GDRIVE_MEDIA_STAGING=/home/node/data/gdrive-media-cache
RCLONE_REMOTE=gdrive
RCLONE_MEDIA_PATH=Shopify Backups/media
RCLONE_CONFIG=/home/node/data/rclone/rclone.conf
```

> **Required for all workflows:** these workflows read the variables above via
> `{{ $env.VAR }}` expressions. n8n blocks expression access to environment
> variables by default, so you must also set:
> ```env
> N8N_BLOCK_ENV_ACCESS_IN_NODE=false
> ```
> Without it, nodes show **`[access to env vars denied]`**. Restart n8n after
> changing it.

### 4. Import Workflows

In n8n: **Workflows** → **Import from File** → select the JSON.

| Workflow                 | Purpose                        | Schedule     |
| ------------------------ | ------------------------------ | ------------ |
| `n8n-backup-local.json`  | Backup to local/NAS storage    | Daily 2:00AM |
| `n8n-backup-gdrive.json` | Backup directly to Google Drive| Daily 3:00AM |
| `n8n-restore.json`       | Restore from local or Drive    | Manual       |

Import one or both backup workflows. The restore workflow supports both sources.

Activate the backup workflow(s) and run manually once to test.

## Quick Test (no n8n required)

Verify the scripts work against your store before importing to n8n:

```bash
export SHOPIFY_STORE=your-store.myshopify.com
export SHOPIFY_CLIENT_ID=your_client_id
export SHOPIFY_CLIENT_SECRET=your_client_secret
# (or, for a legacy app: export SHOPIFY_ACCESS_TOKEN=shpat_xxx)
export BACKUP_OUTPUT_DIR=/tmp/shopify-test

# Run data backup
node shopify-backup.mjs

# Run media sync
export MEDIA_OUTPUT_DIR=/tmp/shopify-media-test
node shopify-media-sync.mjs

# Dry-run restore
node shopify-restore.mjs --file /tmp/shopify-test/shopify-backup_*.json --dry-run
```

## Restoring

Open the **Shopify Restore from Backup** workflow and edit the
**Restore Configuration** node:

```javascript
const config = {
  source: 'local',                              // or 'gdrive'
  backup_path: '/backups/data/FILENAME.json',    // local path
  backup_file_id: '',                            // Google Drive file ID
  resources: 'all',                              // or comma-separated list
  dry_run: true,                                 // preview first!
  skip_existing: false
};
```

**Always dry-run first**, review the output, then set `dry_run: false`.

### Selective Restore

```javascript
{ resources: 'inventory', dry_run: false }
{ resources: 'products,product_metafields,variant_metafields,inventory', dry_run: false }
{ resources: 'pages,page_metafields,blogs,articles', dry_run: false }
{ resources: 'metafield_definitions,metaobject_definitions,metaobjects', dry_run: false }
{ resources: 'theme_assets', dry_run: false }
```

## Notifications

Both backup workflows have a **Notify on Failure** placeholder node. Connect
your preferred service: Email/SMTP, Slack, Telegram, or webhook
(Uptime Kuma, Healthchecks.io, etc.).

## Estimated Runtime

| Store Size | Products | Data Backup | Media (first run) | Media (incremental) |
| ---------- | -------- | ----------- | ----------------- | ------------------- |
| Small      | <100     | 3-5 min     | 5-10 min          | <1 min              |
| Medium     | 100-500  | 5-15 min    | 15-30 min         | 1-3 min             |
| Large      | 500-2000 | 15-45 min   | 30-90 min         | 2-10 min            |

## GDPR / AVG

The backup contains personal data (customers, orders). Under GDPR/AVG:

- Restrict access to backup storage to authorized personnel only
- JSON backups auto-expire after 30 days (retention cleanup in local workflow)
- Media files don't contain PII (images only)
- Google Drive: use a dedicated Workspace account with proper access controls
- Right to erasure: document that backups rotate on a 30-day cycle
- Consider encrypting the backup directory (LUKS, ZFS encryption, etc.)

## Files

```
.
├── README.md                 # This file
├── LICENSE                   # GPL-3.0-or-later
├── shopify-backup.mjs        # Data backup script (REST + GraphQL)
├── shopify-media-sync.mjs    # Incremental media download
├── shopify-restore.mjs       # Selective restore with dry-run
├── n8n-backup-local.json     # n8n workflow: local/NAS backup
├── n8n-backup-gdrive.json    # n8n workflow: Google Drive backup
└── n8n-restore.json          # n8n workflow: restore from local or Drive
```

## License

[GNU General Public License v3.0 or later](LICENSE)
