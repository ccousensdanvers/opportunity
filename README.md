# Danvers Opportunity Agent

Starter repository for an internal Danvers economic development opportunity intelligence tool built on Cloudflare Workers.

## Goal

Build a staff-facing system that:

- ingests public signals relevant to development and redevelopment in Danvers
- turns those signals into structured events
- scores sites and corridors against Town priorities
- generates staff-reviewable alerts and site briefs

## V1 Scope

- Cloudflare Worker API
- D1 database for core entities
- scheduled ingestion entrypoint
- queue consumer for background processing
- initial schema for parcels, sites, events, alerts, and notes

## Tech Stack

- Cloudflare Workers
- D1
- Queues
- Cron Triggers
- TypeScript

## Quick Start

1. Install dependencies:

```bash
npm install
```

2. Copy local environment template:

```bash
cp .dev.vars.example .dev.vars
```

3. Run the bootstrap Worker locally:

```bash
npm run dev
```

4. Create the D1 database and queue resources once you are ready to enable persistence and background ingestion.

5. Add the real `OPPORTUNITYDB` D1 binding values in `wrangler.jsonc`, then apply migrations:

```bash
npm run migrate:local
```

Helpful shortcuts once `wrangler` is available in your environment:

```bash
npm run cf:migrate:local
npm run cf:migrate:remote
npm run cf:smoke
```

## Current Routes

- `GET /` returns the internal dashboard shell.
- The dashboard includes live client-side filtering against the Worker's site dataset.
- The dashboard also shows a live public signal feed from Danvers Agenda Center for Planning Board and ZBA agenda postings.
- `GET /api/status` returns the service status payload as JSON.
- `GET /api/summary` returns the current dashboard summary metrics.
- `GET /api/sites` returns the current watchlist dataset.
- `GET /api/signals` returns the live agenda signal feed.
- `GET /api/briefs` returns lightweight case briefs derived from agenda postings and simple packet parsing.
- `GET /ingest-info` shows current ingestion readiness.
- `POST /api/parcels/upsert` upserts parcel records and parcel aliases into D1.
- `POST /api/opportunities/match` matches opportunity records to parcels and stores every result in D1, including unmatched items that need staff review.
- `GET /api/opportunities/review` returns the current review queue, including low-confidence and no-match items.
- `POST /ingest` runs the full ingest cycle when D1 is configured: refresh Danvers parcels, rebuild agenda briefs, and match address-bearing opportunities to parcels.
- A weekday cron trigger runs the same ingest cycle automatically.

## Initial Priorities

- build one or two source connectors
- normalize changes into `events`
- score affected `sites`
- surface alerts in a lightweight internal dashboard

## Automated Parcel Source

The current automated parcel source is the Town of Danvers public ArcGIS parcel layer:

- `https://gis.danversma.gov/danversexternal/rest/services/DanversMA_Parcels_AGOL/MapServer/1`

The Worker pulls parcel identifiers and addresses from that layer during manual and scheduled ingest runs, then matches case-brief addresses against the refreshed parcel table in D1.
