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

5. Re-enable the `d1_databases` and `queues` bindings in `wrangler.jsonc`, then apply migrations:

```bash
npm run migrate:local
```

## Current Routes

- `GET /` returns the internal dashboard shell.
- `GET /api/status` returns the service status payload as JSON.
- `GET /ingest-info` shows current ingestion readiness.
- `POST /ingest` currently returns a `501` response until queue and database bindings are configured.
- A weekday cron trigger is enabled and writes a log entry for now.

## Initial Priorities

- build one or two source connectors
- normalize changes into `events`
- score affected `sites`
- surface alerts in a lightweight internal dashboard
