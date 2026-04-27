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

3. Create a D1 database and update `wrangler.jsonc` with its IDs.

4. Run local development:

```bash
npm run dev
```

5. Apply migrations:

```bash
npm run migrate:local
```

## Initial Priorities

- build one or two source connectors
- normalize changes into `events`
- score affected `sites`
- surface alerts in a lightweight internal dashboard

