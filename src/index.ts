import {
  listParcelReviewQueue,
  matchAndPersistOpportunities,
  upsertParcels,
  type OpportunityParcelInput,
  type ParcelUpsertInput,
} from "./parcel-matching";

type IngestTrigger = "manual" | "scheduled";
type SiteStatus = "Needs staff review" | "Policy setup" | "Market scan" | "Infrastructure check";
type Readiness = "Early" | "Emerging" | "Advancing";

interface Env {
  OPPORTUNITYDB?: D1Database;
}

interface IngestMessage {
  trigger: IngestTrigger;
  requestedAt: string;
}

interface OpportunitySite {
  id: string;
  site: string;
  corridor: string;
  signal: string;
  focus: string;
  status: SiteStatus;
  readiness: Readiness;
  score: number;
  source: string;
  updatedAt: string;
}

interface SummaryMetric {
  label: string;
  value: string;
  detail: string;
  tone?: "neutral" | "warm" | "cool";
}

interface ActivityItem {
  time: string;
  title: string;
  detail: string;
}

interface AgendaSignal {
  board: "Planning Board" | "Zoning Board of Appeals";
  meetingDate: string;
  title: string;
  agendaUrl: string;
  source: string;
}

interface CaseBrief {
  id: string;
  board: AgendaSignal["board"];
  meetingDate: string;
  title: string;
  agendaUrl: string;
  likelySite: string;
  addresses: string[];
  signalType: string;
  rationale: string;
  confidence: "high" | "medium" | "low";
  source: string;
}

interface DashboardPayload {
  generatedAt: string;
  summary: SummaryMetric[];
  sites: OpportunitySite[];
  activity: ActivityItem[];
  signals: AgendaSignal[];
  briefs: CaseBrief[];
}

const AGENDA_CENTER_URL = "https://www.danversma.gov/AgendaCenter";
const DANVERS_PARCELS_LAYER_URL =
  "https://gis.danversma.gov/danversexternal/rest/services/DanversMA_Parcels_AGOL/MapServer/1/query";
const DANVERS_PARCELS_PAGE_SIZE = 1000;

interface DanversParcelAttributes {
  MAP_PAR_ID?: string | null;
  GIS_ID?: string | null;
  Location?: string | null;
  StreetName?: string | null;
  LUCDescription?: string | null;
  YearBuilt?: string | null;
}

interface DanversParcelFeature {
  attributes?: DanversParcelAttributes;
}

interface DanversParcelQueryResponse {
  features?: DanversParcelFeature[];
  exceededTransferLimit?: boolean;
}

interface IngestRunSummary {
  parcelsIngested: number;
  opportunitiesPrepared: number;
  matched: number;
  reviewNeeded: number;
}

const SITES: OpportunitySite[] = [
  {
    id: "maple-industrial-edge",
    site: "Maple Street Industrial Edge",
    corridor: "Industrial District",
    signal: "ownership change watch",
    focus: "access, parcel assembly, reuse fit",
    status: "Needs staff review",
    readiness: "Emerging",
    score: 82,
    source: "staff seed list",
    updatedAt: "2026-04-27",
  },
  {
    id: "downtown-upper-floors",
    site: "Downtown Upper Floors",
    corridor: "Downtown",
    signal: "small-scale adaptive reuse",
    focus: "code path, mixed-use economics",
    status: "Policy setup",
    readiness: "Early",
    score: 68,
    source: "downtown reuse watch",
    updatedAt: "2026-04-27",
  },
  {
    id: "route-114-retail-cluster",
    site: "Route 114 Retail Cluster",
    corridor: "Route 114",
    signal: "tenant turnover",
    focus: "tax base retention, repositioning",
    status: "Market scan",
    readiness: "Emerging",
    score: 74,
    source: "retail scan",
    updatedAt: "2026-04-26",
  },
  {
    id: "endicott-flex-space",
    site: "Endicott Corridor Flex Space",
    corridor: "Endicott",
    signal: "industrial demand pressure",
    focus: "site readiness, utilities, zoning",
    status: "Infrastructure check",
    readiness: "Advancing",
    score: 79,
    source: "industrial demand watch",
    updatedAt: "2026-04-25",
  },
  {
    id: "cabot-redevelopment-strip",
    site: "Cabot Redevelopment Strip",
    corridor: "Cabot",
    signal: "underused frontage",
    focus: "corridor image, reuse strategy, fiscal upside",
    status: "Needs staff review",
    readiness: "Emerging",
    score: 77,
    source: "corridor screening",
    updatedAt: "2026-04-24",
  },
  {
    id: "north-shore-commerce-node",
    site: "North Shore Commerce Node",
    corridor: "Route 128/95 Access",
    signal: "regional demand alignment",
    focus: "competitiveness, employer fit, visibility",
    status: "Market scan",
    readiness: "Advancing",
    score: 71,
    source: "regional comparison",
    updatedAt: "2026-04-23",
  },
];

const ACTIVITIES: ActivityItem[] = [
  {
    time: "08:30",
    title: "Agenda feed and parcel ingest are wired",
    detail: "Manual and scheduled runs now pull Danvers parcel records, rebuild agenda-derived briefs, and push likely opportunities into parcel matching.",
  },
  {
    time: "09:15",
    title: "Staff review queue is taking shape",
    detail: "Low-confidence and no-match results now stay visible in the review endpoint so staff can see which filings still need manual cleanup.",
  },
  {
    time: "Next",
    title: "Current build focus: parcel-linked triage",
    detail: "We are turning meeting signals into parcel-level follow-up by tightening brief extraction, matching quality, and the staff-facing review workflow.",
  },
];

const FALLBACK_SIGNALS: AgendaSignal[] = [
  {
    board: "Planning Board",
    meetingDate: "May 27, 2025",
    title: "Planning Board Members",
    agendaUrl: "https://www.danversma.gov/AgendaCenter/ViewFile/Agenda/_05272025-1453",
    source: "danvers agenda center fallback",
  },
  {
    board: "Zoning Board of Appeals",
    meetingDate: "Jun 9, 2025",
    title: "Zoning Board of Appeals Members",
    agendaUrl: "https://www.danversma.gov/AgendaCenter/ViewFile/Agenda/_06092025-1461",
    source: "danvers agenda center fallback",
  },
];

function buildIngestMessage(trigger: IngestTrigger): IngestMessage {
  return {
    trigger,
    requestedAt: new Date().toISOString(),
  };
}

async function startIngestionRun(
  db: D1Database,
  trigger: IngestTrigger,
): Promise<number | null> {
  const result = await db
    .prepare(
      `
      INSERT INTO ingestion_runs (trigger, requested_at, status)
      VALUES (?, ?, ?)
      `,
    )
    .bind(trigger, new Date().toISOString(), "accepted")
    .run();

  const runId = Number((result as { meta?: { last_row_id?: number } }).meta?.last_row_id);
  return Number.isFinite(runId) ? runId : null;
}

async function finishIngestionRun(
  db: D1Database,
  runId: number | null,
  status: "completed" | "failed",
): Promise<void> {
  if (!runId) {
    return;
  }

  await db
    .prepare(
      `
      UPDATE ingestion_runs
      SET status = ?
      WHERE id = ?
      `,
    )
    .bind(status, runId)
    .run();
}

function buildStatusPayload(env?: Env) {
  return {
    service: "danvers-opportunity-agent",
    status: "ok",
    checkedAt: new Date().toISOString(),
    data: {
      phase: "dashboard-shell",
      capabilities: {
        api: true,
        dashboard: true,
        agendaSignals: true,
        caseExtraction: true,
        scheduledChecks: true,
        database: Boolean(env?.OPPORTUNITYDB),
        queue: false,
      },
    },
  };
}

function missingDatabaseResponse() {
  return Response.json(
    {
      ok: false,
      error: "OPPORTUNITYDB binding is not configured.",
      nextStep:
        "Add the OPPORTUNITYDB D1 binding in wrangler.jsonc and apply migrations.",
    },
    { status: 503 },
  );
}

async function readJson<T>(request: Request): Promise<T> {
  return (await request.json()) as T;
}

async function fetchJson<T>(url: string, timeoutMs = 8000): Promise<T> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(url, {
      signal: controller.signal,
      headers: {
        "user-agent": "Opportunity/0.1 (+https://www.danversma.gov/)",
      },
    });

    if (!response.ok) {
      throw new Error(`Request failed with ${response.status}`);
    }

    return (await response.json()) as T;
  } finally {
    clearTimeout(timeout);
  }
}

function escapeHtml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function normalizeWhitespace(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

function slugify(value: string): string {
  return value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function formatSignalType(text: string): string {
  const lowered = text.toLowerCase();
  if (lowered.includes("mixed-use")) return "mixed-use redevelopment";
  if (lowered.includes("special permit")) return "special permit";
  if (lowered.includes("variance")) return "variance request";
  if (lowered.includes("site plan")) return "site plan review";
  if (lowered.includes("sign")) return "signage change";
  return "board filing";
}

function inferRationale(signal: AgendaSignal, addresses: string[], packetText: string): string {
  const lowered = `${signal.title} ${packetText}`.toLowerCase();

  if (lowered.includes("mixed-use")) {
    return "Packet text points to a mixed-use or adaptive reuse concept that could affect downtown or corridor development strategy.";
  }
  if (lowered.includes("special permit")) {
    return "Special permit language suggests a live land-use action worth staff review for timing, zoning path, and fiscal implications.";
  }
  if (lowered.includes("variance")) {
    return "Variance language signals a site-specific development constraint or design change that may indicate a more active property.";
  }
  if (addresses.length) {
    return "The agenda packet appears to reference a specific address, which makes this a stronger candidate for site-level review.";
  }
  return "Board posting is live, but the packet parser only found limited structured clues, so this item still needs manual review.";
}

function inferConfidence(addresses: string[], packetText: string): CaseBrief["confidence"] {
  if (addresses.length >= 2) return "high";
  if (addresses.length === 1 || packetText.toLowerCase().includes("special permit")) return "medium";
  return "low";
}

function extractAddresses(text: string): string[] {
  const matches = text.match(/\b\d{1,5}\s+[A-Z][A-Za-z0-9.'-]*(?:\s+[A-Z][A-Za-z0-9.'-]*){0,4}\s+(?:Street|St|Road|Rd|Avenue|Ave|Lane|Ln|Drive|Dr|Boulevard|Blvd|Way|Court|Ct|Terrace|Ter|Place|Pl|Highway|Hwy|Circle|Cir)\b/g) ?? [];
  const deduped = Array.from(new Set(matches.map((value) => normalizeWhitespace(value))));
  return deduped.slice(0, 4);
}

function buildSummaryMetrics(
  sites: OpportunitySite[],
  signals: AgendaSignal[],
  briefs: CaseBrief[],
): SummaryMetric[] {
  const advancing = sites.filter((site) => site.readiness === "Advancing").length;
  const averageScore = Math.round(sites.reduce((sum, site) => sum + site.score, 0) / sites.length);
  const confidentBriefs = briefs.filter((brief) => brief.confidence !== "low").length;

  return [
    {
      label: "Priority Sites",
      value: String(sites.length),
      detail: "active properties or corridor segments under review",
      tone: "warm",
    },
    {
      label: "Live Agenda Signals",
      value: String(signals.length),
      detail: "recent Planning Board and ZBA postings from Danvers Agenda Center",
      tone: "cool",
    },
    {
      label: "Case Briefs",
      value: String(confidentBriefs),
      detail: "items with at least one stronger clue from packet text or address patterns",
    },
    {
      label: "Average Score",
      value: String(averageScore),
      detail: `${advancing} current watchlist items marked advancing`,
    },
  ];
}

class HeadingCollector {
  private active = false;
  private text = "";

  constructor(private onComplete: (value: string) => void) {}

  element(element: Element) {
    this.active = true;
    this.text = "";
    element.onEndTag(() => {
      this.active = false;
      this.onComplete(normalizeWhitespace(this.text));
      this.text = "";
    });
  }

  text(text: Text) {
    if (this.active) {
      this.text += text.text;
    }
  }
}
