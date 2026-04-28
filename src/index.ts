import {
  debugLookupParcelAddress,
  findParcelByAddress,
  listParcelReviewQueue,
  listParcelMatchesForParcelId,
  matchAndPersistOpportunities,
  normalizeAddress,
  upsertParcels,
  type ParcelLookupResult,
  type OpportunityParcelInput,
  type ParcelUpsertInput,
  type ParcelReviewItem,
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
  strategicBrief: StrategicBrief;
  reviewSummary: {
    total: number;
    matched: number;
    needsReview: number;
  };
}

interface StrategicInsight {
  eyebrow: string;
  title: string;
  detail: string;
}

interface StrategicRecommendation {
  action: string;
  whyItMatters: string;
}

interface StrategicBrief {
  generatedAt: string;
  trigger: "live" | IngestTrigger;
  title: string;
  summary: string;
  insights: StrategicInsight[];
  recommendations: StrategicRecommendation[];
  metrics: {
    briefingSignals: number;
    caseBriefs: number;
    matched: number;
    needsReview: number;
    assessedParcels?: number;
    businessZonedParcels?: number;
    floodConstrainedParcels?: number;
    olderBuildingStockParcels?: number;
    waterServedParcels?: number;
    sewerServedParcels?: number;
    externalServiceAreaParcels?: number;
    wetlandConstrainedParcels?: number;
    groundwaterConstrainedParcels?: number;
  };
  sourceCount: number;
}

const AGENDA_CENTER_URL = "https://www.danversma.gov/AgendaCenter";
const PLANNING_BOARD_AGENDA_URL = "https://www.danversma.gov/AgendaCenter/Planning-Board-11";
const ZBA_AGENDA_URL = "https://www.danversma.gov/AgendaCenter/Zoning-Board-of-Appeals-18";
const PLANNING_BOARD_RSS_URL =
  "https://www.danversma.gov/RSSFeed.aspx?CID=Planning-Board-11&ModID=65";
const ZBA_RSS_URL =
  "https://www.danversma.gov/RSSFeed.aspx?CID=Zoning-Board-of-Appeals-18&ModID=65";
const PROJECTS_PAGE_URL = "https://www.danversma.gov/235/Projects";
const DANVERS_PARCELS_LAYER_URL =
  "https://gis.danversma.gov/danversexternal/rest/services/DanversMA_Parcels_AGOL/MapServer/1/query";
const DANVERS_ASSESSOR_TABLE_URL =
  "https://gis.danversma.gov/danversexternal/rest/services/DanversMA_OpsLayers/MapServer/45/query";
const DANVERS_FIRM_LAYER_URL =
  "https://gis.danversma.gov/danversexternal/rest/services/DanversMA_OpsLayers/MapServer/44/query";
const DANVERS_WATER_PIPE_LAYER_URL =
  "https://gis.danversma.gov/danversexternal/rest/services/DanversMA_OpsLayers/MapServer/30/query";
const DANVERS_PEABODY_SEWER_CUSTOMERS_LAYER_URL =
  "https://gis.danversma.gov/danversexternal/rest/services/DanversMA_OpsLayers/MapServer/31/query";
const DANVERS_PEABODY_WATER_CUSTOMERS_LAYER_URL =
  "https://gis.danversma.gov/danversexternal/rest/services/DanversMA_OpsLayers/MapServer/32/query";
const DANVERS_GRAVITY_MAIN_LAYER_URL =
  "https://gis.danversma.gov/danversexternal/rest/services/DanversMA_OpsLayers/MapServer/37/query";
const DANVERS_FORCE_MAIN_LAYER_URL =
  "https://gis.danversma.gov/danversexternal/rest/services/DanversMA_OpsLayers/MapServer/38/query";
const MASSGIS_WETLANDS_LAYER_URL =
  "https://arcgisserver.digital.mass.gov/arcgisserver/rest/services/AGOL/DEP_Wetlands/FeatureServer/0/query";
const DANVERS_GROUNDWATER_PROTECTION_LAYER_URL =
  "https://gis.danversma.gov/danversexternal/rest/services/DanversMA_OpsLayers/MapServer/22/query";
const DANVERS_PARCELS_PAGE_SIZE = 1000;
const INGEST_PARCEL_SAMPLE_LIMIT = 250;

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

interface ArcGisFeature<TAttributes = Record<string, unknown>, TGeometry = Record<string, unknown>> {
  attributes?: TAttributes;
  geometry?: TGeometry;
}

interface ArcGisQueryResponse<TAttributes = Record<string, unknown>, TGeometry = Record<string, unknown>> {
  features?: Array<ArcGisFeature<TAttributes, TGeometry>>;
  exceededTransferLimit?: boolean;
}

interface ArcGisCountResponse {
  count?: number;
}

interface DanversAssessorAttributes {
  PROP_ID?: string | null;
  SITE_ADDR?: string | null;
  LOCATION?: string | null;
  FULL_LOCATION?: string | null;
  OWNER1?: string | null;
  OWN_CO?: string | null;
  ZONING?: string | null;
  TOTAL_VAL?: number | null;
  LAND_VAL?: number | null;
  BLDG_VAL?: number | null;
  LOT_SIZE?: number | null;
  YEAR_BUILT?: number | null;
  USE_CODE?: number | null;
  LOC_ID?: string | null;
}

interface DanversParcelGeometry {
  rings?: number[][][];
  spatialReference?: {
    wkid?: number;
    latestWkid?: number;
  };
}

interface DanversFloodAttributes {
  FLD_ZONE?: string | null;
  SFHA_TF?: string | null;
  STATIC_BFE?: number | null;
  DEPTH?: number | null;
}

interface ParcelContext {
  address: string;
  ownerName: string | null;
  zoning: string | null;
  totalValue: number | null;
  landValue: number | null;
  buildingValue: number | null;
  lotSize: number | null;
  yearBuilt: number | null;
  useCode: number | null;
  floodZones: string[];
  specialFloodHazard: boolean;
  hasMappedWaterAccess: boolean;
  hasMappedSewerAccess: boolean;
  externalWaterServiceArea: boolean;
  externalSewerServiceArea: boolean;
  intersectsWetlands: boolean;
  intersectsGroundwaterProtection: boolean;
}

interface ParcelDetailPayload {
  requestedAddress: string;
  parcel: ParcelLookupResult | null;
  context: ParcelContext | null;
  relatedMatches: ParcelReviewItem[];
  relatedBriefs: CaseBrief[];
  relatedSignals: AgendaSignal[];
}

interface WatchlistDetailPayload {
  site: OpportunitySite;
  relatedBriefs: CaseBrief[];
  relatedSignals: AgendaSignal[];
}

interface StrategicBriefRow {
  generated_at: string;
  trigger: string;
  title: string;
  summary: string;
  insights_json: string;
  recommendations_json: string;
  metrics_json: string;
  source_count: number;
}

interface IngestRunSummary {
  parcelsIngested: number;
  opportunitiesPrepared: number;
  matched: number;
  reviewNeeded: number;
}

interface AgendaSignalDebugResult {
  board: AgendaSignal["board"];
  url: string;
  ok: boolean;
  status?: number;
  error?: string;
  parsedCount: number;
  signals: AgendaSignal[];
  h3Count?: number;
  agendaHrefCount?: number;
  htmlSample?: string;
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
    title: "Parcel context now reaches beyond agenda text",
    detail: "Manual and scheduled runs now screen brief-linked parcels against Danvers assessor, utility, flood, wetlands, and groundwater layers before writing the strategic brief.",
  },
  {
    time: "09:15",
    title: "Parcel drilldowns are live",
    detail: "Staff can open parcel detail pages from case briefs to see ownership, zoning, value, utility context, environmental flags, and related matched opportunity records.",
  },
  {
    time: "Next",
    title: "Current build focus: connected site workflows",
    detail: "Next up is tightening watchlist drilldowns and making it easier to move between strategic briefs, parcel pages, and source records without losing context.",
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

const IGNORED_PACKET_ADDRESSES = new Set([
  "1 Sylvan Street",
  "1 Sylvan St",
]);

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
        queue: Boolean(env?.OPPORTUNITYDB),
        strategicBriefs: Boolean(env?.OPPORTUNITYDB),
        parcelContext: true,
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

function generateCspNonce(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  let value = "";
  for (const byte of bytes) {
    value += String.fromCharCode(byte);
  }
  return btoa(value);
}

function buildContentSecurityPolicy(nonce?: string): string {
  const styleSource = nonce ? `'self' 'nonce-${nonce}'` : "'self'";
  const scriptSource = nonce ? `'self' 'nonce-${nonce}'` : "'self'";
  return [
    "default-src 'self'",
    `style-src ${styleSource}`,
    `script-src ${scriptSource}`,
    "img-src 'self' data: https:",
    "connect-src 'self' https:",
    "object-src 'none'",
    "base-uri 'self'",
    "frame-ancestors 'self'",
  ].join("; ");
}

function withSecurityHeaders(response: Response, nonce?: string): Response {
  const headers = new Headers(response.headers);
  headers.set("content-security-policy", buildContentSecurityPolicy(nonce));
  headers.set("x-frame-options", "SAMEORIGIN");
  headers.set("x-content-type-options", "nosniff");
  headers.set("referrer-policy", "strict-origin-when-cross-origin");

  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
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
  const matches = text.match(/\b\d{1,5}\s+[A-Z][A-Za-z0-9.'-]*(?:\s+[A-Z][A-Za-z0-9.'-]*){0,4}\s+(?:Street|St|Road|Rd|Avenue|Ave|Lane|Ln|Drive|Dr|Boulevard|Blvd|Way|Court|Ct|Terrace|Ter|Place|Pl|Highway|Hwy|Circle|Cir)\b/gi) ?? [];
  const expanded = matches.flatMap((value) => {
    const normalized = normalizeWhitespace(value);
    const rangeMatch = normalized.match(/^(\d{1,5})-(\d{1,5})\s+(.+)$/);
    if (!rangeMatch) {
      return [normalized];
    }

    return [
      `${rangeMatch[1]} ${rangeMatch[3]}`,
      `${rangeMatch[2]} ${rangeMatch[3]}`,
    ];
  });
  const deduped = Array.from(
    new Set(
      expanded
        .filter((value) => !IGNORED_PACKET_ADDRESSES.has(value)),
    ),
  );
  return deduped.slice(0, 4);
}

function extractProjectTitleAddresses(title: string): string[] {
  const normalized = normalizeWhitespace(title);
  const rangeMatch = normalized.match(
    /^(\d{1,5})-(\d{1,5})\s+([A-Za-z0-9.'-]+(?:\s+[A-Za-z0-9.'-]+){0,4}\s+(?:Street|St|Road|Rd|Avenue|Ave|Lane|Ln|Drive|Dr|Boulevard|Blvd|Way|Court|Ct|Terrace|Ter|Place|Pl|Highway|Hwy|Circle|Cir))$/i,
  );

  if (rangeMatch) {
    return [`${rangeMatch[1]} ${rangeMatch[3]}`, `${rangeMatch[2]} ${rangeMatch[3]}`];
  }

  return extractAddresses(title);
}

function formatSourceLabel(source: string): string {
  if (source === "danvers projects page") {
    return "Projects Page";
  }
  if (source === "danvers agenda rss") {
    return "Agenda RSS";
  }
  return source;
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

class AgendaLinkCollector {
  private active = false;
  private href = "";
  private text = "";

  constructor(
    private getBoard: () => string | null,
    private getMeetingDate: () => string | null,
    private onSignal: (signal: AgendaSignal) => void,
  ) {}

  element(element: Element) {
    const href = element.getAttribute("href");
    if (!href || !href.includes("/AgendaCenter/ViewFile/Agenda/")) {
      return;
    }

    this.active = true;
    this.href = href.startsWith("http") ? href : `https://www.danversma.gov${href}`;
    this.text = "";

    element.onEndTag(() => {
      this.active = false;
      const board = this.getBoard();
      const meetingDate = this.getMeetingDate();
      const title = normalizeWhitespace(this.text);

      if (!board || !meetingDate || !title || title === "Agenda" || title === "Previous Versions") {
        this.href = "";
        this.text = "";
        return;
      }

      if (board !== "Planning Board" && board !== "Zoning Board of Appeals") {
        this.href = "";
        this.text = "";
        return;
      }

      this.onSignal({
        board,
        meetingDate,
        title,
        agendaUrl: this.href,
        source: "danvers agenda center",
      });

      this.href = "";
      this.text = "";
    });
  }

  text(text: Text) {
    if (this.active) {
      this.text += text.text;
    }
  }
}

async function fetchAgendaSignalsForBoard(
  board: AgendaSignal["board"],
  url: string,
): Promise<AgendaSignal[]> {
  const result = await fetchAgendaSignalsForBoardWithDebug(board, url);
  return result.signals;
}

function extractRssTag(itemXml: string, tagName: string): string | null {
  const match = itemXml.match(new RegExp(`<${tagName}>([\\s\\S]*?)<\\/${tagName}>`, "i"));
  if (!match) {
    return null;
  }

  return normalizeWhitespace(match[1].replace(/<!\[CDATA\[([\s\S]*?)\]\]>/i, "$1"));
}

function formatRssPubDate(value: string): string {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return normalizeWhitespace(value);
  }

  return parsed.toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
    timeZone: "America/New_York",
  });
}

function looksLikeProjectTitle(value: string): boolean {
  const normalized = normalizeWhitespace(value);
  if (!normalized) {
    return false;
  }

  if (normalized.length < 6 || normalized.length > 120) {
    return false;
  }

  const lowered = normalized.toLowerCase();
  if (
    lowered === "projects" ||
    lowered === "meeting schedule and filing fees" ||
    lowered.includes("application") ||
    lowered.includes("floor plan") ||
    lowered.includes("floor plans") ||
    lowered.includes("elevation") ||
    lowered.includes("elevations") ||
    lowered.includes("detail") ||
    lowered.includes("details") ||
    lowered.includes("memo") ||
    lowered.includes("report") ||
    lowered.includes("appendix") ||
    lowered.includes("rendering") ||
    lowered.includes("renderings") ||
    lowered.includes("exhibit") ||
    lowered.includes("cut sheet") ||
    lowered.includes("cut sheets") ||
    lowered.includes("site plan") ||
    lowered.includes("narrative") ||
    lowered.includes("traffic") ||
    lowered.includes("lighting") ||
    lowered.includes("architectural") ||
    lowered.includes("stormwater") ||
    lowered.includes("special permit") ||
    lowered.includes("project page")
  ) {
    return false;
  }

  return /\d/.test(normalized);
}

async function fetchProjectPageSignals(): Promise<AgendaSignal[]> {
  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 10000);
    const response = await fetch(PROJECTS_PAGE_URL, {
      signal: controller.signal,
      headers: {
        "user-agent": "Opportunity/0.1 (+https://www.danversma.gov/)",
      },
    });
    clearTimeout(timeout);

    if (!response.ok) {
      return [];
    }

    const html = await response.text();
    const titles = Array.from(html.matchAll(/<a\b[^>]*>([\s\S]*?)<\/a>/gi))
      .map((match) => normalizeWhitespace(match[1].replace(/<[^>]+>/g, " ")))
      .filter(looksLikeProjectTitle);

    const uniqueTitles = Array.from(new Set(titles)).slice(0, 10);
    return uniqueTitles.map((title) => ({
      board: "Planning Board",
      meetingDate: new Date().toLocaleDateString("en-US", {
        month: "short",
        day: "numeric",
        year: "numeric",
        timeZone: "America/New_York",
      }),
      title,
      agendaUrl: PROJECTS_PAGE_URL,
      source: "danvers projects page",
    }));
  } catch {
    return [];
  }
}

async function fetchAgendaSignalsForBoardWithDebug(
  board: AgendaSignal["board"],
  url: string,
): Promise<AgendaSignalDebugResult> {
  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 10000);
    const response = await fetch(url, {
      signal: controller.signal,
      headers: {
        "user-agent": "Opportunity/0.1 (+https://www.danversma.gov/)",
      },
    });
    clearTimeout(timeout);

    if (!response.ok) {
      return {
        board,
        url,
        ok: false,
        status: response.status,
        error: `Agenda Center request failed with ${response.status}`,
        parsedCount: 0,
        signals: [],
      };
    }

    const xml = await response.text();
    const signals: AgendaSignal[] = [];
    const seen = new Set<string>();
    const items = Array.from(xml.matchAll(/<item>([\s\S]*?)<\/item>/gi));

    for (const [, itemXml] of items) {
      const title = extractRssTag(itemXml, "title") ?? board;
      const agendaUrl = extractRssTag(itemXml, "link");
      const pubDate = extractRssTag(itemXml, "pubDate");
      const description = extractRssTag(itemXml, "description") ?? "";

      if (!agendaUrl || !pubDate) {
        continue;
      }

      if (title.toLowerCase().includes("cancel")) {
        continue;
      }

      if (description.toLowerCase().includes("minutes added or updated")) {
        continue;
      }

      const meetingDate = formatRssPubDate(pubDate);
      const key = `${board}|${meetingDate}|${agendaUrl}`;
      if (seen.has(key)) {
        continue;
      }

      seen.add(key);
      signals.push({
        board,
        meetingDate,
        title,
        agendaUrl,
        source: "danvers agenda rss",
      });
    }

    return {
      board,
      url,
      ok: true,
      status: response.status,
      parsedCount: signals.length,
      signals: signals.slice(0, 8),
      h3Count: items.length,
      agendaHrefCount: items.length,
      htmlSample: xml.slice(0, 1200),
    };
  } catch (error) {
    return {
      board,
      url,
      ok: false,
      error: error instanceof Error ? error.message : "Unknown fetch failure",
      parsedCount: 0,
      signals: [],
    };
  }
}

async function fetchAgendaSignals(): Promise<AgendaSignal[]> {
  const [planningSignals, zbaSignals, projectSignals] = await Promise.all([
    fetchAgendaSignalsForBoard("Planning Board", PLANNING_BOARD_RSS_URL),
    fetchAgendaSignalsForBoard("Zoning Board of Appeals", ZBA_RSS_URL),
    fetchProjectPageSignals(),
  ]);

  const merged = [...projectSignals, ...planningSignals, ...zbaSignals];
  const deduped: AgendaSignal[] = [];
  const seen = new Set<string>();

  for (const signal of merged) {
    const key = `${signal.board}|${signal.title}|${signal.agendaUrl}`;
    if (seen.has(key)) {
      continue;
    }

    seen.add(key);
    deduped.push(signal);
  }

  const signals = deduped.slice(0, 12);

  return signals.length ? signals : FALLBACK_SIGNALS;
}

async function fetchAgendaSignalsDebug() {
  const [planningBoard, zoningBoard] = await Promise.all([
    fetchAgendaSignalsForBoardWithDebug("Planning Board", PLANNING_BOARD_RSS_URL),
    fetchAgendaSignalsForBoardWithDebug("Zoning Board of Appeals", ZBA_RSS_URL),
  ]);

  const combined = [...planningBoard.signals, ...zoningBoard.signals]
    .sort((left, right) => right.meetingDate.localeCompare(left.meetingDate))
    .slice(0, 8);

  return {
    fallbackUsed: combined.length === 0,
    planningBoard,
    zoningBoard,
    combinedSignals: combined.length ? combined : FALLBACK_SIGNALS,
  };
}

function isPdfDocument(bytes: Uint8Array, contentType: string | null): boolean {
  if (contentType?.toLowerCase().includes("pdf")) {
    return true;
  }

  if (bytes.length < 4) {
    return false;
  }

  return (
    bytes[0] === 0x25 &&
    bytes[1] === 0x50 &&
    bytes[2] === 0x44 &&
    bytes[3] === 0x46
  );
}

function findSequence(bytes: Uint8Array, sequence: number[], startAt = 0): number {
  outer: for (let index = startAt; index <= bytes.length - sequence.length; index += 1) {
    for (let offset = 0; offset < sequence.length; offset += 1) {
      if (bytes[index + offset] !== sequence[offset]) {
        continue outer;
      }
    }

    return index;
  }

  return -1;
}

function stripStreamBoundaryWhitespace(bytes: Uint8Array): Uint8Array {
  let start = 0;
  let end = bytes.length;

  while (start < end && (bytes[start] === 0x0a || bytes[start] === 0x0d)) {
    start += 1;
  }

  while (end > start && (bytes[end - 1] === 0x0a || bytes[end - 1] === 0x0d)) {
    end -= 1;
  }

  return bytes.slice(start, end);
}

async function inflatePdfStream(bytes: Uint8Array): Promise<string> {
  try {
    const stream = new Blob([bytes]).stream().pipeThrough(new DecompressionStream("deflate"));
    return await new Response(stream).text();
  } catch {
    return "";
  }
}

function decodePdfLiteralString(value: string): string {
  let decoded = "";

  for (let index = 0; index < value.length; index += 1) {
    const character = value[index];

    if (character !== "\\") {
      decoded += character;
      continue;
    }

    const next = value[index + 1];

    if (!next) {
      break;
    }

    if (/[0-7]/.test(next)) {
      const octal = value.slice(index + 1, index + 4).match(/^[0-7]{1,3}/)?.[0] ?? next;
      decoded += String.fromCharCode(Number.parseInt(octal, 8));
      index += octal.length;
      continue;
    }

    const replacements: Record<string, string> = {
      n: "\n",
      r: "\r",
      t: "\t",
      b: "\b",
      f: "\f",
      "(": "(",
      ")": ")",
      "\\": "\\",
    };

    decoded += replacements[next] ?? next;
    index += 1;
  }

  return decoded;
}

function decodePdfHexString(value: string): string {
  const normalized = value.replace(/[^0-9a-f]/gi, "");
  const padded = normalized.length % 2 === 0 ? normalized : `${normalized}0`;
  let decoded = "";

  for (let index = 0; index < padded.length; index += 2) {
    decoded += String.fromCharCode(Number.parseInt(padded.slice(index, index + 2), 16));
  }

  return decoded;
}

function extractPdfStrings(text: string): string[] {
  const extracted: string[] = [];
  const literalPattern = /\((?:\\.|[^()\\])*\)/g;
  const hexPattern = /<([0-9a-fA-F\s]+)>/g;

  for (const match of text.matchAll(literalPattern)) {
    const value = match[0].slice(1, -1);
    const decoded = normalizeWhitespace(decodePdfLiteralString(value));
    if (decoded) {
      extracted.push(decoded);
    }
  }

  for (const match of text.matchAll(hexPattern)) {
    const decoded = normalizeWhitespace(decodePdfHexString(match[1]));
    if (decoded) {
      extracted.push(decoded);
    }
  }

  return extracted;
}

async function extractPdfText(bytes: Uint8Array): Promise<string> {
  const rawText = new TextDecoder("latin1").decode(bytes);
  const streamTexts: string[] = [];
  let searchIndex = 0;

  while (searchIndex < bytes.length) {
    const streamIndex = findSequence(bytes, [0x73, 0x74, 0x72, 0x65, 0x61, 0x6d], searchIndex);
    if (streamIndex === -1) {
      break;
    }

    const contentStart = streamIndex + 6;
    const endstreamIndex = findSequence(
      bytes,
      [0x65, 0x6e, 0x64, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d],
      contentStart,
    );

    if (endstreamIndex === -1) {
      break;
    }

    const rawStream = stripStreamBoundaryWhitespace(bytes.slice(contentStart, endstreamIndex));
    const inflated = await inflatePdfStream(rawStream);
    if (inflated) {
      streamTexts.push(inflated);
    }

    searchIndex = endstreamIndex + 9;
  }

  const extracted = extractPdfStrings([rawText, ...streamTexts].join("\n"));
  return normalizeWhitespace(extracted.join(" "));
}

async function fetchPacketText(url: string): Promise<string> {
  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 4000);
    const response = await fetch(url, {
      signal: controller.signal,
      headers: {
        "user-agent": "Opportunity/0.1 (+https://www.danversma.gov/)",
      },
    });
    clearTimeout(timeout);

    if (!response.ok) {
      return "";
    }

    const bytes = new Uint8Array(await response.arrayBuffer());
    if (isPdfDocument(bytes, response.headers.get("content-type"))) {
      return await extractPdfText(bytes);
    }

    const decoded = new TextDecoder("latin1").decode(bytes);
    return normalizeWhitespace(decoded);
  } catch {
    return "";
  }
}

async function buildCaseBriefs(signals: AgendaSignal[]): Promise<CaseBrief[]> {
  const briefs = await Promise.all(
    signals.slice(0, 6).map(async (signal) => {
      const titleAddresses =
        signal.source === "danvers projects page"
          ? extractProjectTitleAddresses(signal.title)
          : extractAddresses(signal.title);
      const packetText = signal.source === "danvers projects page" ? "" : await fetchPacketText(signal.agendaUrl);
      const packetAddresses = packetText ? extractAddresses(packetText) : [];
      const addresses = titleAddresses.length ? titleAddresses : packetAddresses;
      const likelySite =
        signal.source === "danvers projects page" && titleAddresses.length > 1
          ? signal.title
          : addresses[0] ?? `${signal.board} agenda item`;
      const titleAndPacket = normalizeWhitespace(`${signal.title} ${packetText}`);

      return {
        id: slugify(`${signal.board}-${signal.meetingDate}-${signal.title}`),
        board: signal.board,
        meetingDate: signal.meetingDate,
        title: signal.title,
        agendaUrl: signal.agendaUrl,
        likelySite,
        addresses,
        signalType: formatSignalType(titleAndPacket),
        rationale: inferRationale(signal, addresses, packetText),
        confidence: inferConfidence(addresses, packetText),
        source: signal.source,
      };
    }),
  );

  return briefs;
}

async function fetchDashboardReviewSummary(db?: D1Database): Promise<DashboardPayload["reviewSummary"]> {
  if (!db) {
    return { total: 0, matched: 0, needsReview: 0 };
  }

  const totalRow = await db
    .prepare(`SELECT COUNT(*) AS count FROM opportunity_parcel_matches`)
    .first<{ count: number }>();
  const matchedRow = await db
    .prepare(`SELECT COUNT(*) AS count FROM opportunity_parcel_matches WHERE match_type != 'no_match'`)
    .first<{ count: number }>();
  const reviewRow = await db
    .prepare(`SELECT COUNT(*) AS count FROM opportunity_parcel_matches WHERE needs_review = 1`)
    .first<{ count: number }>();

  return {
    total: Number(totalRow?.count ?? 0),
    matched: Number(matchedRow?.count ?? 0),
    needsReview: Number(reviewRow?.count ?? 0),
  };
}

function buildStrategicBrief(
  generatedAt: string,
  trigger: StrategicBrief["trigger"],
  sites: OpportunitySite[],
  signals: AgendaSignal[],
  briefs: CaseBrief[],
  reviewSummary: DashboardPayload["reviewSummary"],
  parcelContexts: ParcelContext[] = [],
): StrategicBrief {
  const planningBoardBriefs = briefs.filter((brief) => brief.board === "Planning Board").length;
  const zbaBriefs = briefs.filter((brief) => brief.board === "Zoning Board of Appeals").length;
  const confidentBriefs = briefs.filter((brief) => brief.confidence !== "low").length;
  const advancingSites = sites.filter((site) => site.readiness === "Advancing").length;
  const averageScore = Math.round(sites.reduce((sum, site) => sum + site.score, 0) / sites.length);
  const reviewShare = reviewSummary.total
    ? Math.round((reviewSummary.needsReview / reviewSummary.total) * 100)
    : 0;
  const businessZonedParcels = parcelContexts.filter((context) => isBusinessZoning(context.zoning)).length;
  const floodConstrainedParcels = parcelContexts.filter((context) => context.specialFloodHazard).length;
  const olderBuildingStockParcels = parcelContexts.filter((context) => isOlderBuildingStock(context)).length;
  const underbuiltBusinessParcels = parcelContexts.filter(
    (context) => isBusinessZoning(context.zoning) && isLikelyUnderbuiltParcel(context),
  ).length;
  const waterServedParcels = parcelContexts.filter((context) => context.hasMappedWaterAccess).length;
  const sewerServedParcels = parcelContexts.filter((context) => context.hasMappedSewerAccess).length;
  const externalServiceAreaParcels = parcelContexts.filter(
    (context) => context.externalWaterServiceArea || context.externalSewerServiceArea,
  ).length;
  const wetlandConstrainedParcels = parcelContexts.filter((context) => context.intersectsWetlands).length;
  const groundwaterConstrainedParcels = parcelContexts.filter(
    (context) => context.intersectsGroundwaterProtection,
  ).length;

  let boardTitle = "Board activity is split across both review lanes";
  let boardDetail =
    "Planning Board and ZBA postings are landing at a similar pace, so Danvers should keep watching both formal development review and use-relief activity.";

  if (planningBoardBriefs > zbaBriefs) {
    boardTitle = "Planning Board is setting the near-term development pipeline";
    boardDetail = `${planningBoardBriefs} of ${briefs.length} current briefs come from Planning Board materials, which suggests the strongest near-term signals are tied to formal site planning, subdivision, or project review.`;
  } else if (zbaBriefs > planningBoardBriefs) {
    boardTitle = "ZBA filings are surfacing the most immediate site friction";
    boardDetail = `${zbaBriefs} of ${briefs.length} current briefs come from ZBA materials, pointing to a heavier mix of variance, use, and site-constraint questions that may need staff attention before broader redevelopment can move.`;
  }

  const queueTitle =
    reviewSummary.needsReview > 0
      ? "Staff review capacity is still shaping how fast leads become usable"
      : "The parcel review queue is currently under control";
  const queueDetail =
    reviewSummary.total > 0
      ? `${reviewSummary.needsReview} of ${reviewSummary.total} parcel-linked records still need staff review. Clearing ambiguous matches should improve how quickly Danvers can turn agenda signals into actionable property-level follow-up.`
      : "Parcel-linked review records are not yet built up in the database, so the next value comes from continuing ingest and building a more complete site-level queue.";

  const postureTitle =
    parcelContexts.length && businessZonedParcels > 0
      ? "Brief-linked parcels are starting to show a real business-location pattern"
      : advancingSites >= 2
        ? "Several tracked Danvers sites are moving beyond early watchlist status"
        : "Most tracked opportunities are still in an early-read posture";
  const postureDetail =
    parcelContexts.length && businessZonedParcels > 0
      ? `${businessZonedParcels} of ${parcelContexts.length} brief-linked parcels fall in business-oriented zoning, ${olderBuildingStockParcels} show older building stock, and ${underbuiltBusinessParcels} look potentially underbuilt based on land-versus-building value. That is a stronger redevelopment screen than agenda text alone.`
      : confidentBriefs > 0
        ? `${advancingSites} sites are marked advancing, the average watchlist score is ${averageScore}, and ${confidentBriefs} briefs now include medium or high confidence clues. Danvers can start prioritizing corridor-specific response, not just broad monitoring.`
        : `${advancingSites} sites are marked advancing, but the current extraction layer still produces limited high-confidence clues. Better source coverage and parcel validation should come before heavier policy moves.`;

  const constraintInsightTitle =
    floodConstrainedParcels > 0 || wetlandConstrainedParcels > 0 || groundwaterConstrainedParcels > 0
      ? "Some active leads also show clear physical constraints"
      : "The current lead set does not yet show major flood-, wetlands-, or groundwater-driven screening pressure";
  const constraintInsightDetail =
    floodConstrainedParcels > 0 || wetlandConstrainedParcels > 0 || groundwaterConstrainedParcels > 0
      ? `${floodConstrainedParcels} brief-linked parcels intersect special flood hazard areas, ${wetlandConstrainedParcels} intersect mapped wetlands, and ${groundwaterConstrainedParcels} fall within groundwater protection areas. Those sites may still matter, but they should be treated as higher-friction redevelopment candidates needing earlier diligence.`
      : parcelContexts.length
        ? "The first parcel-context pass did not flag major flood, wetlands, or groundwater screening pressure for the assessed lead set, which improves the odds that the current queue contains workable follow-up candidates."
        : "Flood, wetlands, groundwater, and assessor screening have not yet returned parcel context for the current queue, so constraints still need to be checked case by case.";

  const utilityInsightTitle =
    waterServedParcels > 0 || sewerServedParcels > 0
      ? "Mapped utility context is starting to separate easier sites from harder ones"
      : "Utility readiness is still mostly unconfirmed for the current queue";
  const utilityInsightDetail =
    waterServedParcels > 0 || sewerServedParcels > 0
      ? `${waterServedParcels} brief-linked parcels intersect mapped water infrastructure or service areas, ${sewerServedParcels} intersect mapped sewer infrastructure or service areas, and ${externalServiceAreaParcels} fall in mapped Peabody customer areas. That gives Danvers an early site-readiness read before engineering review.`
      : parcelContexts.length
        ? "The first utility pass did not find mapped water or sewer context for the current assessed leads, which means service readiness still needs manual follow-up."
        : "Utility screening has not yet returned parcel context for the current queue.";

  const recommendations: StrategicRecommendation[] = [
    {
      action:
        reviewSummary.needsReview > 0
          ? "Clear the highest-confidence review queue first."
          : "Keep building parcel-linked coverage from live board activity.",
      whyItMatters:
        reviewSummary.needsReview > 0
          ? "That is the fastest way to convert live postings into specific sites, owners, and follow-up candidates for staff."
          : "A larger parcel-linked record set will make future recommendations more specific and more defensible.",
    },
    {
      action:
        planningBoardBriefs >= zbaBriefs
          ? "Track Planning Board items as the main near-term development pipeline."
          : "Track ZBA items as the clearest sign of near-term site friction and adaptation.",
      whyItMatters:
        planningBoardBriefs >= zbaBriefs
          ? "Those filings are most likely to signal commercial expansion, redevelopment timing, and infrastructure questions early enough for Town response."
          : "Those filings can reveal where zoning, site constraints, or reuse issues are slowing investment before projects mature.",
    },
    {
      action:
        businessZonedParcels > 0
          ? "Use business-zoned brief-linked parcels as the first redevelopment follow-up list."
          : "Use the advancing watchlist to focus business-retention and redevelopment follow-up.",
      whyItMatters:
        businessZonedParcels > 0
          ? "That shortlist now has parcel context behind it, which makes staff outreach and internal coordination more targeted and more defensible."
          : "The combination of site readiness, case-brief confidence, and corridor context gives Danvers a practical shortlist for staff outreach and internal coordination.",
    },
    {
      action:
        floodConstrainedParcels > 0 || wetlandConstrainedParcels > 0 || groundwaterConstrainedParcels > 0
          ? "Separate flood-, wetlands-, and groundwater-constrained leads from easier near-term candidates."
          : "Keep adding parcel screening so constraints can be ruled in or out earlier.",
      whyItMatters:
        floodConstrainedParcels > 0 || wetlandConstrainedParcels > 0 || groundwaterConstrainedParcels > 0
          ? "That prevents the Town from spending the same level of attention on straightforward sites and more complex resilience- or permitting-heavy sites."
          : "The next jump in recommendation quality will come from consistently screening physical and regulatory constraints before staff time is spent.",
    },
    {
      action:
        waterServedParcels > 0 || sewerServedParcels > 0
          ? "Prioritize parcels with mapped utility context for the first site-readiness shortlist."
          : "Add more utility validation before treating current leads as truly site-ready.",
      whyItMatters:
        waterServedParcels > 0 || sewerServedParcels > 0
          ? "Those parcels are more likely to move faster from policy interest to realistic development conversations because service context is already partly visible."
          : "Economic-development recommendations are much stronger when they distinguish promising sites from sites that still need basic infrastructure confirmation.",
    },
  ];

  return {
    generatedAt,
    trigger,
    title: "Danvers Strategic Brief",
    summary: `This briefing reflects ${signals.length} live agenda signals, ${briefs.length} case briefs, ${reviewSummary.matched} parcel-linked opportunities, and ${parcelContexts.length} parcel-context checks from Danvers assessor and flood data. It is intended as decision support for Danvers economic development follow-up.`,
    insights: [
      {
        eyebrow: "Strategic Insight",
        title: boardTitle,
        detail: boardDetail,
      },
      {
        eyebrow: reviewShare > 50 ? "Operational Pressure" : "Review Queue",
        title: queueTitle,
        detail: queueDetail,
      },
      {
        eyebrow: "Danvers Posture",
        title: postureTitle,
        detail: postureDetail,
      },
      {
        eyebrow: "Constraints",
        title: constraintInsightTitle,
        detail: constraintInsightDetail,
      },
      {
        eyebrow: "Utility Readiness",
        title: utilityInsightTitle,
        detail: utilityInsightDetail,
      },
    ],
    recommendations,
    metrics: {
      briefingSignals: signals.length,
      caseBriefs: briefs.length,
      matched: reviewSummary.matched,
      needsReview: reviewSummary.needsReview,
      assessedParcels: parcelContexts.length,
      businessZonedParcels,
      floodConstrainedParcels,
      olderBuildingStockParcels,
      waterServedParcels,
      sewerServedParcels,
      externalServiceAreaParcels,
      wetlandConstrainedParcels,
      groundwaterConstrainedParcels,
    },
    sourceCount: signals.length + briefs.length + parcelContexts.length,
  };
}

function mapStrategicBriefRow(row: StrategicBriefRow | null): StrategicBrief | null {
  if (!row) {
    return null;
  }

  try {
    return {
      generatedAt: row.generated_at,
      trigger: row.trigger === "manual" || row.trigger === "scheduled" ? row.trigger : "live",
      title: row.title,
      summary: row.summary,
      insights: JSON.parse(row.insights_json) as StrategicInsight[],
      recommendations: JSON.parse(row.recommendations_json) as StrategicRecommendation[],
      metrics: JSON.parse(row.metrics_json) as StrategicBrief["metrics"],
      sourceCount: Number(row.source_count ?? 0),
    };
  } catch {
    return null;
  }
}

async function fetchLatestStrategicBrief(db?: D1Database): Promise<StrategicBrief | null> {
  if (!db) {
    return null;
  }

  try {
    const row = await db
      .prepare(
        `
        SELECT generated_at, trigger, title, summary, insights_json, recommendations_json, metrics_json, source_count
        FROM strategic_briefs
        ORDER BY generated_at DESC
        LIMIT 1
        `,
      )
      .first<StrategicBriefRow>();

    return mapStrategicBriefRow(row ?? null);
  } catch {
    return null;
  }
}

async function listStrategicBriefs(db?: D1Database, limit = 10): Promise<StrategicBrief[]> {
  if (!db) {
    return [];
  }

  const safeLimit = Math.max(1, Math.min(limit, 25));
  try {
    const result = await db
      .prepare(
        `
        SELECT generated_at, trigger, title, summary, insights_json, recommendations_json, metrics_json, source_count
        FROM strategic_briefs
        ORDER BY generated_at DESC
        LIMIT ?
        `,
      )
      .bind(safeLimit)
      .all<StrategicBriefRow>();

    return (result.results ?? [])
      .map((row) => mapStrategicBriefRow(row))
      .filter((row): row is StrategicBrief => Boolean(row));
  } catch {
    return [];
  }
}

async function persistStrategicBrief(db: D1Database, brief: StrategicBrief): Promise<void> {
  try {
    await db
      .prepare(
        `
        INSERT INTO strategic_briefs (
          generated_at,
          trigger,
          title,
          summary,
          insights_json,
          recommendations_json,
          metrics_json,
          source_count
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        `,
      )
      .bind(
        brief.generatedAt,
        brief.trigger,
        brief.title,
        brief.summary,
        JSON.stringify(brief.insights),
        JSON.stringify(brief.recommendations),
        JSON.stringify(brief.metrics),
        brief.sourceCount,
      )
      .run();
  } catch {
    return;
  }
}

async function createAndPersistStrategicBrief(
  db: D1Database,
  trigger: IngestTrigger,
): Promise<StrategicBrief> {
  const signals = await fetchAgendaSignals();
  const briefs = await buildCaseBriefs(signals);
  const reviewSummary = await fetchDashboardReviewSummary(db);
  const parcelContexts = await fetchParcelContextForAddresses(
    Array.from(new Set(briefs.flatMap((brief) => brief.addresses))),
  );
  const brief = buildStrategicBrief(
    new Date().toISOString(),
    trigger,
    SITES,
    signals,
    briefs,
    reviewSummary,
    parcelContexts,
  );
  await persistStrategicBrief(db, brief);
  return brief;
}

function buildDanversParcelQueryUrl(resultOffset: number): string {
  const params = new URLSearchParams({
    where: "1=1",
    returnGeometry: "false",
    outFields: "MAP_PAR_ID,GIS_ID,Location,StreetName,LUCDescription,YearBuilt",
    orderByFields: "OBJECTID ASC",
    resultOffset: String(resultOffset),
    resultRecordCount: String(DANVERS_PARCELS_PAGE_SIZE),
    f: "json",
  });

  return `${DANVERS_PARCELS_LAYER_URL}?${params.toString()}`;
}

function escapeArcGisWhereLiteral(value: string): string {
  return value.replaceAll("'", "''");
}

function toTitleCase(value: string): string {
  return value
    .toLowerCase()
    .split(" ")
    .map((token) => (token ? `${token[0].toUpperCase()}${token.slice(1)}` : token))
    .join(" ");
}

function buildArcGisAddressVariants(address: string): string[] {
  return Array.from(
    new Set([
      normalizeWhitespace(address),
      normalizeWhitespace(address).toUpperCase(),
      toTitleCase(normalizeWhitespace(address)),
    ].filter(Boolean)),
  );
}

function buildDanversParcelWhereClause(address: string): string {
  const variants = buildArcGisAddressVariants(address);

  return variants
    .map((value) => {
      const escaped = escapeArcGisWhereLiteral(value);
      return `(Location = '${escaped}' OR StreetName = '${escaped}')`;
    })
    .join(" OR ");
}

function buildDanversParcelAddressQueryUrl(address: string): string {
  const where = buildDanversParcelWhereClause(address);
  const params = new URLSearchParams({
    where,
    returnGeometry: "false",
    outFields: "MAP_PAR_ID,GIS_ID,Location,StreetName,LUCDescription,YearBuilt",
    f: "json",
  });

  return `${DANVERS_PARCELS_LAYER_URL}?${params.toString()}`;
}

function buildDanversAssessorQueryUrl(address: string): string {
  const variants = buildArcGisAddressVariants(address);

  const where = variants
    .map((value) => {
      const escaped = escapeArcGisWhereLiteral(value);
      return `(SITE_ADDR = '${escaped}' OR LOCATION = '${escaped}' OR FULL_LOCATION = '${escaped}')`;
    })
    .join(" OR ");

  const params = new URLSearchParams({
    where,
    returnGeometry: "false",
    outFields:
      "PROP_ID,SITE_ADDR,LOCATION,FULL_LOCATION,OWNER1,OWN_CO,ZONING,TOTAL_VAL,LAND_VAL,BLDG_VAL,LOT_SIZE,YEAR_BUILT,USE_CODE,LOC_ID",
    f: "json",
  });

  return `${DANVERS_ASSESSOR_TABLE_URL}?${params.toString()}`;
}

function buildDanversParcelGeometryQueryUrl(address: string): string {
  const params = new URLSearchParams({
    where: buildDanversParcelWhereClause(address),
    returnGeometry: "true",
    outFields: "MAP_PAR_ID,Location,StreetName",
    outSR: "2249",
    f: "json",
  });

  return `${DANVERS_PARCELS_LAYER_URL}?${params.toString()}`;
}

function buildDanversFloodQueryUrl(geometry: DanversParcelGeometry): string {
  const params = new URLSearchParams({
    geometry: JSON.stringify(geometry),
    geometryType: "esriGeometryPolygon",
    inSR: String(geometry.spatialReference?.latestWkid ?? geometry.spatialReference?.wkid ?? 2249),
    spatialRel: "esriSpatialRelIntersects",
    returnGeometry: "false",
    outFields: "FLD_ZONE,SFHA_TF,STATIC_BFE,DEPTH",
    f: "json",
  });

  return `${DANVERS_FIRM_LAYER_URL}?${params.toString()}`;
}

function buildArcGisGeometryCountQueryUrl(layerUrl: string, geometry: DanversParcelGeometry): string {
  const params = new URLSearchParams({
    geometry: JSON.stringify(geometry),
    geometryType: "esriGeometryPolygon",
    inSR: String(geometry.spatialReference?.latestWkid ?? geometry.spatialReference?.wkid ?? 2249),
    spatialRel: "esriSpatialRelIntersects",
    returnCountOnly: "true",
    f: "json",
  });

  return `${layerUrl}?${params.toString()}`;
}

function buildParcelAliases(attributes: DanversParcelAttributes): ParcelUpsertInput["aliases"] {
  const aliases: NonNullable<ParcelUpsertInput["aliases"]> = [];

  if (attributes.GIS_ID) {
    aliases.push({ type: "map_lot", value: attributes.GIS_ID, confidence: 0.9 });
  }

  if (attributes.StreetName && attributes.Location && attributes.Location !== attributes.StreetName) {
    aliases.push({ type: "address", value: attributes.StreetName, confidence: 0.5 });
  }

  return aliases.length ? aliases : undefined;
}

function mapDanversParcelFeatureToInput(feature: DanversParcelFeature): ParcelUpsertInput | null {
  const attributes = feature.attributes;
  const mapLot = attributes?.MAP_PAR_ID?.trim();

  if (!mapLot) {
    return null;
  }

  return {
    mapLot,
    address: attributes.Location?.trim() || attributes.StreetName?.trim() || null,
    aliases: buildParcelAliases(attributes),
  };
}

async function fetchDanversParcels(priorityAddresses: string[] = []): Promise<ParcelUpsertInput[]> {
  const parcels: ParcelUpsertInput[] = [];
  const seenMapLots = new Set<string>();

  for (const address of priorityAddresses) {
    const payload = await fetchJson<DanversParcelQueryResponse>(
      buildDanversParcelAddressQueryUrl(address),
      15000,
    );

    for (const feature of payload.features ?? []) {
      const parcel = mapDanversParcelFeatureToInput(feature);
      if (!parcel || seenMapLots.has(parcel.mapLot)) {
        continue;
      }

      seenMapLots.add(parcel.mapLot);
      parcels.push(parcel);

      if (parcels.length >= INGEST_PARCEL_SAMPLE_LIMIT) {
        return parcels;
      }
    }
  }

  let resultOffset = 0;

  while (true) {
    const payload = await fetchJson<DanversParcelQueryResponse>(
      buildDanversParcelQueryUrl(resultOffset),
      15000,
    );

    const features = payload.features ?? [];
    if (!features.length) {
      break;
    }

    for (const feature of features) {
      const parcel = mapDanversParcelFeatureToInput(feature);
      if (!parcel || seenMapLots.has(parcel.mapLot)) {
        continue;
      }

      seenMapLots.add(parcel.mapLot);
      parcels.push(parcel);

      if (parcels.length >= INGEST_PARCEL_SAMPLE_LIMIT) {
        return parcels;
      }
    }

    if (!payload.exceededTransferLimit && features.length < DANVERS_PARCELS_PAGE_SIZE) {
      break;
    }

    resultOffset += DANVERS_PARCELS_PAGE_SIZE;
  }

  return parcels;
}

function normalizeParcelContextAddress(attributes: DanversAssessorAttributes): string | null {
  return attributes.SITE_ADDR?.trim()
    || attributes.LOCATION?.trim()
    || attributes.FULL_LOCATION?.trim()
    || null;
}

function normalizeFloodZoneLabel(value?: string | null): string | null {
  const normalized = normalizeWhitespace(value ?? "");
  return normalized || null;
}

function isBusinessZoning(zoning?: string | null): boolean {
  const normalized = (zoning ?? "").trim().toUpperCase();
  return normalized.startsWith("C")
    || normalized.startsWith("I")
    || normalized === "HC"
    || normalized === "HCD"
    || normalized === "VIL";
}

function isOlderBuildingStock(context: ParcelContext): boolean {
  return typeof context.yearBuilt === "number" && context.yearBuilt > 0 && context.yearBuilt <= 1980;
}

function isLikelyUnderbuiltParcel(context: ParcelContext): boolean {
  if (
    typeof context.landValue !== "number"
    || typeof context.buildingValue !== "number"
    || context.landValue <= 0
    || context.buildingValue < 0
  ) {
    return false;
  }

  return context.landValue >= context.buildingValue;
}

async function fetchParcelFloodContext(geometry: DanversParcelGeometry): Promise<{
  floodZones: string[];
  specialFloodHazard: boolean;
}> {
  try {
    const floodPayload = await fetchJson<ArcGisQueryResponse<DanversFloodAttributes>>(
      buildDanversFloodQueryUrl(geometry),
      15000,
    );
    const zones = Array.from(
      new Set(
        (floodPayload.features ?? [])
          .map((feature) => normalizeFloodZoneLabel(feature.attributes?.FLD_ZONE))
          .filter((value): value is string => Boolean(value)),
      ),
    );
    const specialFloodHazard = (floodPayload.features ?? []).some(
      (feature) => normalizeWhitespace(feature.attributes?.SFHA_TF ?? "").toUpperCase() === "T",
    );

    return { floodZones: zones, specialFloodHazard };
  } catch {
    return { floodZones: [], specialFloodHazard: false };
  }
}

async function fetchParcelUtilityContext(geometry: DanversParcelGeometry): Promise<{
  hasMappedWaterAccess: boolean;
  hasMappedSewerAccess: boolean;
  externalWaterServiceArea: boolean;
  externalSewerServiceArea: boolean;
}> {
  try {
    const [
      waterPipeCount,
      gravityMainCount,
      forceMainCount,
      peabodyWaterAreaCount,
      peabodySewerAreaCount,
    ] = await Promise.all([
      fetchJson<ArcGisCountResponse>(
        buildArcGisGeometryCountQueryUrl(DANVERS_WATER_PIPE_LAYER_URL, geometry),
        15000,
      ),
      fetchJson<ArcGisCountResponse>(
        buildArcGisGeometryCountQueryUrl(DANVERS_GRAVITY_MAIN_LAYER_URL, geometry),
        15000,
      ),
      fetchJson<ArcGisCountResponse>(
        buildArcGisGeometryCountQueryUrl(DANVERS_FORCE_MAIN_LAYER_URL, geometry),
        15000,
      ),
      fetchJson<ArcGisCountResponse>(
        buildArcGisGeometryCountQueryUrl(DANVERS_PEABODY_WATER_CUSTOMERS_LAYER_URL, geometry),
        15000,
      ),
      fetchJson<ArcGisCountResponse>(
        buildArcGisGeometryCountQueryUrl(DANVERS_PEABODY_SEWER_CUSTOMERS_LAYER_URL, geometry),
        15000,
      ),
    ]);

    return {
      hasMappedWaterAccess: Number(waterPipeCount.count ?? 0) > 0 || Number(peabodyWaterAreaCount.count ?? 0) > 0,
      hasMappedSewerAccess:
        Number(gravityMainCount.count ?? 0) > 0
        || Number(forceMainCount.count ?? 0) > 0
        || Number(peabodySewerAreaCount.count ?? 0) > 0,
      externalWaterServiceArea: Number(peabodyWaterAreaCount.count ?? 0) > 0,
      externalSewerServiceArea: Number(peabodySewerAreaCount.count ?? 0) > 0,
    };
  } catch {
    return {
      hasMappedWaterAccess: false,
      hasMappedSewerAccess: false,
      externalWaterServiceArea: false,
      externalSewerServiceArea: false,
    };
  }
}

async function fetchParcelWetlandsContext(geometry: DanversParcelGeometry): Promise<{
  intersectsWetlands: boolean;
}> {
  try {
    const wetlandsCount = await fetchJson<ArcGisCountResponse>(
      buildArcGisGeometryCountQueryUrl(MASSGIS_WETLANDS_LAYER_URL, geometry),
      15000,
    );

    return {
      intersectsWetlands: Number(wetlandsCount.count ?? 0) > 0,
    };
  } catch {
    return {
      intersectsWetlands: false,
    };
  }
}

async function fetchParcelGroundwaterContext(geometry: DanversParcelGeometry): Promise<{
  intersectsGroundwaterProtection: boolean;
}> {
  try {
    const groundwaterCount = await fetchJson<ArcGisCountResponse>(
      buildArcGisGeometryCountQueryUrl(DANVERS_GROUNDWATER_PROTECTION_LAYER_URL, geometry),
      15000,
    );

    return {
      intersectsGroundwaterProtection: Number(groundwaterCount.count ?? 0) > 0,
    };
  } catch {
    return {
      intersectsGroundwaterProtection: false,
    };
  }
}

async function fetchParcelContextForAddresses(addresses: string[]): Promise<ParcelContext[]> {
  const contexts: ParcelContext[] = [];
  const seenAddresses = new Set<string>();

  for (const rawAddress of addresses) {
    const address = normalizeWhitespace(rawAddress);
    if (!address || seenAddresses.has(address.toLowerCase())) {
      continue;
    }

    seenAddresses.add(address.toLowerCase());

    try {
      const assessorPayload = await fetchJson<ArcGisQueryResponse<DanversAssessorAttributes>>(
        buildDanversAssessorQueryUrl(address),
        15000,
      );
      const feature = (assessorPayload.features ?? []).find(
        (candidate) => Boolean(normalizeParcelContextAddress(candidate.attributes ?? {})),
      );

      if (!feature?.attributes) {
        continue;
      }

      const normalizedAddress = normalizeParcelContextAddress(feature.attributes);
      if (!normalizedAddress) {
        continue;
      }

      const parcelPayload = await fetchJson<ArcGisQueryResponse<Record<string, unknown>, DanversParcelGeometry>>(
        buildDanversParcelGeometryQueryUrl(normalizedAddress),
        15000,
      );
      const parcelGeometry = parcelPayload.features?.find((candidate) => candidate.geometry?.rings)?.geometry;
      const flood = parcelGeometry
        ? await fetchParcelFloodContext(parcelGeometry)
        : { floodZones: [], specialFloodHazard: false };
      const utility = parcelGeometry
        ? await fetchParcelUtilityContext(parcelGeometry)
        : {
            hasMappedWaterAccess: false,
            hasMappedSewerAccess: false,
            externalWaterServiceArea: false,
            externalSewerServiceArea: false,
          };
      const wetlands = parcelGeometry
        ? await fetchParcelWetlandsContext(parcelGeometry)
        : { intersectsWetlands: false };
      const groundwater = parcelGeometry
        ? await fetchParcelGroundwaterContext(parcelGeometry)
        : { intersectsGroundwaterProtection: false };
      contexts.push({
        address: normalizedAddress,
        ownerName: feature.attributes.OWNER1?.trim() || feature.attributes.OWN_CO?.trim() || null,
        zoning: feature.attributes.ZONING?.trim() || null,
        totalValue: typeof feature.attributes.TOTAL_VAL === "number" ? feature.attributes.TOTAL_VAL : null,
        landValue: typeof feature.attributes.LAND_VAL === "number" ? feature.attributes.LAND_VAL : null,
        buildingValue: typeof feature.attributes.BLDG_VAL === "number" ? feature.attributes.BLDG_VAL : null,
        lotSize: typeof feature.attributes.LOT_SIZE === "number" ? feature.attributes.LOT_SIZE : null,
        yearBuilt: typeof feature.attributes.YEAR_BUILT === "number" ? feature.attributes.YEAR_BUILT : null,
        useCode: typeof feature.attributes.USE_CODE === "number" ? feature.attributes.USE_CODE : null,
        floodZones: flood.floodZones,
        specialFloodHazard: flood.specialFloodHazard,
        hasMappedWaterAccess: utility.hasMappedWaterAccess,
        hasMappedSewerAccess: utility.hasMappedSewerAccess,
        externalWaterServiceArea: utility.externalWaterServiceArea,
        externalSewerServiceArea: utility.externalSewerServiceArea,
        intersectsWetlands: wetlands.intersectsWetlands,
        intersectsGroundwaterProtection: groundwater.intersectsGroundwaterProtection,
      });
    } catch {
      continue;
    }
  }

  return contexts;
}

function buildOpportunityInputs(
  briefs: CaseBrief[],
  parcels: ParcelUpsertInput[],
): OpportunityParcelInput[] {
  const derivedFromBriefs = briefs.flatMap((brief) => {
    if (!brief.addresses.length) {
      return [{
        id: brief.id,
        address: null,
      }];
    }

    return brief.addresses.map((address, index) => ({
      id: brief.addresses.length === 1 ? brief.id : `${brief.id}-address-${index + 1}`,
      address,
    }));
  });

  return derivedFromBriefs;
}

async function runAutomaticIngest(db: D1Database): Promise<IngestRunSummary> {
  const signals = await fetchAgendaSignals();
  const briefs = await buildCaseBriefs(signals);
  const priorityAddresses = Array.from(
    new Set(briefs.flatMap((brief) => brief.addresses)),
  );
  const parcels = await fetchDanversParcels(priorityAddresses);
  await upsertParcels(db, parcels);

  const opportunities = buildOpportunityInputs(briefs, parcels);
  const results = await matchAndPersistOpportunities(db, opportunities);

  return {
    parcelsIngested: parcels.length,
    opportunitiesPrepared: opportunities.length,
    matched: results.filter((result) => result.matchType !== "no_match").length,
    reviewNeeded: results.filter((result) => result.needsReview).length,
  };
}

async function buildDashboardPayload(signals: AgendaSignal[], db?: D1Database): Promise<DashboardPayload> {
  const briefs = await buildCaseBriefs(signals);
  const reviewSummary = await fetchDashboardReviewSummary(db);
  const latestStoredBrief = await fetchLatestStrategicBrief(db);
  const parcelContexts = latestStoredBrief
    ? []
    : await fetchParcelContextForAddresses(Array.from(new Set(briefs.flatMap((brief) => brief.addresses))));
  const strategicBrief =
    latestStoredBrief ??
    buildStrategicBrief(new Date().toISOString(), "live", SITES, signals, briefs, reviewSummary, parcelContexts);

  return {
    generatedAt: new Date().toISOString(),
    summary: buildSummaryMetrics(SITES, signals, briefs),
    sites: SITES,
    activity: ACTIVITIES,
    signals,
    briefs,
    strategicBrief,
    reviewSummary,
  };
}

async function buildParcelDetailPayload(address: string, db?: D1Database): Promise<ParcelDetailPayload> {
  const contexts = await fetchParcelContextForAddresses([address]);
  const context = contexts[0] ?? null;
  const parcel = db ? await findParcelByAddress(db, address) : null;
  const relatedMatches = db && parcel ? await listParcelMatchesForParcelId(db, parcel.id, 10) : [];
  const signals = await fetchAgendaSignals();
  const briefs = await buildCaseBriefs(signals);
  const parcelAddress = parcel?.address ?? context?.address ?? address;
  const relatedBriefs = buildRelatedParcelBriefs(briefs, parcelAddress);
  const relatedSignals = buildRelatedParcelSignals(signals, relatedBriefs);

  return {
    requestedAddress: address,
    parcel,
    context,
    relatedMatches,
    relatedBriefs,
    relatedSignals,
  };
}

async function buildWatchlistDetailPayload(siteId: string): Promise<WatchlistDetailPayload | null> {
  const site = SITES.find((candidate) => candidate.id === siteId);
  if (!site) {
    return null;
  }

  const signals = await fetchAgendaSignals();
  const briefs = await buildCaseBriefs(signals);
  const relatedBriefs = buildRelatedWatchlistBriefs(site, briefs);
  const relatedSignals = buildRelatedParcelSignals(signals, relatedBriefs);

  return {
    site,
    relatedBriefs,
    relatedSignals,
  };
}

function renderMetricMarkup(summary: SummaryMetric[]): string {
  return summary
    .map(
      (item) => `
        <article class="metric ${item.tone ?? "neutral"}">
          <p class="metric-label">${escapeHtml(item.label)}</p>
          <p class="metric-value">${escapeHtml(item.value)}</p>
          <p class="metric-detail">${escapeHtml(item.detail)}</p>
        </article>`,
    )
    .join("");
}

function buildParcelDetailHref(address: string): string {
  const params = new URLSearchParams({ address });
  return `/parcel?${params.toString()}`;
}

function buildWatchlistDetailHref(siteId: string): string {
  const params = new URLSearchParams({ id: siteId });
  return `/watchlist?${params.toString()}`;
}

function renderTableRows(sites: OpportunitySite[]): string {
  return sites
    .map(
      (site) => `
        <tr>
          <td><strong><a class="watchlist-site-link" href="${escapeHtml(buildWatchlistDetailHref(site.id))}">${escapeHtml(site.site)}</a></strong><div class="cell-subtle">${escapeHtml(site.corridor)}</div></td>
          <td>${escapeHtml(site.signal)}</td>
          <td>${escapeHtml(site.focus)}</td>
          <td>${escapeHtml(String(site.score))}</td>
          <td><span class="status-pill">${escapeHtml(site.status)}</span></td>
        </tr>`,
    )
    .join("");
}

function renderBriefMarkup(briefs: CaseBrief[]): string {
  return briefs
    .map(
      (brief) => {
        const drilldownAddress = brief.addresses[0] ?? "";
        const siteMarkup = drilldownAddress
          ? `<a class="brief-site-link" href="${escapeHtml(buildParcelDetailHref(drilldownAddress))}">${escapeHtml(brief.likelySite)}</a>`
          : escapeHtml(brief.likelySite);

        return `
        <li class="brief-item">
          <div class="brief-topline"><span>${escapeHtml(brief.board)}</span><span>${escapeHtml(brief.confidence)}</span></div>
          <p class="brief-site">${siteMarkup}</p>
          <p class="brief-type">${escapeHtml(brief.signalType)}</p>
          <p class="brief-rationale">${escapeHtml(brief.rationale)}</p>
          <div class="brief-meta"><span>${escapeHtml(brief.meetingDate)} · ${escapeHtml(formatSourceLabel(brief.source))}</span><a href="${escapeHtml(brief.agendaUrl)}" target="_blank" rel="noreferrer">Open source</a></div>
        </li>`;
      },
    )
    .join("");
}

function renderSignalMarkup(signals: AgendaSignal[]): string {
  return signals
    .map(
      (signal) => `
        <li class="signal-item">
          <div class="signal-meta"><span>${escapeHtml(signal.board)}</span><span>${escapeHtml(signal.meetingDate)}</span></div>
          <a class="signal-link" href="${escapeHtml(signal.agendaUrl)}" target="_blank" rel="noreferrer">${escapeHtml(signal.title)}</a>
          <p class="signal-source">${escapeHtml(formatSourceLabel(signal.source))}</p>
        </li>`,
    )
    .join("");
}

function renderActivityMarkup(activity: ActivityItem[]): string {
  return activity
    .map(
      (item) => `
        <li class="activity-item">
          <p class="activity-time">${escapeHtml(item.time)}</p>
          <div>
            <p class="activity-title">${escapeHtml(item.title)}</p>
            <p class="activity-detail">${escapeHtml(item.detail)}</p>
          </div>
        </li>`,
    )
    .join("");
}

function renderStrategicInsightsMarkup(payload: DashboardPayload): string {
  return payload.strategicBrief.insights
    .map(
      (insight) => `
        <div class="insight">
          <p class="eyebrow">${escapeHtml(insight.eyebrow)}</p>
          <strong>${escapeHtml(insight.title)}</strong>
          <p>${escapeHtml(insight.detail)}</p>
        </div>`,
    )
    .join("");
}

function renderStrategicRecommendationsMarkup(payload: DashboardPayload): string {
  return payload.strategicBrief.recommendations
    .map(
      (recommendation) => `
        <li class="recommendation-item">
          <strong>${escapeHtml(recommendation.action)}</strong>
          <p>${escapeHtml(recommendation.whyItMatters)}</p>
        </li>`,
    )
    .join("");
}

function formatCurrency(value: number | null | undefined): string {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return "Not available";
  }

  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(value);
}

function formatNumber(value: number | null | undefined): string {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return "Not available";
  }

  return new Intl.NumberFormat("en-US").format(value);
}

function formatBooleanLabel(value: boolean, yesLabel: string, noLabel: string): string {
  return value ? yesLabel : noLabel;
}

function buildRelatedParcelBriefs(briefs: CaseBrief[], address: string): CaseBrief[] {
  const normalizedTarget = normalizeAddress(address);
  if (!normalizedTarget) {
    return [];
  }

  return briefs.filter((brief) =>
    brief.addresses.some((candidate) => normalizeAddress(candidate) === normalizedTarget),
  );
}

function buildRelatedParcelSignals(signals: AgendaSignal[], briefs: CaseBrief[]): AgendaSignal[] {
  const agendaUrls = new Set(briefs.map((brief) => brief.agendaUrl));
  return signals.filter((signal) => agendaUrls.has(signal.agendaUrl));
}

function tokenizeWatchlistText(site: OpportunitySite): string[] {
  const stopWords = new Set([
    "street",
    "route",
    "corridor",
    "district",
    "north",
    "shore",
    "upper",
    "floors",
    "space",
    "cluster",
    "node",
    "strip",
    "industrial",
    "downtown",
    "retail",
    "commerce",
    "redevelopment",
    "adaptive",
    "reuse",
    "watch",
    "scan",
  ]);

  return Array.from(
    new Set(
      normalizeWhitespace(`${site.site} ${site.corridor} ${site.signal} ${site.focus}`)
        .toLowerCase()
        .split(/[^a-z0-9]+/)
        .filter((token) => token.length >= 4 && !stopWords.has(token)),
    ),
  );
}

function scoreBriefForWatchlist(site: OpportunitySite, brief: CaseBrief): number {
  const keywords = tokenizeWatchlistText(site);
  const haystack = normalizeWhitespace(
    `${brief.title} ${brief.likelySite} ${brief.signalType} ${brief.rationale}`,
  ).toLowerCase();

  let score = 0;
  for (const keyword of keywords) {
    if (haystack.includes(keyword)) {
      score += 1;
    }
  }

  if (brief.confidence === "high") {
    score += 0.5;
  } else if (brief.confidence === "medium") {
    score += 0.25;
  }

  return score;
}

function buildRelatedWatchlistBriefs(site: OpportunitySite, briefs: CaseBrief[]): CaseBrief[] {
  const scored = briefs
    .map((brief) => ({ brief, score: scoreBriefForWatchlist(site, brief) }))
    .filter((item) => item.score > 0)
    .sort((a, b) => b.score - a.score);

  if (scored.length) {
    return scored.slice(0, 5).map((item) => item.brief);
  }

  return briefs.slice(0, 3);
}

function renderStrategicScorecardMarkup(payload: DashboardPayload): string {
  const metrics = payload.strategicBrief.metrics;
  const cards = [
    {
      label: "Assessed Parcels",
      value: String(metrics.assessedParcels ?? 0),
      detail: "brief-linked parcels screened",
      tone: "neutral",
    },
    {
      label: "Utility Ready",
      value: String(Math.max(metrics.waterServedParcels ?? 0, metrics.sewerServedParcels ?? 0)),
      detail: "mapped water or sewer context",
      tone: "positive",
    },
    {
      label: "Constraint Flags",
      value: String((metrics.floodConstrainedParcels ?? 0) + (metrics.wetlandConstrainedParcels ?? 0) + (metrics.groundwaterConstrainedParcels ?? 0)),
      detail: "flood, wetlands, or groundwater flags",
      tone: "caution",
    },
    {
      label: "Business Zoned",
      value: String(metrics.businessZonedParcels ?? 0),
      detail: "commercial or industrial districts",
      tone: "neutral",
    },
  ];

  return cards
    .map(
      (card) => `
        <div class="scorecard-item scorecard-${escapeHtml(card.tone)}">
          <p class="eyebrow">${escapeHtml(card.label)}</p>
          <strong>${escapeHtml(card.value)}</strong>
          <span>${escapeHtml(card.detail)}</span>
        </div>`,
    )
    .join("");
}

function renderDashboard(payload: DashboardPayload, nonce: string): string {
  const generatedAt = new Date(payload.generatedAt).toLocaleString("en-US", {
    dateStyle: "medium",
    timeStyle: "short",
  });
  const briefGeneratedAt = new Date(payload.strategicBrief.generatedAt).toLocaleString("en-US", {
    dateStyle: "medium",
    timeStyle: "short",
  });
  const initialTableRows = renderTableRows(payload.sites);
  const strategicInsightsMarkup = renderStrategicInsightsMarkup(payload);
  const strategicRecommendationsMarkup = renderStrategicRecommendationsMarkup(payload);
  const strategicScorecardMarkup = renderStrategicScorecardMarkup(payload);
  const reviewSummaryMarkup = `
    <div class="review-summary">
      <div class="review-card review-card-match">
        <p class="eyebrow">Matched</p>
        <strong>${escapeHtml(String(payload.reviewSummary.matched))}</strong>
        <span>parcel-linked opportunities</span>
      </div>
      <div class="review-card review-card-review">
        <p class="eyebrow">Needs Review</p>
        <strong>${escapeHtml(String(payload.reviewSummary.needsReview))}</strong>
        <span>items still needing staff review</span>
      </div>
    </div>`;

  return `<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Opportunity</title>
    <style nonce="${escapeHtml(nonce)}">
      :root {
        --bg: #edf4fb;
        --panel: #f8fbff;
        --ink: #12324f;
        --muted: #5d7791;
        --line: rgba(18, 50, 79, 0.12);
        --accent: #005a9c;
        --accent-soft: rgba(0, 90, 156, 0.1);
        --warm: #2a7abf;
        --warm-soft: rgba(42, 122, 191, 0.14);
      }

      * {
        box-sizing: border-box;
      }

      body {
        margin: 0;
        font-family: Georgia, "Times New Roman", serif;
        background: linear-gradient(180deg, #f1f7fd 0%, #dce9f6 100%);
        color: var(--ink);
      }

      a {
        color: inherit;
      }

      .shell {
        min-height: 100vh;
        display: grid;
        grid-template-columns: 280px minmax(0, 1fr);
      }

      .rail {
        padding: 28px 22px;
        background: rgba(246, 251, 255, 0.94);
        border-right: 1px solid var(--line);
        display: grid;
        align-content: space-between;
        gap: 28px;
      }

      .brand {
        display: grid;
        grid-template-columns: 42px minmax(0, 1fr);
        gap: 14px;
        align-items: start;
      }

      .brand-mark {
        width: 42px;
        height: 42px;
        border-radius: 10px;
        display: grid;
        place-items: center;
        background: var(--accent);
        color: #fff;
        font-weight: 700;
      }

      .eyebrow {
        margin: 0 0 6px;
        color: var(--muted);
        font-size: 0.8rem;
        text-transform: uppercase;
        letter-spacing: 0.06em;
      }

      h1,
      h2,
      h3,
      p {
        margin-top: 0;
      }

      .nav-group {
        display: grid;
        gap: 8px;
        margin-top: 22px;
      }

      .nav-item {
        padding: 10px 12px;
        border-radius: 8px;
        text-decoration: none;
        color: var(--muted);
      }

      .nav-item.active,
      .nav-item:hover {
        background: var(--accent-soft);
        color: var(--accent);
      }

      .rail-footer {
        display: grid;
        gap: 10px;
        color: var(--muted);
        font-size: 0.9rem;
      }

      .main {
        padding: 28px 26px 40px;
      }

      .topbar,
      .workspace {
        display: grid;
        gap: 18px;
      }

      .topbar {
        grid-template-columns: minmax(0, 1fr) 280px;
        margin-bottom: 18px;
      }

      .topbar-meta,
      .panel,
      .metric {
        background: rgba(248, 251, 255, 0.88);
        border: 1px solid var(--line);
        border-radius: 14px;
      }

      .topbar-meta,
      .panel {
        padding: 18px;
      }

      .metrics {
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 14px;
        margin-bottom: 18px;
      }

      .metric {
        padding: 16px;
      }

      .metric.warm {
        background: linear-gradient(180deg, #f8fbff, #e3effb);
      }

      .metric.cool {
        background: linear-gradient(180deg, #f8fbff, #d8eafc);
      }

      .metric-label,
      .metric-detail {
        color: var(--muted);
      }

      .metric-value {
        margin: 10px 0 8px;
        font-size: 2rem;
      }

      .review-summary {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 10px;
        margin-top: 14px;
      }

      .review-card {
        padding: 12px 14px;
        border-radius: 10px;
        border: 1px solid var(--line);
      }

      .review-card strong {
        display: block;
        font-size: 1.8rem;
        margin: 2px 0 4px;
      }

      .review-card span {
        color: var(--muted);
        font-size: 0.92rem;
      }

      .review-card-match {
        background: linear-gradient(180deg, #eef7ff, #dcecff);
      }

      .review-card-review {
        background: linear-gradient(180deg, #f8fbff, #e9f3fd);
      }

      .workspace {
        grid-template-columns: minmax(0, 1.3fr) minmax(320px, 0.9fr);
        align-items: start;
      }

      .watchlist {
        grid-column: 1;
      }

      .side-stack {
        grid-column: 2;
        grid-row: 1 / span 2;
      }

      .strategic-panel {
        grid-column: 1;
      }

      .watchlist-head,
      .panel-head {
        display: flex;
        justify-content: space-between;
        gap: 16px;
        align-items: start;
        margin-bottom: 14px;
      }

      .controls {
        display: grid;
        grid-template-columns: minmax(0, 1fr) 180px 180px;
        gap: 10px;
        margin-bottom: 14px;
      }

      .search,
      .select {
        width: 100%;
        padding: 10px 12px;
        border-radius: 8px;
        border: 1px solid var(--line);
        background: #fffdf9;
        color: var(--ink);
      }

      table {
        width: 100%;
        border-collapse: collapse;
      }

      th,
      td {
        padding: 12px 10px;
        text-align: left;
        border-top: 1px solid var(--line);
        vertical-align: top;
      }

      th {
        color: var(--muted);
        font-size: 0.82rem;
        text-transform: uppercase;
        letter-spacing: 0.04em;
      }

      .cell-subtle,
      .empty,
      .signal-source,
      .brief-rationale,
      .activity-detail {
        color: var(--muted);
      }

      .status-pill {
        display: inline-flex;
        align-items: center;
        min-height: 32px;
        padding: 0 12px;
        border-radius: 999px;
        background: var(--accent-soft);
        color: var(--accent);
        white-space: nowrap;
      }

      .side-stack {
        display: grid;
        gap: 16px;
      }

      .brief-list,
      .signal-list,
      .activity-list,
      .recommendation-list {
        display: grid;
        gap: 14px;
        padding: 0;
        margin: 0;
        list-style: none;
      }

      .brief-item,
      .signal-item,
      .activity-item {
        padding-top: 14px;
        border-top: 1px solid var(--line);
      }

      .brief-topline,
      .signal-meta {
        display: flex;
        justify-content: space-between;
        gap: 8px;
        color: var(--muted);
        font-size: 0.82rem;
        text-transform: uppercase;
        letter-spacing: 0.04em;
      }

      .brief-site {
        margin: 8px 0 0;
        font-size: 1.05rem;
      }

      .brief-site-link {
        color: var(--ink);
        text-decoration-color: rgba(0, 90, 156, 0.35);
        text-underline-offset: 2px;
      }

      .watchlist-site-link {
        color: var(--ink);
        text-decoration-color: rgba(0, 90, 156, 0.35);
        text-underline-offset: 2px;
      }

      .watchlist-site-link:hover,
      .brief-site-link:hover {
        color: var(--accent);
      }

      .brief-type {
        margin: 6px 0 0;
        color: var(--accent);
      }

      .brief-meta {
        display: flex;
        justify-content: space-between;
        gap: 10px;
        margin-top: 10px;
        font-size: 0.92rem;
      }

      .signal-link {
        display: block;
        margin-top: 8px;
        line-height: 1.45;
      }

      .activity-item {
        display: grid;
        grid-template-columns: 56px minmax(0, 1fr);
        gap: 14px;
      }

      .activity-time,
      .activity-title {
        margin: 0;
      }

      .activity-time {
        color: var(--muted);
        font-size: 0.86rem;
        letter-spacing: 0.04em;
        text-transform: uppercase;
      }

      .insight-grid {
        display: grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 12px;
      }

      .insight {
        min-height: 108px;
        padding: 14px;
        border-radius: 10px;
        background: rgba(255, 252, 247, 0.74);
        border: 1px solid rgba(34, 42, 38, 0.08);
      }

      .strategic-summary {
        margin: 12px 0 14px;
        color: var(--muted);
        line-height: 1.55;
      }

      .strategic-scorecard {
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 10px;
        margin-bottom: 16px;
      }

      .scorecard-item {
        padding: 12px 14px;
        border-radius: 10px;
        border: 1px solid var(--line);
        background: rgba(255, 255, 255, 0.7);
      }

      .scorecard-item strong {
        display: block;
        font-size: 1.55rem;
        margin: 2px 0 4px;
      }

      .scorecard-item span {
        color: var(--muted);
        font-size: 0.88rem;
      }

      .scorecard-positive {
        background: linear-gradient(180deg, #eef8f2, #dceee4);
      }

      .scorecard-caution {
        background: linear-gradient(180deg, #fff8ef, #fce8d5);
      }

      .strategic-meta {
        color: var(--muted);
        font-size: 0.9rem;
        margin-bottom: 14px;
      }

      .recommendation-list {
        margin-top: 14px;
      }

      .recommendation-item {
        padding-top: 14px;
        border-top: 1px solid var(--line);
      }

      .recommendation-item p {
        margin: 8px 0 0;
        color: var(--muted);
      }

      @media (max-width: 1100px) {
        .shell,
        .topbar,
        .workspace,
        .controls,
        .metrics,
        .insight-grid,
        .strategic-scorecard {
          grid-template-columns: 1fr;
        }

        .rail {
          border-right: 0;
          border-bottom: 1px solid var(--line);
        }
      }
    </style>
  </head>
  <body>
    <div class="shell">
      <aside class="rail">
        <div>
          <div class="brand">
            <div class="brand-mark">O</div>
            <div>
              <p class="eyebrow">Danvers Economic Development</p>
              <h1>Opportunity</h1>
              <p>Internal workspace for tracking redevelopment signals, corridor shifts, and site-level follow-up.</p>
            </div>
          </div>
          <nav class="nav-group" aria-label="Primary">
            <a class="nav-item active" href="/">Overview</a>
            <a class="nav-item" href="/api/summary">Summary API</a>
            <a class="nav-item" href="/api/sites">Sites API</a>
            <a class="nav-item" href="/api/signals">Signals API</a>
            <a class="nav-item" href="/api/briefs">Case Briefs API</a>
            <a class="nav-item" href="/api/strategic-briefs">Strategic Briefs API</a>
            <a class="nav-item" href="/api/status">System Status</a>
          </nav>
        </div>
        <div class="rail-footer">
          <span>Shell generated ${escapeHtml(generatedAt)}</span>
          <span>First live source: Danvers Agenda Center for Planning Board and Zoning Board postings.</span>
        </div>
      </aside>
      <main class="main">
        <header class="topbar">
          <div class="topbar-copy">
            <p class="eyebrow">Staff Dashboard</p>
            <h2>See where change may be becoming opportunity.</h2>
            <p>
              This version includes one live public source feed from Danvers plus a lightweight case-extraction layer.
              The tool is moving from meeting notices toward site-specific review leads.
            </p>
          </div>
          <div class="topbar-meta">
            <div>Worker name: <strong>opportunity</strong></div>
            <div>Mode: dashboard plus case briefs</div>
            <div>Data store: seeded watchlist, live public feed</div>
            ${reviewSummaryMarkup}
          </div>
        </header>

        <section class="metrics" aria-label="Top metrics">
          ${renderMetricMarkup(payload.summary)}
        </section>

        <section class="workspace">
          <div class="panel watchlist">
            <div class="watchlist-head">
              <div>
                <p class="eyebrow">Watchlist</p>
                <h3>Current review lanes</h3>
                <p>The controls below let staff narrow the list by corridor, status, or keyword while the source-matching layer matures.</p>
              </div>
              <span class="status-pill">Explainable alerts only</span>
            </div>

            <div class="controls">
              <input id="search-input" class="search" type="search" placeholder="Search sites, corridors, or signals" />
              <select id="corridor-filter" class="select" aria-label="Filter by corridor">
                <option value="all">All corridors</option>
              </select>
              <select id="status-filter" class="select" aria-label="Filter by status">
                <option value="all">All statuses</option>
              </select>
            </div>

            <table>
              <thead>
                <tr>
                  <th>Site</th>
                  <th>Signal</th>
                  <th>Focus</th>
                  <th>Score</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody id="watchlist-body">${initialTableRows}</tbody>
            </table>
            <div id="empty-state" class="empty" hidden>No sites match the current filter.</div>
          </div>

          <section class="panel strategic-panel">
            <p class="eyebrow">Strategic Insights</p>
            <h3>${escapeHtml(payload.strategicBrief.title)}</h3>
            <p class="strategic-summary">${escapeHtml(payload.strategicBrief.summary)}</p>
            <div class="strategic-scorecard">${strategicScorecardMarkup}</div>
            <div class="strategic-meta">
              Latest brief ${escapeHtml(briefGeneratedAt)} · Trigger ${escapeHtml(payload.strategicBrief.trigger)} · Sources reviewed ${escapeHtml(String(payload.strategicBrief.sourceCount))}
            </div>
            <div class="insight-grid">
              ${strategicInsightsMarkup}
            </div>
            <ul class="recommendation-list">${strategicRecommendationsMarkup}</ul>
          </section>

          <div class="side-stack">
            <section class="panel">
              <div class="panel-head">
                <div>
                  <p class="eyebrow">Case Briefs</p>
                  <h3>Likely site signals</h3>
                </div>
              </div>
              <ul id="brief-list" class="brief-list">${renderBriefMarkup(payload.briefs)}</ul>
            </section>

            <section class="panel">
              <div class="panel-head">
                <div>
                  <p class="eyebrow">Live Source Feed</p>
                  <h3>Recent agenda postings</h3>
                </div>
              </div>
              <ul id="signal-list" class="signal-list">${renderSignalMarkup(payload.signals)}</ul>
            </section>

            <section class="panel">
              <div class="panel-head">
                <div>
                  <p class="eyebrow">Activity</p>
                  <h3>Build notes</h3>
                  <p>What is live now, and what the current build cycle is focused on next.</p>
                </div>
              </div>
              <ul class="activity-list">${renderActivityMarkup(payload.activity)}</ul>
            </section>
          </div>
        </section>
      </main>
    </div>
    <script nonce="${escapeHtml(nonce)}">
      const initialData = ${JSON.stringify(payload)};

      const watchlistBody = document.getElementById("watchlist-body");
      const emptyState = document.getElementById("empty-state");
      const searchInput = document.getElementById("search-input");
      const corridorFilter = document.getElementById("corridor-filter");
      const statusFilter = document.getElementById("status-filter");
      const signalList = document.getElementById("signal-list");
      const briefList = document.getElementById("brief-list");

      function escapeHtml(value) {
        return value
          .replaceAll("&", "&amp;")
          .replaceAll("<", "&lt;")
          .replaceAll(">", "&gt;")
          .replaceAll('"', "&quot;")
          .replaceAll("'", "&#39;");
      }

      function populateFilters(sites) {
        const corridors = [...new Set(sites.map((site) => site.corridor))].sort();
        const statuses = [...new Set(sites.map((site) => site.status))].sort();

        corridorFilter.innerHTML = '<option value="all">All corridors</option>' +
          corridors.map((corridor) => '<option value="' + escapeHtml(corridor) + '">' + escapeHtml(corridor) + '</option>').join("");

        statusFilter.innerHTML = '<option value="all">All statuses</option>' +
          statuses.map((status) => '<option value="' + escapeHtml(status) + '">' + escapeHtml(status) + '</option>').join("");
      }

      function renderRows(sites) {
        if (!sites.length) {
          watchlistBody.innerHTML = "";
          emptyState.hidden = false;
          return;
        }

        emptyState.hidden = true;
        watchlistBody.innerHTML = sites.map((item) => {
          return '<tr>' +
            '<td><strong><a class="watchlist-site-link" href="/watchlist?id=' + encodeURIComponent(item.id) + '">' + escapeHtml(item.site) + '</a></strong><div class="cell-subtle">' + escapeHtml(item.corridor) + '</div></td>' +
            '<td>' + escapeHtml(item.signal) + '</td>' +
            '<td>' + escapeHtml(item.focus) + '</td>' +
            '<td>' + escapeHtml(String(item.score)) + '</td>' +
            '<td><span class="status-pill">' + escapeHtml(item.status) + '</span></td>' +
          '</tr>';
        }).join("");
      }

      function renderSignals(signals) {
        signalList.innerHTML = signals.map((signal) => {
          return '<li class="signal-item">' +
            '<div class="signal-meta"><span>' + escapeHtml(signal.board) + '</span><span>' + escapeHtml(signal.meetingDate) + '</span></div>' +
            '<a class="signal-link" href="' + escapeHtml(signal.agendaUrl) + '" target="_blank" rel="noreferrer">' + escapeHtml(signal.title) + '</a>' +
            '<p class="signal-source">' + escapeHtml(signal.source) + '</p>' +
          '</li>';
        }).join("");
      }

      function renderBriefs(briefs) {
        briefList.innerHTML = briefs.map((brief) => {
          const drilldownAddress = brief.addresses && brief.addresses.length ? brief.addresses[0] : "";
          const siteMarkup = drilldownAddress
            ? '<a class="brief-site-link" href="/parcel?address=' + encodeURIComponent(drilldownAddress) + '">' + escapeHtml(brief.likelySite) + '</a>'
            : escapeHtml(brief.likelySite);
          return '<li class="brief-item">' +
            '<div class="brief-topline"><span>' + escapeHtml(brief.board) + '</span><span>' + escapeHtml(brief.confidence) + '</span></div>' +
            '<p class="brief-site">' + siteMarkup + '</p>' +
            '<p class="brief-type">' + escapeHtml(brief.signalType) + '</p>' +
            '<p class="brief-rationale">' + escapeHtml(brief.rationale) + '</p>' +
            '<div class="brief-meta"><span>' + escapeHtml(brief.meetingDate) + '</span><a href="' + escapeHtml(brief.agendaUrl) + '" target="_blank" rel="noreferrer">Open agenda</a></div>' +
          '</li>';
        }).join("");
      }

      function applyFilters() {
        const term = searchInput.value.trim().toLowerCase();
        const corridor = corridorFilter.value;
        const status = statusFilter.value;

        const filtered = initialData.sites.filter((site) => {
          const matchesTerm = !term || [site.site, site.corridor, site.signal, site.focus]
            .join(" ")
            .toLowerCase()
            .includes(term);
          const matchesCorridor = corridor === "all" || site.corridor === corridor;
          const matchesStatus = status === "all" || site.status === status;
          return matchesTerm && matchesCorridor && matchesStatus;
        });

        renderRows(filtered);
      }

      populateFilters(initialData.sites);
      renderRows(initialData.sites);
      renderSignals(initialData.signals);
      renderBriefs(initialData.briefs);

      searchInput.addEventListener("input", applyFilters);
      corridorFilter.addEventListener("change", applyFilters);
      statusFilter.addEventListener("change", applyFilters);
    </script>
  </body>
</html>`;
}

function renderParcelDetailPage(payload: ParcelDetailPayload, nonce: string): string {
  const context = payload.context;
  const title = payload.parcel?.address ?? context?.address ?? payload.requestedAddress;
  const assessorLink = buildDanversAssessorQueryUrl(title);
  const parcelQueryLink = buildDanversParcelAddressQueryUrl(title);
  const relatedMatchesMarkup = payload.relatedMatches.length
    ? payload.relatedMatches.map((item) => `
        <li class="detail-list-item">
          <strong>${escapeHtml(item.input.address ?? item.input.mapLot ?? item.opportunityId)}</strong>
          <p>${escapeHtml(item.matchType)} · confidence ${escapeHtml(String(item.confidence))}${item.needsReview ? " · needs review" : ""}</p>
          <span>Updated ${escapeHtml(new Date(item.updatedAt).toLocaleString("en-US", { dateStyle: "medium", timeStyle: "short" }))}</span>
        </li>`).join("")
    : `<li class="detail-list-item"><strong>No related matched opportunities yet.</strong><p>This parcel has not been tied to stored opportunity records in D1 yet.</p></li>`;
  const relatedBriefsMarkup = payload.relatedBriefs.length
    ? payload.relatedBriefs.map((brief) => `
        <li class="detail-list-item">
          <strong>${escapeHtml(brief.likelySite)}</strong>
          <p>${escapeHtml(brief.board)} · ${escapeHtml(brief.meetingDate)} · ${escapeHtml(brief.signalType)}</p>
          <span><a href="${escapeHtml(brief.agendaUrl)}" target="_blank" rel="noreferrer">Open source</a></span>
        </li>`).join("")
    : `<li class="detail-list-item"><strong>No related case briefs found.</strong><p>The current live brief set does not yet include this address explicitly.</p></li>`;
  const relatedSignalsMarkup = payload.relatedSignals.length
    ? payload.relatedSignals.map((signal) => `
        <li class="detail-list-item">
          <strong>${escapeHtml(signal.title)}</strong>
          <p>${escapeHtml(signal.board)} · ${escapeHtml(signal.meetingDate)} · ${escapeHtml(formatSourceLabel(signal.source))}</p>
          <span><a href="${escapeHtml(signal.agendaUrl)}" target="_blank" rel="noreferrer">Open source</a></span>
        </li>`).join("")
    : `<li class="detail-list-item"><strong>No related live signals found.</strong><p>The current signal set does not include a direct posting tied to this address.</p></li>`;

  return `<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>${escapeHtml(title)} | Opportunity</title>
    <style nonce="${escapeHtml(nonce)}">
      :root {
        --bg: #eef5fb;
        --panel: rgba(248, 251, 255, 0.9);
        --ink: #12324f;
        --muted: #5d7791;
        --line: rgba(18, 50, 79, 0.12);
        --accent: #005a9c;
      }
      * { box-sizing: border-box; }
      body {
        margin: 0;
        font-family: Georgia, "Times New Roman", serif;
        background: linear-gradient(180deg, #f3f8fd 0%, #dde9f5 100%);
        color: var(--ink);
      }
      a { color: inherit; }
      .page {
        max-width: 1180px;
        margin: 0 auto;
        padding: 28px 22px 48px;
      }
      .topbar {
        display: flex;
        justify-content: space-between;
        gap: 16px;
        align-items: start;
        margin-bottom: 18px;
      }
      .back-link {
        text-decoration: none;
        color: var(--accent);
      }
      .eyebrow {
        margin: 0 0 6px;
        color: var(--muted);
        font-size: 0.8rem;
        text-transform: uppercase;
        letter-spacing: 0.06em;
      }
      h1, h2, h3, p { margin-top: 0; }
      .grid {
        display: grid;
        grid-template-columns: minmax(0, 1.2fr) minmax(320px, 0.8fr);
        gap: 18px;
      }
      .panel {
        background: var(--panel);
        border: 1px solid var(--line);
        border-radius: 14px;
        padding: 18px;
      }
      .detail-grid {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 12px;
      }
      .detail-card {
        padding: 14px;
        border-radius: 10px;
        border: 1px solid var(--line);
        background: rgba(255, 255, 255, 0.78);
      }
      .detail-card strong {
        display: block;
        font-size: 1.2rem;
        margin: 4px 0 6px;
      }
      .detail-card span,
      .detail-list-item span,
      .detail-card p {
        color: var(--muted);
      }
      .detail-list {
        list-style: none;
        padding: 0;
        margin: 0;
        display: grid;
        gap: 14px;
      }
      .detail-list-item {
        padding-top: 14px;
        border-top: 1px solid var(--line);
      }
      .status-row {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 10px;
      }
      .status-pill {
        display: inline-flex;
        align-items: center;
        min-height: 32px;
        padding: 0 12px;
        border-radius: 999px;
        background: rgba(0, 90, 156, 0.1);
        color: var(--accent);
      }
      .link-row {
        display: flex;
        flex-wrap: wrap;
        gap: 10px;
        margin-top: 14px;
      }
      .source-link {
        text-decoration: none;
        color: var(--accent);
      }
      @media (max-width: 980px) {
        .grid,
        .detail-grid,
        .status-row {
          grid-template-columns: 1fr;
        }
      }
    </style>
  </head>
  <body>
    <div class="page">
      <div class="topbar">
        <div>
          <p class="eyebrow">Parcel Drilldown</p>
          <h1>${escapeHtml(title)}</h1>
          <p>This page pulls together ownership, zoning, value, and screening context for the selected address.</p>
        </div>
        <a class="back-link" href="/">Back to dashboard</a>
      </div>
      <div class="grid">
        <section class="panel">
          <p class="eyebrow">Parcel Snapshot</p>
          <div class="detail-grid">
            <div class="detail-card">
              <p class="eyebrow">Owner</p>
              <strong>${escapeHtml(context?.ownerName ?? payload.parcel?.ownerName ?? "Not available")}</strong>
              <span>Current assessor ownership name</span>
            </div>
            <div class="detail-card">
              <p class="eyebrow">Zoning</p>
              <strong>${escapeHtml(context?.zoning ?? payload.parcel?.zoningDistrict ?? "Not available")}</strong>
              <span>Current zoning district</span>
            </div>
            <div class="detail-card">
              <p class="eyebrow">Map-Lot</p>
              <strong>${escapeHtml(payload.parcel?.mapLot ?? "Not available")}</strong>
              <span>Stored parcel identifier</span>
            </div>
            <div class="detail-card">
              <p class="eyebrow">Year Built</p>
              <strong>${escapeHtml(context?.yearBuilt ? String(context.yearBuilt) : "Not available")}</strong>
              <span>Assessor building year</span>
            </div>
            <div class="detail-card">
              <p class="eyebrow">Total Value</p>
              <strong>${escapeHtml(formatCurrency(context?.totalValue))}</strong>
              <span>Assessor total value</span>
            </div>
            <div class="detail-card">
              <p class="eyebrow">Lot Size</p>
              <strong>${escapeHtml(formatNumber(context?.lotSize))}</strong>
              <span>Square feet from assessor data</span>
            </div>
          </div>
        </section>
        <section class="panel">
          <p class="eyebrow">Readiness and Constraints</p>
          <div class="status-row">
            <div class="detail-card">
              <p class="eyebrow">Utilities</p>
              <strong>${escapeHtml(formatBooleanLabel(Boolean(context?.hasMappedWaterAccess || context?.hasMappedSewerAccess), "Mapped utility context found", "No mapped utility context found"))}</strong>
              <span>Water ${escapeHtml(context?.hasMappedWaterAccess ? "yes" : "no")} · Sewer ${escapeHtml(context?.hasMappedSewerAccess ? "yes" : "no")}</span>
            </div>
            <div class="detail-card">
              <p class="eyebrow">Environmental Flags</p>
              <strong>${escapeHtml(formatBooleanLabel(Boolean(context?.specialFloodHazard || context?.intersectsWetlands || context?.intersectsGroundwaterProtection), "Constraint screening flagged", "No major screening flags found"))}</strong>
              <span>Flood ${escapeHtml(context?.specialFloodHazard ? "yes" : "no")} · Wetlands ${escapeHtml(context?.intersectsWetlands ? "yes" : "no")} · Groundwater ${escapeHtml(context?.intersectsGroundwaterProtection ? "yes" : "no")}</span>
            </div>
          </div>
          <div style="margin-top: 14px;">
            <span class="status-pill">${escapeHtml(context?.floodZones?.length ? context.floodZones.join(", ") : "No mapped flood zone label")}</span>
          </div>
          <div class="link-row">
            <a class="source-link" href="${escapeHtml(parcelQueryLink)}" target="_blank" rel="noreferrer">Open parcel query</a>
            <a class="source-link" href="${escapeHtml(assessorLink)}" target="_blank" rel="noreferrer">Open assessor query</a>
          </div>
        </section>
      </div>
      <section class="panel" style="margin-top: 18px;">
        <p class="eyebrow">Related Opportunity Records</p>
        <ul class="detail-list">${relatedMatchesMarkup}</ul>
      </section>
      <div class="grid" style="margin-top: 18px;">
        <section class="panel">
          <p class="eyebrow">Related Case Briefs</p>
          <ul class="detail-list">${relatedBriefsMarkup}</ul>
        </section>
        <section class="panel">
          <p class="eyebrow">Related Live Signals</p>
          <ul class="detail-list">${relatedSignalsMarkup}</ul>
        </section>
      </div>
    </div>
  </body>
</html>`;
}

function renderWatchlistDetailPage(payload: WatchlistDetailPayload, nonce: string): string {
  const relatedBriefsMarkup = payload.relatedBriefs.length
    ? payload.relatedBriefs.map((brief) => {
      const address = brief.addresses[0] ?? "";
      const href = address ? buildParcelDetailHref(address) : brief.agendaUrl;
      const linkLabel = address ? "Open parcel detail" : "Open source";

      return `
        <li class="detail-list-item">
          <strong>${escapeHtml(brief.likelySite)}</strong>
          <p>${escapeHtml(brief.board)} · ${escapeHtml(brief.meetingDate)} · ${escapeHtml(brief.signalType)}</p>
          <span><a href="${escapeHtml(href)}"${address ? "" : ' target="_blank" rel="noreferrer"'}>${escapeHtml(linkLabel)}</a></span>
        </li>`;
    }).join("")
    : `<li class="detail-list-item"><strong>No related briefs found.</strong><p>The current live brief set has not been tied to this watchlist item yet.</p></li>`;
  const relatedSignalsMarkup = payload.relatedSignals.length
    ? payload.relatedSignals.map((signal) => `
        <li class="detail-list-item">
          <strong>${escapeHtml(signal.title)}</strong>
          <p>${escapeHtml(signal.board)} · ${escapeHtml(signal.meetingDate)} · ${escapeHtml(formatSourceLabel(signal.source))}</p>
          <span><a href="${escapeHtml(signal.agendaUrl)}" target="_blank" rel="noreferrer">Open source</a></span>
        </li>`).join("")
    : `<li class="detail-list-item"><strong>No related signals found.</strong><p>This watchlist item is still mostly being tracked through the seeded staff list.</p></li>`;

  return `<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>${escapeHtml(payload.site.site)} | Opportunity</title>
    <style nonce="${escapeHtml(nonce)}">
      :root {
        --bg: #eef5fb;
        --panel: rgba(248, 251, 255, 0.9);
        --ink: #12324f;
        --muted: #5d7791;
        --line: rgba(18, 50, 79, 0.12);
        --accent: #005a9c;
      }
      * { box-sizing: border-box; }
      body {
        margin: 0;
        font-family: Georgia, "Times New Roman", serif;
        background: linear-gradient(180deg, #f3f8fd 0%, #dde9f5 100%);
        color: var(--ink);
      }
      a { color: inherit; }
      .page {
        max-width: 1180px;
        margin: 0 auto;
        padding: 28px 22px 48px;
      }
      .topbar {
        display: flex;
        justify-content: space-between;
        gap: 16px;
        align-items: start;
        margin-bottom: 18px;
      }
      .back-link {
        text-decoration: none;
        color: var(--accent);
      }
      .eyebrow {
        margin: 0 0 6px;
        color: var(--muted);
        font-size: 0.8rem;
        text-transform: uppercase;
        letter-spacing: 0.06em;
      }
      h1, h2, h3, p { margin-top: 0; }
      .grid {
        display: grid;
        grid-template-columns: minmax(0, 1fr) minmax(320px, 0.9fr);
        gap: 18px;
      }
      .panel {
        background: var(--panel);
        border: 1px solid var(--line);
        border-radius: 14px;
        padding: 18px;
      }
      .detail-grid {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 12px;
      }
      .detail-card {
        padding: 14px;
        border-radius: 10px;
        border: 1px solid var(--line);
        background: rgba(255, 255, 255, 0.78);
      }
      .detail-card strong {
        display: block;
        font-size: 1.2rem;
        margin: 4px 0 6px;
      }
      .detail-card span,
      .detail-card p,
      .detail-list-item span {
        color: var(--muted);
      }
      .detail-list {
        list-style: none;
        padding: 0;
        margin: 0;
        display: grid;
        gap: 14px;
      }
      .detail-list-item {
        padding-top: 14px;
        border-top: 1px solid var(--line);
      }
      @media (max-width: 980px) {
        .grid,
        .detail-grid {
          grid-template-columns: 1fr;
        }
      }
    </style>
  </head>
  <body>
    <div class="page">
      <div class="topbar">
        <div>
          <p class="eyebrow">Watchlist Drilldown</p>
          <h1>${escapeHtml(payload.site.site)}</h1>
          <p>${escapeHtml(payload.site.corridor)} · ${escapeHtml(payload.site.signal)} · ${escapeHtml(payload.site.focus)}</p>
        </div>
        <a class="back-link" href="/">Back to dashboard</a>
      </div>
      <div class="grid">
        <section class="panel">
          <p class="eyebrow">Watchlist Snapshot</p>
          <div class="detail-grid">
            <div class="detail-card">
              <p class="eyebrow">Status</p>
              <strong>${escapeHtml(payload.site.status)}</strong>
              <span>Current staff-facing watchlist status</span>
            </div>
            <div class="detail-card">
              <p class="eyebrow">Readiness</p>
              <strong>${escapeHtml(payload.site.readiness)}</strong>
              <span>Current readiness posture</span>
            </div>
            <div class="detail-card">
              <p class="eyebrow">Score</p>
              <strong>${escapeHtml(String(payload.site.score))}</strong>
              <span>Current seeded watchlist score</span>
            </div>
            <div class="detail-card">
              <p class="eyebrow">Updated</p>
              <strong>${escapeHtml(payload.site.updatedAt)}</strong>
              <span>Most recent seed-list update</span>
            </div>
          </div>
        </section>
        <section class="panel">
          <p class="eyebrow">Current Interpretation</p>
          <p>This seeded watchlist item is now linked to the live brief and signal workflow below so staff can move from corridor-level monitoring into parcel-level follow-up where addresses are available.</p>
        </section>
      </div>
      <div class="grid" style="margin-top: 18px;">
        <section class="panel">
          <p class="eyebrow">Related Briefs</p>
          <ul class="detail-list">${relatedBriefsMarkup}</ul>
        </section>
        <section class="panel">
          <p class="eyebrow">Related Signals</p>
          <ul class="detail-list">${relatedSignalsMarkup}</ul>
        </section>
      </div>
    </div>
  </body>
</html>`;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (request.method === "GET" && url.pathname === "/") {
      const signals = await fetchAgendaSignals();
      const payload = await buildDashboardPayload(signals, env.OPPORTUNITYDB);
      const nonce = generateCspNonce();
      return withSecurityHeaders(
        new Response(renderDashboard(payload, nonce), {
          headers: {
            "content-type": "text/html; charset=utf-8",
          },
        }),
        nonce,
      );
    }

    if (request.method === "GET" && url.pathname === "/parcel") {
      const address = url.searchParams.get("address")?.trim() ?? "";
      if (!address) {
        return withSecurityHeaders(
          Response.json(
            { ok: false, error: "Query string must include an address parameter." },
            { status: 400 },
          ),
        );
      }

      const payload = await buildParcelDetailPayload(address, env.OPPORTUNITYDB);
      const nonce = generateCspNonce();
      return withSecurityHeaders(
        new Response(renderParcelDetailPage(payload, nonce), {
          headers: {
            "content-type": "text/html; charset=utf-8",
          },
        }),
        nonce,
      );
    }

    if (request.method === "GET" && url.pathname === "/watchlist") {
      const siteId = url.searchParams.get("id")?.trim() ?? "";
      const payload = await buildWatchlistDetailPayload(siteId);
      if (!payload) {
        return withSecurityHeaders(new Response("Watchlist item not found", { status: 404 }));
      }

      const nonce = generateCspNonce();
      return withSecurityHeaders(
        new Response(renderWatchlistDetailPage(payload, nonce), {
          headers: {
            "content-type": "text/html; charset=utf-8",
          },
        }),
        nonce,
      );
    }

    if (request.method === "GET" && url.pathname === "/api/status") {
      return withSecurityHeaders(Response.json(buildStatusPayload(env)));
    }

    if (request.method === "GET" && url.pathname === "/api/parcel-detail") {
      const address = url.searchParams.get("address")?.trim() ?? "";
      if (!address) {
        return withSecurityHeaders(
          Response.json(
            { ok: false, error: "Query string must include an address parameter." },
            { status: 400 },
          ),
        );
      }

      const payload = await buildParcelDetailPayload(address, env.OPPORTUNITYDB);
      return withSecurityHeaders(
        Response.json({
          ok: true,
          ...payload,
        }),
      );
    }

    if (request.method === "GET" && url.pathname === "/api/sites") {
      return withSecurityHeaders(
        Response.json({
          sites: SITES,
          count: SITES.length,
        }),
      );
    }

    if (request.method === "GET" && url.pathname === "/api/signals") {
      const signals = await fetchAgendaSignals();
      return withSecurityHeaders(
        Response.json({
          signals,
          source: AGENDA_CENTER_URL,
          updatedAt: new Date().toISOString(),
        }),
      );
    }

    if (request.method === "GET" && url.pathname === "/api/debug/signals") {
      const debug = await fetchAgendaSignalsDebug();
      return withSecurityHeaders(
        Response.json({
          ...debug,
          updatedAt: new Date().toISOString(),
        }),
      );
    }

    if (request.method === "GET" && url.pathname === "/api/briefs") {
      const signals = await fetchAgendaSignals();
      const briefs = await buildCaseBriefs(signals);
      return withSecurityHeaders(
        Response.json({
          briefs,
          source: AGENDA_CENTER_URL,
          updatedAt: new Date().toISOString(),
        }),
      );
    }

    if (request.method === "GET" && url.pathname === "/api/summary") {
      const signals = await fetchAgendaSignals();
      const briefs = await buildCaseBriefs(signals);
      return withSecurityHeaders(
        Response.json({
          summary: buildSummaryMetrics(SITES, signals, briefs),
          updatedAt: new Date().toISOString(),
        }),
      );
    }

    if (request.method === "GET" && url.pathname === "/api/strategic-briefs") {
      const limitParam = Number(url.searchParams.get("limit") ?? "10");
      const briefs = env.OPPORTUNITYDB
        ? await listStrategicBriefs(env.OPPORTUNITYDB, limitParam)
        : [];
      return withSecurityHeaders(
        Response.json({
          briefs,
          count: briefs.length,
          updatedAt: new Date().toISOString(),
        }),
      );
    }

    if (request.method === "GET" && url.pathname === "/ingest-info") {
      return withSecurityHeaders(
        Response.json(
          {
            enabled: Boolean(env.OPPORTUNITYDB),
            detail: env.OPPORTUNITYDB
              ? "D1 binding is available for automated parcel ingest, parcel matching, stored strategic briefs, and parcel-context screening including flood, wetlands, groundwater, and utility context."
              : "D1 binding is not configured yet.",
            nextStep: env.OPPORTUNITYDB
              ? "Manual and scheduled ingest now refresh Danvers parcel records, build agenda briefs, match them to parcels, screen brief-linked parcels against assessor, flood, wetlands, groundwater, and utility context, and save a strategic brief for the dashboard."
              : "Attach D1 and persist case briefs, then map briefs to parcels, corridors, and recurring strategic briefs.",
          },
          { status: 200 },
        ),
      );
    }

    if (request.method === "POST" && url.pathname === "/api/parcels/upsert") {
      if (!env.OPPORTUNITYDB) {
        return withSecurityHeaders(missingDatabaseResponse());
      }

      const body = await readJson<{ parcels?: ParcelUpsertInput[] }>(request);
      if (!Array.isArray(body.parcels)) {
        return withSecurityHeaders(
          Response.json(
            { ok: false, error: "Request body must include a parcels array." },
            { status: 400 },
          ),
        );
      }

      await upsertParcels(env.OPPORTUNITYDB, body.parcels);

      return withSecurityHeaders(
        Response.json({
          ok: true,
          ingested: body.parcels.length,
        }),
      );
    }

    if (request.method === "POST" && url.pathname === "/api/opportunities/match") {
      if (!env.OPPORTUNITYDB) {
        return withSecurityHeaders(missingDatabaseResponse());
      }

      const body = await readJson<{ opportunities?: OpportunityParcelInput[] }>(request);
      if (!Array.isArray(body.opportunities)) {
        return withSecurityHeaders(
          Response.json(
            { ok: false, error: "Request body must include an opportunities array." },
            { status: 400 },
          ),
        );
      }

      const results = await matchAndPersistOpportunities(env.OPPORTUNITYDB, body.opportunities);

      return withSecurityHeaders(
        Response.json({
          ok: true,
          matched: results.filter((result) => result.matchType !== "no_match").length,
          reviewNeeded: results.filter((result) => result.needsReview).length,
          results,
        }),
      );
    }

    if (request.method === "GET" && url.pathname === "/api/opportunities/review") {
      if (!env.OPPORTUNITYDB) {
        return withSecurityHeaders(missingDatabaseResponse());
      }

      const limitParam = Number(url.searchParams.get("limit") ?? "25");
      const needsReviewOnly = url.searchParams.get("needsReviewOnly") !== "false";
      const queue = await listParcelReviewQueue(env.OPPORTUNITYDB, {
        limit: Number.isFinite(limitParam) ? limitParam : 25,
        needsReviewOnly,
      });

      return withSecurityHeaders(
        Response.json({
          ok: true,
          count: queue.length,
          needsReviewOnly,
          results: queue,
        }),
      );
    }

    if (request.method === "GET" && url.pathname === "/api/debug/parcel-lookup") {
      if (!env.OPPORTUNITYDB) {
        return withSecurityHeaders(missingDatabaseResponse());
      }

      const address = url.searchParams.get("address")?.trim() ?? "";
      if (!address) {
        return withSecurityHeaders(
          Response.json(
            { ok: false, error: "Query string must include an address parameter." },
            { status: 400 },
          ),
        );
      }

      const result = await debugLookupParcelAddress(env.OPPORTUNITYDB, address);
      return withSecurityHeaders(
        Response.json({
          ok: true,
          ...result,
        }),
      );
    }

    if (request.method === "POST" && url.pathname === "/ingest") {
      if (!env.OPPORTUNITYDB) {
        return withSecurityHeaders(
          Response.json(
            {
              accepted: false,
              message: buildIngestMessage("manual"),
              detail: "Queue and database bindings are not configured yet.",
            },
            { status: 501 },
          ),
        );
      }

      const runId = await startIngestionRun(env.OPPORTUNITYDB, "manual");

      try {
        const summary = await runAutomaticIngest(env.OPPORTUNITYDB);
        const strategicBrief = await createAndPersistStrategicBrief(env.OPPORTUNITYDB, "manual");
        await finishIngestionRun(env.OPPORTUNITYDB, runId, "completed");

        return withSecurityHeaders(
          Response.json({
            accepted: true,
            message: buildIngestMessage("manual"),
            detail:
              "Manual ingest completed: Danvers parcels refreshed, case briefs rebuilt, opportunities matched, and a strategic brief was saved for the dashboard.",
            summary,
            strategicBrief,
          }),
        );
      } catch (error) {
        await finishIngestionRun(env.OPPORTUNITYDB, runId, "failed");

        return withSecurityHeaders(
          Response.json(
            {
              accepted: false,
              message: buildIngestMessage("manual"),
              error: error instanceof Error ? error.message : "Ingest failed.",
            },
            { status: 500 },
          ),
        );
      }
    }

    return withSecurityHeaders(new Response("Not Found", { status: 404 }));
  },

  async scheduled(controller: { cron: string }, env: Env): Promise<void> {
    const timestamp = new Date().toISOString();

    if (env.OPPORTUNITYDB) {
      const runId = await startIngestionRun(env.OPPORTUNITYDB, "scheduled");

      try {
        const summary = await runAutomaticIngest(env.OPPORTUNITYDB);
        const strategicBrief = await createAndPersistStrategicBrief(env.OPPORTUNITYDB, "scheduled");
        await finishIngestionRun(env.OPPORTUNITYDB, runId, "completed");

        console.log(
          JSON.stringify({
            event: "scheduled-run",
            cron: controller.cron,
            at: timestamp,
            databaseConfigured: true,
            summary,
            strategicBriefGeneratedAt: strategicBrief.generatedAt,
          }),
        );
        return;
      } catch (error) {
        await finishIngestionRun(env.OPPORTUNITYDB, runId, "failed");

        console.error(
          JSON.stringify({
            event: "scheduled-run-failed",
            cron: controller.cron,
            at: timestamp,
            databaseConfigured: true,
            error: error instanceof Error ? error.message : "Unknown ingest failure",
          }),
        );
        throw error;
      }
    }

    console.log(
      JSON.stringify({
        event: "scheduled-run",
        cron: controller.cron,
        at: timestamp,
        databaseConfigured: Boolean(env.OPPORTUNITYDB),
      }),
    );
  },
}
