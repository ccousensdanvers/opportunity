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
const PLANNING_BOARD_AGENDA_URL = "https://www.danversma.gov/AgendaCenter/Planning-Board-11";
const ZBA_AGENDA_URL = "https://www.danversma.gov/AgendaCenter/Zoning-Board-of-Appeals-18";
const DANVERS_PARCELS_LAYER_URL =
  "https://gis.danversma.gov/danversexternal/rest/services/DanversMA_Parcels_AGOL/MapServer/1/query";
const DANVERS_PARCELS_PAGE_SIZE = 1000;
const INGEST_PARCEL_SAMPLE_LIMIT = 100;

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

    const html = await response.text();
    const signals: AgendaSignal[] = [];
    const seen = new Set<string>();
    const dateBlocks = Array.from(
      html.matchAll(/<h3[^>]*>\s*(?:<[^>]+>\s*)*([^<]+?)\s*(?:<\/[^>]+>\s*)*<\/h3>([\s\S]*?)(?=<h3\b|$)/gi),
    );
    const agendaHrefCount = Array.from(
      html.matchAll(/href=['"][^'"]*\/AgendaCenter\/ViewFile\/Agenda\/[^'"]+['"]/gi),
    ).length;

    for (const [, rawDate, block] of dateBlocks) {
      const meetingDate = normalizeWhitespace(rawDate);
      if (!meetingDate) {
        continue;
      }

      const agendaUrlMatch = block.match(
        /href=['"]([^'"]*\/AgendaCenter\/ViewFile\/Agenda\/[^'"]+)['"]/i,
      );
      if (!agendaUrlMatch) {
        continue;
      }

      const titleCandidates = Array.from(
        block.matchAll(/<a\b[^>]*>([\s\S]*?)<\/a>/gi),
      )
        .map((match) => normalizeWhitespace(match[1].replace(/<[^>]+>/g, " ")))
        .filter((value) => value && value !== "Agenda" && value !== "Previous Versions");

      const title = titleCandidates[0] ?? board;
      const agendaUrl = agendaUrlMatch[1].startsWith("http")
        ? agendaUrlMatch[1]
        : `https://www.danversma.gov${agendaUrlMatch[1]}`;

      if (!title) {
        continue;
      }

      if (title.toLowerCase().includes("cancel")) {
        continue;
      }

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
        source: "danvers agenda center",
      });
    }

    return {
      board,
      url,
      ok: true,
      status: response.status,
      parsedCount: signals.length,
      signals: signals.slice(0, 8),
      h3Count: dateBlocks.length,
      agendaHrefCount,
      htmlSample: html.slice(0, 1200),
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
  const [planningSignals, zbaSignals] = await Promise.all([
    fetchAgendaSignalsForBoard("Planning Board", PLANNING_BOARD_AGENDA_URL),
    fetchAgendaSignalsForBoard("Zoning Board of Appeals", ZBA_AGENDA_URL),
  ]);

  const signals = [...planningSignals, ...zbaSignals]
    .sort((left, right) => right.meetingDate.localeCompare(left.meetingDate))
    .slice(0, 8);

  return signals.length ? signals : FALLBACK_SIGNALS;
}

async function fetchAgendaSignalsDebug() {
  const [planningBoard, zoningBoard] = await Promise.all([
    fetchAgendaSignalsForBoardWithDebug("Planning Board", PLANNING_BOARD_AGENDA_URL),
    fetchAgendaSignalsForBoardWithDebug("Zoning Board of Appeals", ZBA_AGENDA_URL),
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
      const packetText = await fetchPacketText(signal.agendaUrl);
      const addresses = extractAddresses(packetText);
      const likelySite = addresses[0] ?? `${signal.board} agenda item`;
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

async function fetchDanversParcels(): Promise<ParcelUpsertInput[]> {
  const parcels: ParcelUpsertInput[] = [];
  const seenMapLots = new Set<string>();
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

function buildOpportunityInputs(
  briefs: CaseBrief[],
  parcels: ParcelUpsertInput[],
): OpportunityParcelInput[] {
  const derivedFromBriefs = briefs.map((brief) => ({
    id: brief.id,
    address: brief.addresses[0] ?? null,
  }));

  const seededParcelTests = parcels
    .filter((parcel) => parcel.address)
    .slice(0, 3)
    .map((parcel, index) => ({
      id: `seeded-parcel-test-${index + 1}`,
      address: parcel.address,
    }));

  return [...derivedFromBriefs, ...seededParcelTests];
}

async function runAutomaticIngest(db: D1Database): Promise<IngestRunSummary> {
  const parcels = await fetchDanversParcels();
  await upsertParcels(db, parcels);

  const signals = await fetchAgendaSignals();
  const briefs = await buildCaseBriefs(signals);
  const opportunities = buildOpportunityInputs(briefs, parcels);
  const results = await matchAndPersistOpportunities(db, opportunities);

  return {
    parcelsIngested: parcels.length,
    opportunitiesPrepared: opportunities.length,
    matched: results.filter((result) => result.matchType !== "no_match").length,
    reviewNeeded: results.filter((result) => result.needsReview).length,
  };
}

async function buildDashboardPayload(signals: AgendaSignal[]): Promise<DashboardPayload> {
  const briefs = await buildCaseBriefs(signals);

  return {
    generatedAt: new Date().toISOString(),
    summary: buildSummaryMetrics(SITES, signals, briefs),
    sites: SITES,
    activity: ACTIVITIES,
    signals,
    briefs,
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

function renderTableRows(sites: OpportunitySite[]): string {
  return sites
    .map(
      (site) => `
        <tr>
          <td><strong>${escapeHtml(site.site)}</strong><div class="cell-subtle">${escapeHtml(site.corridor)}</div></td>
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
      (brief) => `
        <li class="brief-item">
          <div class="brief-topline"><span>${escapeHtml(brief.board)}</span><span>${escapeHtml(brief.confidence)}</span></div>
          <p class="brief-site">${escapeHtml(brief.likelySite)}</p>
          <p class="brief-type">${escapeHtml(brief.signalType)}</p>
          <p class="brief-rationale">${escapeHtml(brief.rationale)}</p>
          <div class="brief-meta"><span>${escapeHtml(brief.meetingDate)}</span><a href="${escapeHtml(brief.agendaUrl)}" target="_blank" rel="noreferrer">Open agenda</a></div>
        </li>`,
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
          <p class="signal-source">${escapeHtml(signal.source)}</p>
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

function renderDashboard(payload: DashboardPayload): string {
  const generatedAt = new Date(payload.generatedAt).toLocaleString("en-US", {
    dateStyle: "medium",
    timeStyle: "short",
  });
  const initialTableRows = renderTableRows(payload.sites);

  return `<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Opportunity</title>
    <style>
      :root {
        --bg: #f5f1e8;
        --panel: #fffaf2;
        --ink: #1f2a27;
        --muted: #5e6a67;
        --line: rgba(31, 42, 39, 0.12);
        --accent: #16423c;
        --accent-soft: rgba(22, 66, 60, 0.1);
        --warm: #af5e2f;
        --warm-soft: rgba(175, 94, 47, 0.12);
      }

      * {
        box-sizing: border-box;
      }

      body {
        margin: 0;
        font-family: Georgia, "Times New Roman", serif;
        background: linear-gradient(180deg, #f7f2e9 0%, #efe7da 100%);
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
        background: rgba(255, 250, 242, 0.9);
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
        background: rgba(255, 250, 242, 0.82);
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
        background: linear-gradient(180deg, #fffaf2, #f8ecdf);
      }

      .metric.cool {
        background: linear-gradient(180deg, #fffaf2, #eef5f3);
      }

      .metric-label,
      .metric-detail {
        color: var(--muted);
      }

      .metric-value {
        margin: 10px 0 8px;
        font-size: 2rem;
      }

      .workspace {
        grid-template-columns: minmax(0, 1.3fr) minmax(320px, 0.9fr);
        align-items: start;
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
      .activity-list {
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

      @media (max-width: 1100px) {
        .shell,
        .topbar,
        .workspace,
        .controls,
        .metrics,
        .insight-grid {
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

        <section class="panel" style="margin-top: 16px;">
          <p class="eyebrow">Decision Support</p>
          <h3>What this next layer unlocks</h3>
          <div class="insight-grid">
            <div class="insight">
              <strong>Early Read</strong>
              <p>Board packets can now be screened for address-like patterns and common land-use terms before staff opens every file manually.</p>
            </div>
            <div class="insight">
              <strong>Targeting</strong>
              <p>Likely sites and project types create a cleaner bridge to future parcel matching, D1 storage, and explainable alert scoring.</p>
            </div>
            <div class="insight">
              <strong>Town Action</strong>
              <p>Staff can sort which postings are administrative and which may deserve zoning, infrastructure, or redevelopment attention.</p>
            </div>
          </div>
        </section>
      </main>
    </div>
    <script>
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
            '<td><strong>' + escapeHtml(item.site) + '</strong><div class="cell-subtle">' + escapeHtml(item.corridor) + '</div></td>' +
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
          return '<li class="brief-item">' +
            '<div class="brief-topline"><span>' + escapeHtml(brief.board) + '</span><span>' + escapeHtml(brief.confidence) + '</span></div>' +
            '<p class="brief-site">' + escapeHtml(brief.likelySite) + '</p>' +
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

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (request.method === "GET" && url.pathname === "/") {
      const signals = await fetchAgendaSignals();
      const payload = await buildDashboardPayload(signals);
      return new Response(renderDashboard(payload), {
        headers: {
          "content-type": "text/html; charset=utf-8",
        },
      });
    }

    if (request.method === "GET" && url.pathname === "/api/status") {
      return Response.json(buildStatusPayload(env));
    }

    if (request.method === "GET" && url.pathname === "/api/sites") {
      return Response.json({
        sites: SITES,
        count: SITES.length,
      });
    }

    if (request.method === "GET" && url.pathname === "/api/signals") {
      const signals = await fetchAgendaSignals();
      return Response.json({
        signals,
        source: AGENDA_CENTER_URL,
        updatedAt: new Date().toISOString(),
      });
    }

    if (request.method === "GET" && url.pathname === "/api/debug/signals") {
      const debug = await fetchAgendaSignalsDebug();
      return Response.json({
        ...debug,
        updatedAt: new Date().toISOString(),
      });
    }

    if (request.method === "GET" && url.pathname === "/api/briefs") {
      const signals = await fetchAgendaSignals();
      const briefs = await buildCaseBriefs(signals);
      return Response.json({
        briefs,
        source: AGENDA_CENTER_URL,
        updatedAt: new Date().toISOString(),
      });
    }

    if (request.method === "GET" && url.pathname === "/api/summary") {
      const signals = await fetchAgendaSignals();
      const briefs = await buildCaseBriefs(signals);
      return Response.json({
        summary: buildSummaryMetrics(SITES, signals, briefs),
        updatedAt: new Date().toISOString(),
      });
    }

    if (request.method === "GET" && url.pathname === "/ingest-info") {
      return Response.json(
        {
          enabled: Boolean(env.OPPORTUNITYDB),
          detail: env.OPPORTUNITYDB
            ? "D1 binding is available for automated parcel ingest and parcel matching."
            : "D1 binding is not configured yet.",
          nextStep: env.OPPORTUNITYDB
            ? "Manual and scheduled ingest now refresh Danvers parcel records, build agenda briefs, and match them to parcels."
            : "Attach D1 and persist case briefs, then map briefs to parcels and corridors.",
        },
        { status: 200 },
      );
    }

    if (request.method === "POST" && url.pathname === "/api/parcels/upsert") {
      if (!env.OPPORTUNITYDB) {
        return missingDatabaseResponse();
      }

      const body = await readJson<{ parcels?: ParcelUpsertInput[] }>(request);
      if (!Array.isArray(body.parcels)) {
        return Response.json(
          { ok: false, error: "Request body must include a parcels array." },
          { status: 400 },
        );
      }

      await upsertParcels(env.OPPORTUNITYDB, body.parcels);

      return Response.json({
        ok: true,
        ingested: body.parcels.length,
      });
    }

    if (request.method === "POST" && url.pathname === "/api/opportunities/match") {
      if (!env.OPPORTUNITYDB) {
        return missingDatabaseResponse();
      }

      const body = await readJson<{ opportunities?: OpportunityParcelInput[] }>(request);
      if (!Array.isArray(body.opportunities)) {
        return Response.json(
          { ok: false, error: "Request body must include an opportunities array." },
          { status: 400 },
        );
      }

      const results = await matchAndPersistOpportunities(env.OPPORTUNITYDB, body.opportunities);

      return Response.json({
        ok: true,
        matched: results.filter((result) => result.matchType !== "no_match").length,
        reviewNeeded: results.filter((result) => result.needsReview).length,
        results,
      });
    }

    if (request.method === "GET" && url.pathname === "/api/opportunities/review") {
      if (!env.OPPORTUNITYDB) {
        return missingDatabaseResponse();
      }

      const limitParam = Number(url.searchParams.get("limit") ?? "25");
      const needsReviewOnly = url.searchParams.get("needsReviewOnly") !== "false";
      const queue = await listParcelReviewQueue(env.OPPORTUNITYDB, {
        limit: Number.isFinite(limitParam) ? limitParam : 25,
        needsReviewOnly,
      });

      return Response.json({
        ok: true,
        count: queue.length,
        needsReviewOnly,
        results: queue,
      });
    }

    if (request.method === "POST" && url.pathname === "/ingest") {
      if (!env.OPPORTUNITYDB) {
        return Response.json(
          {
            accepted: false,
            message: buildIngestMessage("manual"),
            detail: "Queue and database bindings are not configured yet.",
          },
          { status: 501 },
        );
      }

      const runId = await startIngestionRun(env.OPPORTUNITYDB, "manual");

      try {
        const summary = await runAutomaticIngest(env.OPPORTUNITYDB);
        await finishIngestionRun(env.OPPORTUNITYDB, runId, "completed");

        return Response.json({
          accepted: true,
          message: buildIngestMessage("manual"),
          detail:
            "Manual ingest completed: Danvers parcels refreshed, case briefs rebuilt, and opportunities matched.",
          summary,
        });
      } catch (error) {
        await finishIngestionRun(env.OPPORTUNITYDB, runId, "failed");

        return Response.json(
          {
            accepted: false,
            message: buildIngestMessage("manual"),
            error: error instanceof Error ? error.message : "Ingest failed.",
          },
          { status: 500 },
        );
      }
    }

    return new Response("Not Found", { status: 404 });
  },

  async scheduled(controller: { cron: string }, env: Env): Promise<void> {
    const timestamp = new Date().toISOString();

    if (env.OPPORTUNITYDB) {
      const runId = await startIngestionRun(env.OPPORTUNITYDB, "scheduled");

      try {
        const summary = await runAutomaticIngest(env.OPPORTUNITYDB);
        await finishIngestionRun(env.OPPORTUNITYDB, runId, "completed");

        console.log(
          JSON.stringify({
            event: "scheduled-run",
            cron: controller.cron,
            at: timestamp,
            databaseConfigured: true,
            summary,
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
