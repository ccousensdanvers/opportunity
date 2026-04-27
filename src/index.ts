type IngestTrigger = "manual" | "scheduled";
type SiteStatus = "Needs staff review" | "Policy setup" | "Market scan" | "Infrastructure check";
type Readiness = "Early" | "Emerging" | "Advancing";

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
    title: "Worker deployment stabilized",
    detail: "Cloudflare Worker now serves an internal dashboard instead of a raw bootstrap response.",
  },
  {
    time: "09:15",
    title: "Source connector added",
    detail: "The dashboard now pulls Planning Board and ZBA agenda postings directly from the Danvers Agenda Center.",
  },
  {
    time: "Next",
    title: "Case extraction layer",
    detail: "This build attempts lightweight packet parsing so staff can see likely sites and project clues, not just meeting titles.",
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

function buildStatusPayload() {
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
        database: false,
        queue: false,
      },
    },
  };
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

async function fetchAgendaSignals(): Promise<AgendaSignal[]> {
  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 4000);
    const response = await fetch(AGENDA_CENTER_URL, {
      signal: controller.signal,
      headers: {
        "user-agent": "Opportunity/0.1 (+https://www.danversma.gov/)",
      },
    });
    clearTimeout(timeout);

    if (!response.ok) {
      throw new Error(`Agenda Center request failed with ${response.status}`);
    }

    const html = await response.text();
    let currentBoard: string | null = null;
    let currentDate: string | null = null;
    const signals: AgendaSignal[] = [];
    const seen = new Set<string>();

    const boardCollector = new HeadingCollector((value) => {
      currentBoard = value || null;
      currentDate = null;
    });
    const dateCollector = new HeadingCollector((value) => {
      currentDate = value || null;
    });
    const linkCollector = new AgendaLinkCollector(
      () => currentBoard,
      () => currentDate,
      (signal) => {
        const key = `${signal.board}|${signal.meetingDate}|${signal.agendaUrl}`;
        if (!seen.has(key)) {
          seen.add(key);
          signals.push(signal);
        }
      },
    );

    await new HTMLRewriter()
      .on("h2", boardCollector)
      .on("h3", dateCollector)
      .on("a", linkCollector)
      .transform(new Response(html))
      .text();

    const filtered = signals
      .filter(
        (signal) =>
          signal.board === "Planning Board" || signal.board === "Zoning Board of Appeals",
      )
      .slice(0, 8);

    return filtered.length ? filtered : FALLBACK_SIGNALS;
  } catch {
    return FALLBACK_SIGNALS;
  }
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

    const bytes = await response.arrayBuffer();
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

function renderMetricMarkup(metrics: SummaryMetric[]): string {
  return metrics
    .map((metric, index) => {
      const toneClass = metric.tone ? `metric metric-${metric.tone}` : "metric";
      return `
        <section class="${toneClass}" style="animation-delay:${index * 70}ms">
          <p class="eyebrow">${escapeHtml(metric.label)}</p>
          <p class="metric-value">${escapeHtml(metric.value)}</p>
          <p class="metric-detail">${escapeHtml(metric.detail)}</p>
        </section>
      `;
    })
    .join("");
}

function renderActivityMarkup(items: ActivityItem[]): string {
  return items
    .map(
      (item) => `
        <li class="activity-item">
          <p class="activity-time">${escapeHtml(item.time)}</p>
          <div>
            <p class="activity-title">${escapeHtml(item.title)}</p>
            <p class="activity-detail">${escapeHtml(item.detail)}</p>
          </div>
        </li>
      `,
    )
    .join("");
}

function renderSignalMarkup(signals: AgendaSignal[]): string {
  return signals
    .map(
      (signal) => `
        <li class="signal-item">
          <div class="signal-meta">
            <span>${escapeHtml(signal.board)}</span>
            <span>${escapeHtml(signal.meetingDate)}</span>
          </div>
          <a class="signal-link" href="${escapeHtml(signal.agendaUrl)}" target="_blank" rel="noreferrer">
            ${escapeHtml(signal.title)}
          </a>
          <p class="signal-source">${escapeHtml(signal.source)}</p>
        </li>
      `,
    )
    .join("");
}

function renderBriefMarkup(briefs: CaseBrief[]): string {
  return briefs
    .map(
      (brief) => `
        <li class="brief-item">
          <div class="brief-topline">
            <span>${escapeHtml(brief.board)}</span>
            <span>${escapeHtml(brief.confidence)}</span>
          </div>
          <p class="brief-site">${escapeHtml(brief.likelySite)}</p>
          <p class="brief-type">${escapeHtml(brief.signalType)}</p>
          <p class="brief-rationale">${escapeHtml(brief.rationale)}</p>
          <div class="brief-meta">
            <span>${escapeHtml(brief.meetingDate)}</span>
            <a href="${escapeHtml(brief.agendaUrl)}" target="_blank" rel="noreferrer">Open agenda</a>
          </div>
        </li>
      `,
    )
    .join("");
}

function renderDashboard(payload: DashboardPayload): string {
  const generatedAt = new Date(payload.generatedAt).toLocaleString("en-US", {
    dateStyle: "medium",
    timeStyle: "short",
    timeZone: "America/New_York",
  });

  const initialTableRows = payload.sites
    .map(
      (item) => `
        <tr>
          <td>
            <strong>${escapeHtml(item.site)}</strong>
            <div class="cell-subtle">${escapeHtml(item.corridor)}</div>
          </td>
          <td>${escapeHtml(item.signal)}</td>
          <td>${escapeHtml(item.focus)}</td>
          <td>${escapeHtml(String(item.score))}</td>
          <td><span class="status-pill">${escapeHtml(item.status)}</span></td>
        </tr>
      `,
    )
    .join("");

  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Opportunity</title>
    <style>
      :root {
        --bg: #f3f1eb;
        --surface: rgba(255, 252, 247, 0.9);
        --surface-strong: rgba(255, 252, 247, 0.97);
        --ink: #1e2321;
        --muted: #5e655f;
        --line: rgba(34, 42, 38, 0.12);
        --warm: #af5e2f;
        --cool: #2c6a73;
        --accent: #16423c;
        --accent-soft: rgba(22, 66, 60, 0.08);
        --shadow: 0 24px 60px rgba(26, 36, 33, 0.08);
      }

      * { box-sizing: border-box; }

      body {
        margin: 0;
        min-height: 100vh;
        background:
          radial-gradient(circle at top left, rgba(175, 94, 47, 0.12), transparent 32%),
          linear-gradient(180deg, #f8f5ef 0%, var(--bg) 58%, #ece8df 100%);
        color: var(--ink);
        font-family: Georgia, "Times New Roman", serif;
      }

      a { color: inherit; text-decoration: none; }
      button, input { font: inherit; }

      .shell {
        display: grid;
        grid-template-columns: 260px minmax(0, 1fr);
        min-height: 100vh;
      }

      .rail {
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        padding: 32px 24px 28px;
        border-right: 1px solid var(--line);
        background: rgba(247, 243, 236, 0.8);
        backdrop-filter: blur(18px);
      }

      .brand {
        display: grid;
        gap: 14px;
      }

      .brand-mark {
        width: 44px;
        height: 44px;
        border-radius: 12px;
        display: grid;
        place-items: center;
        background: var(--accent);
        color: #f8f5ef;
        font-size: 18px;
      }

      .brand h1 {
        margin: 0;
        font-size: 2rem;
        font-weight: 600;
      }

      .brand p,
      .rail-footer {
        margin: 0;
        color: var(--muted);
        line-height: 1.5;
      }

      .nav-group {
        display: grid;
        gap: 8px;
        margin-top: 28px;
      }

      .nav-item {
        padding: 12px 14px;
        border-radius: 12px;
        color: var(--muted);
        transition: background 180ms ease, color 180ms ease, transform 180ms ease;
      }

      .nav-item.active,
      .nav-item:hover {
        background: var(--surface-strong);
        color: var(--ink);
        transform: translateX(2px);
      }

      .rail-footer {
        display: grid;
        gap: 8px;
        font-size: 0.92rem;
      }

      .main {
        padding: 28px 32px 36px;
      }

      .topbar {
        display: flex;
        align-items: flex-start;
        justify-content: space-between;
        gap: 18px;
        margin-bottom: 28px;
      }

      .topbar-copy {
        max-width: 760px;
      }

      .eyebrow {
        margin: 0 0 8px;
        color: var(--muted);
        font-size: 0.8rem;
        letter-spacing: 0.08em;
        text-transform: uppercase;
      }

      .topbar h2 {
        margin: 0;
        font-size: clamp(2rem, 4vw, 3.7rem);
        line-height: 0.96;
        font-weight: 600;
      }

      .topbar p {
        margin: 14px 0 0;
        max-width: 58ch;
        color: var(--muted);
        font-size: 1.04rem;
        line-height: 1.6;
      }

      .topbar-meta {
        min-width: 220px;
        padding-top: 8px;
        color: var(--muted);
        font-size: 0.95rem;
        text-align: right;
      }

      .metrics {
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 14px;
        margin-bottom: 18px;
      }

      .metric {
        min-height: 158px;
        padding: 18px 18px 20px;
        background: var(--surface);
        border: 1px solid var(--line);
        border-radius: 16px;
        box-shadow: var(--shadow);
        opacity: 0;
        transform: translateY(14px);
        animation: rise 560ms ease forwards;
      }

      .metric-warm {
        background: linear-gradient(180deg, rgba(175, 94, 47, 0.1), var(--surface-strong));
      }

      .metric-cool {
        background: linear-gradient(180deg, rgba(44, 106, 115, 0.12), var(--surface-strong));
      }

      .metric-value {
        margin: 0;
        font-size: clamp(2rem, 3vw, 2.8rem);
        line-height: 1;
      }

      .metric-detail {
        margin: 14px 0 0;
        color: var(--muted);
        line-height: 1.5;
      }

      .workspace {
        display: grid;
        grid-template-columns: minmax(0, 1.4fr) minmax(320px, 0.6fr);
        gap: 16px;
        align-items: start;
      }

      .panel {
        background: var(--surface);
        border: 1px solid var(--line);
        border-radius: 18px;
        box-shadow: var(--shadow);
      }

      .watchlist,
      .briefs,
      .signals,
      .activity,
      .insight-band {
        padding: 18px 20px 20px;
      }

      .watchlist-head,
      .panel-head {
        display: flex;
        align-items: flex-end;
        justify-content: space-between;
        gap: 16px;
        margin-bottom: 18px;
      }

      .watchlist-head h3,
      .panel-head h3,
      .insight-band h3 {
        margin: 0;
        font-size: 1.35rem;
        font-weight: 600;
      }

      .watchlist-head p,
      .panel-head p,
      .insight-band p {
        margin: 6px 0 0;
        color: var(--muted);
        line-height: 1.5;
      }

      .controls {
        display: grid;
        grid-template-columns: minmax(0, 1fr) auto auto;
        gap: 10px;
        margin-bottom: 14px;
      }

      .search,
      .select {
        width: 100%;
        min-height: 42px;
        padding: 0 14px;
        border: 1px solid var(--line);
        border-radius: 12px;
        background: rgba(255, 252, 247, 0.9);
        color: var(--ink);
      }

      table {
        width: 100%;
        border-collapse: collapse;
      }

      th,
      td {
        padding: 14px 0;
        text-align: left;
        vertical-align: top;
        border-top: 1px solid var(--line);
      }

      th {
        color: var(--muted);
        font-size: 0.78rem;
        letter-spacing: 0.06em;
        text-transform: uppercase;
      }

      td {
        font-size: 0.97rem;
        line-height: 1.5;
      }

      .cell-subtle {
        color: var(--muted);
        font-size: 0.92rem;
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

      .empty {
        padding: 22px 0 8px;
        color: var(--muted);
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

      .brief-rationale,
      .signal-source {
        margin: 8px 0 0;
        color: var(--muted);
        line-height: 1.5;
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
        color: var(--ink);
        line-height: 1.45;
      }

      .signal-link:hover,
      .brief-meta a:hover {
        color: var(--accent);
      }

      .activity-item {
        display: grid;
        grid-template-columns: 56px minmax(0, 1fr);
        gap: 14px;
      }

      .activity-time {
        margin: 0;
        color: var(--muted);
        font-size: 0.86rem;
        letter-spacing: 0.04em;
        text-transform: uppercase;
      }

      .activity-title {
        margin: 0;
        font-size: 1rem;
      }

      .activity-detail {
        margin: 8px 0 0;
        color: var(--muted);
        line-height: 1.55;
      }

      .insight-band {
        overflow: hidden;
        position: relative;
      }

      .insight-band::after {
        content: "";
        position: absolute;
        inset: auto -20% -20% 35%;
        height: 160px;
        background: linear-gradient(90deg, rgba(22, 66, 60, 0.05), rgba(175, 94, 47, 0.16));
        transform: rotate(-6deg);
      }

      .insight-grid {
        display: grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 12px;
        margin-top: 16px;
        position: relative;
        z-index: 1;
      }

      .insight {
        min-height: 108px;
        padding: 14px;
        border-radius: 14px;
        background: rgba(255, 252, 247, 0.74);
        border: 1px solid rgba(34, 42, 38, 0.08);
      }

      .insight strong {
        display: block;
        margin-bottom: 10px;
        font-size: 0.84rem;
        letter-spacing: 0.05em;
        text-transform: uppercase;
      }

      .insight p {
        margin: 0;
        color: var(--muted);
        line-height: 1.5;
      }

      @keyframes rise {
        to {
          opacity: 1;
          transform: translateY(0);
        }
      }

      @media (max-width: 1100px) {
        .shell {
          grid-template-columns: 1fr;
        }

        .rail {
          gap: 22px;
          border-right: 0;
          border-bottom: 1px solid var(--line);
        }

        .metrics,
        .insight-grid {
          grid-template-columns: 1fr 1fr;
        }

        .workspace,
        .controls {
          grid-template-columns: 1fr;
        }

        .topbar {
          flex-direction: column;
        }

        .topbar-meta {
          text-align: left;
        }
      }

      @media (max-width: 760px) {
        .main,
        .rail {
          padding: 22px 18px;
        }

        .metrics,
        .insight-grid,
        .nav-group {
          grid-template-columns: 1fr;
        }

        .activity-item {
          grid-template-columns: 1fr;
        }

        th:nth-child(3),
        td:nth-child(3) {
          display: none;
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
              This version now includes one live public source feed from Danvers itself plus a lightweight case-extraction layer.
              The tool is beginning to move from meeting notices toward site-specific review leads.
            </p>
          </div>
          <div class="topbar-meta">
            <div>Worker name: <strong>opportunity</strong></div>
            <div>Mode: dashboard plus case briefs</div>
            <div>Data store: seeded watchlist, live public feed</div>
          </div>
        </header>

        <section class="metrics" aria-label="Top metrics" id="summary-metrics">
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
              <tbody id="watchlist-body">
                ${initialTableRows}
              </tbody>
            </table>
            <div id="empty-state" class="empty" hidden>No sites match the current filter.</div>
          </div>

          <div class="side-stack">
            <section class="panel briefs">
              <div class="panel-head">
                <div>
                  <p class="eyebrow">Case Briefs</p>
                  <h3>Likely site signals</h3>
                  <p>These briefs combine live agenda postings with simple packet parsing to pull out likely locations and project types.</p>
                </div>
              </div>
              <ul id="brief-list" class="brief-list">
                ${renderBriefMarkup(payload.briefs)}
              </ul>
            </section>

            <section class="panel signals">
              <div class="panel-head">
                <div>
                  <p class="eyebrow">Live Source Feed</p>
                  <h3>Recent agenda postings</h3>
                  <p>Direct from Danvers Agenda Center. These are the earliest reliable public signals now wired into the tool.</p>
                </div>
              </div>
              <ul id="signal-list" class="signal-list">
                ${renderSignalMarkup(payload.signals)}
              </ul>
            </section>

            <section class="panel activity">
              <div class="panel-head">
                <div>
                  <p class="eyebrow">Activity</p>
                  <h3>Build notes</h3>
                  <p>What the system is doing now and what comes next.</p>
                </div>
              </div>
              <ul class="activity-list">
                ${renderActivityMarkup(payload.activity)}
              </ul>
            </section>
          </div>
        </section>

        <section class="panel insight-band" style="margin-top:16px;">
          <p class="eyebrow">Decision Support</p>
          <h3>What this next layer unlocks</h3>
          <p>Danvers now has not only a live feed of development-relevant public meetings, but also first-pass briefs that can be triaged into parcels, corridors, and follow-up actions.</p>
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
              <p>Staff can begin sorting which postings are merely administrative and which may deserve zoning, infrastructure, or redevelopment attention.</p>
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

      fetch("/api/signals")
        .then((response) => response.json())
        .then((data) => {
          if (Array.isArray(data.signals)) {
            initialData.signals = data.signals;
            renderSignals(initialData.signals);
          }
        })
        .catch(() => {
          // Keep the server-rendered signal list in place if refresh fails.
        });

      fetch("/api/briefs")
        .then((response) => response.json())
        .then((data) => {
          if (Array.isArray(data.briefs)) {
            initialData.briefs = data.briefs;
            renderBriefs(initialData.briefs);
          }
        })
        .catch(() => {
          // Keep the server-rendered case briefs in place if refresh fails.
        });
    </script>
  </body>
</html>`;
}

export default {
  async fetch(request: Request): Promise<Response> {
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
      return Response.json(buildStatusPayload());
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
          enabled: false,
          detail: "Queue and database bindings are not configured yet.",
          nextStep: "Attach D1 and persist case briefs, then map briefs to parcels and corridors.",
        },
        { status: 200 },
      );
    }

    if (request.method === "POST" && url.pathname === "/ingest") {
      return Response.json(
        {
          accepted: false,
          message: buildIngestMessage("manual"),
          detail: "Queue and database bindings are not configured yet.",
        },
        { status: 501 },
      );
    }

    return new Response("Not Found", { status: 404 });
  },

  async scheduled(controller: { cron: string }): Promise<void> {
    console.log(
      JSON.stringify({
        event: "scheduled-run",
        cron: controller.cron,
        at: new Date().toISOString(),
      }),
    );
  },
};
