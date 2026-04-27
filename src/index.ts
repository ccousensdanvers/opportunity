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
    title: "Initial data model added",
    detail: "The dashboard now runs off reusable site records and summary calculations instead of hard-coded tiles.",
  },
  {
    time: "Next",
    title: "First source connector",
    detail: "Next build should swap seeded records for one public source feed with explainable alert logic.",
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

function buildSummaryMetrics(sites: OpportunitySite[]): SummaryMetric[] {
  const freshSignals = sites.filter((site) => site.updatedAt >= "2026-04-24").length;
  const advancing = sites.filter((site) => site.readiness === "Advancing").length;
  const averageScore = Math.round(sites.reduce((sum, site) => sum + site.score, 0) / sites.length);
  const corridors = new Set(sites.map((site) => site.corridor)).size;

  return [
    {
      label: "Priority Sites",
      value: String(sites.length),
      detail: "active properties or corridor segments under review",
      tone: "warm",
    },
    {
      label: "Fresh Signals",
      value: String(freshSignals),
      detail: "records updated in the last few days",
      tone: "cool",
    },
    {
      label: "Advancing",
      value: String(advancing),
      detail: "opportunities with clearer near-term action paths",
    },
    {
      label: "Average Score",
      value: String(averageScore),
      detail: `${corridors} corridors represented in the current watchlist`,
    },
  ];
}

function buildDashboardPayload() {
  return {
    generatedAt: new Date().toISOString(),
    summary: buildSummaryMetrics(SITES),
    sites: SITES,
    activity: ACTIVITIES,
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

function renderDashboard(): string {
  const payload = buildDashboardPayload();
  const generatedAt = new Date(payload.generatedAt).toLocaleString("en-US", {
    dateStyle: "medium",
    timeStyle: "short",
    timeZone: "America/New_York",
  });

  const initialTableRows = SITES.map(
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
  ).join("");

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

      a {
        color: inherit;
        text-decoration: none;
      }

      button, input {
        font: inherit;
      }

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
      .rail-footer,
      .subtle {
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
        grid-template-columns: minmax(0, 1.4fr) minmax(300px, 0.6fr);
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
      .activity,
      .insight-band {
        padding: 18px 20px 20px;
      }

      .watchlist-head,
      .activity-head {
        display: flex;
        align-items: flex-end;
        justify-content: space-between;
        gap: 16px;
        margin-bottom: 18px;
      }

      .watchlist-head h3,
      .activity-head h3,
      .insight-band h3 {
        margin: 0;
        font-size: 1.35rem;
        font-weight: 600;
      }

      .watchlist-head p,
      .activity-head p,
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

      .activity-list {
        display: grid;
        gap: 16px;
        padding: 0;
        margin: 0;
        list-style: none;
      }

      .activity-item {
        display: grid;
        grid-template-columns: 56px minmax(0, 1fr);
        gap: 14px;
        padding-top: 16px;
        border-top: 1px solid var(--line);
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
            <a class="nav-item" href="/api/status">System Status</a>
            <a class="nav-item" href="/ingest-info">Ingestion</a>
          </nav>
        </div>
        <div class="rail-footer">
          <span>Shell generated ${escapeHtml(generatedAt)}</span>
          <span>Next step: connect one public source and replace seeded records with explainable live signals.</span>
        </div>
      </aside>
      <main class="main">
        <header class="topbar">
          <div class="topbar-copy">
            <p class="eyebrow">Staff Dashboard</p>
            <h2>See where change may be becoming opportunity.</h2>
            <p>
              This version is still lightweight, but it now behaves like a real internal workspace:
              the summary, table, and filters all run off the same site dataset and API routes.
            </p>
          </div>
          <div class="topbar-meta">
            <div>Worker name: <strong>opportunity</strong></div>
            <div>Mode: dashboard shell plus APIs</div>
            <div>Data store: seeded records</div>
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
                <p>The controls below let staff narrow the list by corridor, status, or keyword before connectors are live.</p>
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
            <section class="panel activity">
              <div class="activity-head">
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

            <section class="panel insight-band">
              <p class="eyebrow">Decision Support</p>
              <h3>What this version can already do</h3>
              <p>Give Danvers staff one place to scan seeded opportunities, sort by score, and review which corridors deserve follow-up first.</p>
              <div class="insight-grid">
                <div class="insight">
                  <strong>Tax Base</strong>
                  <p>Flag sites where turnover, reuse, or vacancy could affect long-term valuation and business activity.</p>
                </div>
                <div class="insight">
                  <strong>Readiness</strong>
                  <p>Separate early ideas from sites that look closer to realistic intervention or market traction.</p>
                </div>
                <div class="insight">
                  <strong>Action</strong>
                  <p>Keep the interface ready for explainable source-driven alerts instead of black-box recommendations.</p>
                </div>
              </div>
            </section>
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

      searchInput.addEventListener("input", applyFilters);
      corridorFilter.addEventListener("change", applyFilters);
      statusFilter.addEventListener("change", applyFilters);

      fetch("/api/sites")
        .then((response) => response.json())
        .then((data) => {
          if (Array.isArray(data.sites)) {
            initialData.sites = data.sites;
            populateFilters(initialData.sites);
            renderRows(initialData.sites);
          }
        })
        .catch(() => {
          // Keep the server-rendered seed data in place if the fetch fails.
        });
    </script>
  </body>
</html>`;
}

export default {
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    if (request.method === "GET" && url.pathname === "/") {
      return new Response(renderDashboard(), {
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

    if (request.method === "GET" && url.pathname === "/api/summary") {
      return Response.json({
        summary: buildSummaryMetrics(SITES),
        updatedAt: new Date().toISOString(),
      });
    }

    if (request.method === "GET" && url.pathname === "/ingest-info") {
      return Response.json(
        {
          enabled: false,
          detail: "Queue and database bindings are not configured yet.",
          nextStep: "Attach D1 and the first public source connector.",
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
