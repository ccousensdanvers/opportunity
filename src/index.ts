type IngestTrigger = "manual" | "scheduled";

interface IngestMessage {
  trigger: IngestTrigger;
  requestedAt: string;
}

interface DashboardMetric {
  label: string;
  value: string;
  detail: string;
  tone?: "neutral" | "warm" | "cool";
}

interface WatchItem {
  site: string;
  signal: string;
  focus: string;
  status: string;
}

interface ActivityItem {
  time: string;
  title: string;
  detail: string;
}

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

function renderDashboard(): string {
  const generatedAt = new Date().toLocaleString("en-US", {
    dateStyle: "medium",
    timeStyle: "short",
    timeZone: "America/New_York",
  });

  const metrics: DashboardMetric[] = [
    {
      label: "Priority Sites",
      value: "12",
      detail: "active properties under staff watch",
      tone: "warm",
    },
    {
      label: "Fresh Signals",
      value: "4",
      detail: "items needing review this week",
      tone: "cool",
    },
    {
      label: "Redevelopment Corridors",
      value: "3",
      detail: "Cabot, Endicott, and Route 114",
    },
    {
      label: "Readiness",
      value: "Bootstrap",
      detail: "worker live, data connectors pending",
    },
  ];

  const watchItems: WatchItem[] = [
    {
      site: "Maple Street Industrial Edge",
      signal: "ownership change watch",
      focus: "access, parcel assembly, reuse fit",
      status: "Needs staff review",
    },
    {
      site: "Downtown Upper Floors",
      signal: "small-scale adaptive reuse",
      focus: "code path, mixed-use economics",
      status: "Policy setup",
    },
    {
      site: "Route 114 Retail Cluster",
      signal: "tenant turnover",
      focus: "tax base retention, repositioning",
      status: "Market scan",
    },
    {
      site: "Endicott Corridor Flex Space",
      signal: "industrial demand pressure",
      focus: "site readiness, utilities, zoning",
      status: "Infrastructure check",
    },
  ];

  const activities: ActivityItem[] = [
    {
      time: "08:30",
      title: "Worker deployment stabilized",
      detail: "Cloudflare Worker now serves the dashboard shell instead of a raw bootstrap response.",
    },
    {
      time: "09:15",
      title: "Migration scaffold prepared",
      detail: "Initial tables exist for parcels, sites, events, alerts, notes, and ingestion runs.",
    },
    {
      time: "Next",
      title: "Source connector pass",
      detail: "Next build should add one public signal feed and persist explainable alerts.",
    },
  ];

  const metricMarkup = metrics
    .map((metric) => {
      const toneClass = metric.tone ? `metric metric-${metric.tone}` : "metric";
      return `
        <section class="${toneClass}">
          <p class="eyebrow">${metric.label}</p>
          <p class="metric-value">${metric.value}</p>
          <p class="metric-detail">${metric.detail}</p>
        </section>
      `;
    })
    .join("");

  const watchMarkup = watchItems
    .map(
      (item) => `
        <tr>
          <td>${item.site}</td>
          <td>${item.signal}</td>
          <td>${item.focus}</td>
          <td><span class="status-pill">${item.status}</span></td>
        </tr>
      `,
    )
    .join("");

  const activityMarkup = activities
    .map(
      (item) => `
        <li class="activity-item">
          <p class="activity-time">${item.time}</p>
          <div>
            <p class="activity-title">${item.title}</p>
            <p class="activity-detail">${item.detail}</p>
          </div>
        </li>
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
        --surface: rgba(255, 252, 247, 0.88);
        --surface-strong: rgba(255, 252, 247, 0.96);
        --ink: #1e2321;
        --muted: #5e655f;
        --line: rgba(34, 42, 38, 0.12);
        --warm: #af5e2f;
        --cool: #2c6a73;
        --accent: #16423c;
        --accent-soft: rgba(22, 66, 60, 0.08);
        --shadow: 0 24px 60px rgba(26, 36, 33, 0.08);
      }

      * {
        box-sizing: border-box;
      }

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
        background: rgba(247, 243, 236, 0.78);
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
        letter-spacing: 0.04em;
      }

      .brand h1 {
        margin: 0;
        font-size: 2rem;
        font-weight: 600;
      }

      .brand p {
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
        color: var(--muted);
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
        max-width: 720px;
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
        font-size: clamp(2rem, 4vw, 3.6rem);
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

      .metric:nth-child(2) {
        animation-delay: 70ms;
      }

      .metric:nth-child(3) {
        animation-delay: 140ms;
      }

      .metric:nth-child(4) {
        animation-delay: 210ms;
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
        grid-template-columns: minmax(0, 1.3fr) minmax(280px, 0.7fr);
        gap: 16px;
        align-items: start;
      }

      .panel {
        background: var(--surface);
        border: 1px solid var(--line);
        border-radius: 18px;
        box-shadow: var(--shadow);
      }

      .watchlist {
        padding: 18px 20px 10px;
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

      .activity {
        padding: 18px 20px;
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
        padding: 20px;
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
        .workspace,
        .insight-grid {
          grid-template-columns: 1fr 1fr;
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
        .workspace,
        .insight-grid {
          grid-template-columns: 1fr;
        }

        .nav-group {
          grid-template-columns: 1fr 1fr;
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
            <a class="nav-item" href="/api/status">System Status</a>
            <a class="nav-item" href="/ingest-info">Ingestion</a>
            <span class="nav-item">Sites</span>
            <span class="nav-item">Events</span>
            <span class="nav-item">Alerts</span>
          </nav>
        </div>
        <div class="rail-footer">
          <span>Shell generated ${generatedAt}</span>
          <span>Next step: connect one public source and save explainable alerts.</span>
        </div>
      </aside>
      <main class="main">
        <header class="topbar">
          <div class="topbar-copy">
            <p class="eyebrow">Staff Dashboard</p>
            <h2>See where change may be becoming opportunity.</h2>
            <p>
              This first shell is designed for daily scanning. It highlights where Danvers staff may want to
              review a property, corridor, or market shift before it becomes a missed opening.
            </p>
          </div>
          <div class="topbar-meta">
            <div>Worker name: <strong>opportunity</strong></div>
            <div>Mode: dashboard shell</div>
            <div>Data store: not connected yet</div>
          </div>
        </header>

        <section class="metrics" aria-label="Top metrics">
          ${metricMarkup}
        </section>

        <section class="workspace">
          <div class="panel watchlist">
            <div class="watchlist-head">
              <div>
                <p class="eyebrow">Watchlist</p>
                <h3>Current review lanes</h3>
                <p>Not live data yet. This layout shows the operating surface we can begin wiring into real signals.</p>
              </div>
              <span class="status-pill">Explainable alerts only</span>
            </div>
            <table>
              <thead>
                <tr>
                  <th>Site</th>
                  <th>Signal</th>
                  <th>Focus</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                ${watchMarkup}
              </tbody>
            </table>
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
                ${activityMarkup}
              </ul>
            </section>

            <section class="panel insight-band">
              <p class="eyebrow">Decision Support</p>
              <h3>What this shell is meant to support</h3>
              <p>Fast municipal triage around commercial tax base, site readiness, and corridor-level intervention choices.</p>
              <div class="insight-grid">
                <div class="insight">
                  <strong>Tax Base</strong>
                  <p>Flag sites where turnover, reuse, or vacancy could affect long-term valuation and business activity.</p>
                </div>
                <div class="insight">
                  <strong>Readiness</strong>
                  <p>Track whether a site’s real blocker is zoning, utilities, parcel conditions, or market uncertainty.</p>
                </div>
                <div class="insight">
                  <strong>Action</strong>
                  <p>Separate what the Town can move directly from what depends on owners, utilities, or outside partners.</p>
                </div>
              </div>
            </section>
          </div>
        </section>
      </main>
    </div>
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

    if (request.method === "GET" && url.pathname === "/ingest-info") {
      return Response.json(
        {
          enabled: false,
          detail: "Queue and database bindings are not configured yet.",
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
