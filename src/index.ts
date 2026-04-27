type IngestTrigger = "manual" | "scheduled";

interface IngestMessage {
  trigger: IngestTrigger;
  requestedAt: string;
}

function buildIngestMessage(trigger: IngestTrigger): IngestMessage {
  return {
    trigger,
    requestedAt: new Date().toISOString(),
  };
}

export default {
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    if (request.method === "GET" && url.pathname === "/") {
      return Response.json({
        service: "danvers-opportunity-agent",
        status: "ok",
        checkedAt: new Date().toISOString(),
        data: {
          phase: "bootstrap",
          capabilities: {
            api: true,
            scheduledChecks: true,
            database: false,
            queue: false,
          },
        },
      });
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
