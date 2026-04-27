# Repo Instructions

This repository is for an internal Danvers opportunity intelligence tool.

## Product Rules

- Prioritize factual accuracy over speculative or clever output.
- Do not label a parcel or site as "available" without evidence.
- Do not infer owner intent from weak signals.
- Treat AI-generated matching as staff decision support, not automated outreach.
- Preserve an audit trail for every alert and score.

## Development Rules

- Keep v1 simple and operational.
- Favor structured rules before model-based inference.
- Keep data models explicit and documented.
- Prefer additive migrations.
- Avoid introducing heavy dependencies unless they remove real complexity.

## Source Rules

- Public sources are allowed by default.
- Private or credentialed sources should only be added with explicit configuration and documentation.
- Every source connector should document:
  - source name
  - update cadence
  - terms or limitations
  - parser assumptions

## Output Rules

- Internal alerts must show why the system flagged a site.
- Scores should be explainable.
- Generated summaries should identify what is known versus inferred.

