export type ParcelMatchType =
  | "map_lot_exact"
  | "address_exact"
  | "owner_exact"
  | "address_prefix"
  | "no_match";

export interface OpportunityParcelInput {
  id: string;
  mapLot?: string | null;
  address?: string | null;
  ownerName?: string | null;
}

export interface ParcelUpsertInput {
  mapLot: string;
  address?: string | null;
  ownerName?: string | null;
  zoningDistrict?: string | null;
  landAreaSqft?: number | null;
  aliases?: Array<{
    type: "map_lot" | "address" | "owner";
    value: string;
    confidence?: number;
  }>;
}

export interface ParcelMatchResult {
  opportunityId: string;
  parcelId: number | null;
  matchType: ParcelMatchType;
  confidence: number;
  inputValue: string | null;
  matchedValue: string | null;
  needsReview: boolean;
}

interface ParcelRow {
  id: number;
  map_lot: string | null;
  address: string | null;
  owner_name: string | null;
}

interface MatchRule {
  type: Exclude<ParcelMatchType, "no_match">;
  value: string | null;
  confidence: number;
  needsReview?: boolean;
  query: string;
}

export interface ParcelReviewItem {
  opportunityId: string;
  parcelId: number;
  matchType: Exclude<ParcelMatchType, "no_match">;
  confidence: number;
  inputValue: string | null;
  matchedValue: string | null;
  needsReview: boolean;
  createdAt: string;
  updatedAt: string;
  parcel: {
    mapLot: string | null;
    address: string | null;
    ownerName: string | null;
    zoningDistrict: string | null;
  };
}

const ADDRESS_SYNONYMS: Record<string, string> = {
  avenue: "ave",
  av: "ave",
  boulevard: "blvd",
  circle: "cir",
  court: "ct",
  drive: "dr",
  highway: "hwy",
  lane: "ln",
  place: "pl",
  road: "rd",
  route: "rt",
  square: "sq",
  street: "st",
  terrace: "ter",
};

function normalizeWhitespace(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

export function normalizeMapLot(value?: string | null): string | null {
  if (!value) {
    return null;
  }

  const cleaned = value.toUpperCase().replace(/[^A-Z0-9]/g, "");
  return cleaned || null;
}

function normalizeGeneric(value?: string | null): string | null {
  if (!value) {
    return null;
  }

  const cleaned = normalizeWhitespace(value.toLowerCase().replace(/[^a-z0-9\s]/g, " "));
  return cleaned || null;
}

export function normalizeAddress(value?: string | null): string | null {
  const generic = normalizeGeneric(value);
  if (!generic) {
    return null;
  }

  return generic
    .split(" ")
    .map((token) => ADDRESS_SYNONYMS[token] ?? token)
    .join(" ");
}

export function normalizeOwnerName(value?: string | null): string | null {
  const generic = normalizeGeneric(value);
  if (!generic) {
    return null;
  }

  return generic
    .replace(/\b(llc|inc|corp|co|realty|trust|et al)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export async function upsertParcels(
  db: D1Database,
  parcels: ParcelUpsertInput[],
): Promise<void> {
  for (const parcel of parcels) {
    const mapLot = normalizeMapLot(parcel.mapLot);
    if (!mapLot) {
      continue;
    }

    await db
      .prepare(
        `
        INSERT INTO parcels (
          map_lot,
          address,
          owner_name,
          zoning_district,
          land_area_sqft,
          updated_at
        ) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(map_lot) DO UPDATE SET
          address = excluded.address,
          owner_name = excluded.owner_name,
          zoning_district = excluded.zoning_district,
          land_area_sqft = excluded.land_area_sqft,
          updated_at = CURRENT_TIMESTAMP
        `,
      )
      .bind(
        mapLot,
        parcel.address ?? null,
        parcel.ownerName ?? null,
        parcel.zoningDistrict ?? null,
        parcel.landAreaSqft ?? null,
      )
      .run();

    const parcelRow = await db
      .prepare(`SELECT id FROM parcels WHERE map_lot = ? LIMIT 1`)
      .bind(mapLot)
      .first<{ id: number }>();

    if (!parcelRow) {
      continue;
    }

    const aliases = [
      { type: "map_lot", value: parcel.mapLot, confidence: 1 },
      { type: "address", value: parcel.address, confidence: 1 },
      { type: "owner", value: parcel.ownerName, confidence: 1 },
      ...(parcel.aliases ?? []),
    ];

    for (const alias of aliases) {
      if (!alias.value) {
        continue;
      }

      const normalized =
        alias.type === "map_lot"
          ? normalizeMapLot(alias.value)
          : alias.type === "address"
            ? normalizeAddress(alias.value)
            : normalizeOwnerName(alias.value);

      if (!normalized) {
        continue;
      }

      await db
        .prepare(
          `
          INSERT INTO parcel_aliases (
            parcel_id,
            alias_type,
            alias_value_raw,
            alias_value_norm,
            confidence,
            updated_at
          ) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
          ON CONFLICT(alias_type, alias_value_norm) DO UPDATE SET
            parcel_id = excluded.parcel_id,
            alias_value_raw = excluded.alias_value_raw,
            confidence = excluded.confidence,
            updated_at = CURRENT_TIMESTAMP
          `,
        )
        .bind(
          parcelRow.id,
          alias.type,
          alias.value,
          normalized,
          alias.confidence ?? 1,
        )
        .run();
    }
  }
}

function buildMatchRules(input: OpportunityParcelInput): MatchRule[] {
  return [
    {
      type: "map_lot_exact",
      value: normalizeMapLot(input.mapLot),
      confidence: 1,
      query: `
        SELECT id, map_lot, address, owner_name
        FROM parcels
        WHERE map_lot = ?
        LIMIT 1
      `,
    },
    {
      type: "address_exact",
      value: normalizeAddress(input.address),
      confidence: 0.94,
      query: `
        SELECT p.id, p.map_lot, p.address, p.owner_name
        FROM parcel_aliases a
        JOIN parcels p ON p.id = a.parcel_id
        WHERE a.alias_type = 'address' AND a.alias_value_norm = ?
        LIMIT 1
      `,
    },
    {
      type: "owner_exact",
      value: normalizeOwnerName(input.ownerName),
      confidence: 0.84,
      needsReview: true,
      query: `
        SELECT p.id, p.map_lot, p.address, p.owner_name
        FROM parcel_aliases a
        JOIN parcels p ON p.id = a.parcel_id
        WHERE a.alias_type = 'owner' AND a.alias_value_norm = ?
        LIMIT 1
      `,
    },
    {
      type: "address_prefix",
      value: normalizeAddress(input.address),
      confidence: 0.74,
      needsReview: true,
      query: `
        SELECT p.id, p.map_lot, p.address, p.owner_name
        FROM parcel_aliases a
        JOIN parcels p ON p.id = a.parcel_id
        WHERE a.alias_type = 'address' AND a.alias_value_norm LIKE ? || '%'
        LIMIT 1
      `,
    },
  ];
}

export async function matchOpportunityToParcel(
  db: D1Database,
  input: OpportunityParcelInput,
): Promise<ParcelMatchResult> {
  for (const rule of buildMatchRules(input)) {
    if (!rule.value) {
      continue;
    }

    const row = await db.prepare(rule.query).bind(rule.value).first<ParcelRow>();

    if (!row) {
      continue;
    }

    const matchedValue =
      rule.type === "map_lot_exact"
        ? row.map_lot
        : rule.type === "owner_exact"
          ? row.owner_name
          : row.address;

    return {
      opportunityId: input.id,
      parcelId: row.id,
      matchType: rule.type,
      confidence: rule.confidence,
      inputValue: rule.value,
      matchedValue: matchedValue ?? null,
      needsReview: rule.needsReview ?? false,
    };
  }

  return {
    opportunityId: input.id,
    parcelId: null,
    matchType: "no_match",
    confidence: 0,
    inputValue: null,
    matchedValue: null,
    needsReview: true,
  };
}

export async function matchAndPersistOpportunity(
  db: D1Database,
  input: OpportunityParcelInput,
): Promise<ParcelMatchResult> {
  const match = await matchOpportunityToParcel(db, input);

  if (!match.parcelId) {
    return match;
  }

  await db
    .prepare(
      `
      INSERT INTO opportunity_parcel_matches (
        opportunity_id,
        parcel_id,
        match_type,
        confidence,
        input_value,
        matched_value,
        needs_review,
        updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
      ON CONFLICT(opportunity_id) DO UPDATE SET
        parcel_id = excluded.parcel_id,
        match_type = excluded.match_type,
        confidence = excluded.confidence,
        input_value = excluded.input_value,
        matched_value = excluded.matched_value,
        needs_review = excluded.needs_review,
        updated_at = CURRENT_TIMESTAMP
      `,
    )
    .bind(
      match.opportunityId,
      match.parcelId,
      match.matchType,
      match.confidence,
      match.inputValue,
      match.matchedValue,
      match.needsReview ? 1 : 0,
    )
    .run();

  return match;
}

export async function matchAndPersistOpportunities(
  db: D1Database,
  opportunities: OpportunityParcelInput[],
): Promise<ParcelMatchResult[]> {
  const results: ParcelMatchResult[] = [];

  for (const opportunity of opportunities) {
    results.push(await matchAndPersistOpportunity(db, opportunity));
  }

  return results;
}

export async function listParcelReviewQueue(
  db: D1Database,
  options: {
    limit?: number;
    needsReviewOnly?: boolean;
  } = {},
): Promise<ParcelReviewItem[]> {
  const limit = Math.min(Math.max(options.limit ?? 25, 1), 100);

  const statement = options.needsReviewOnly === false
    ? db.prepare(
        `
        SELECT
          m.opportunity_id AS opportunityId,
          m.parcel_id AS parcelId,
          m.match_type AS matchType,
          m.confidence AS confidence,
          m.input_value AS inputValue,
          m.matched_value AS matchedValue,
          m.needs_review AS needsReview,
          m.created_at AS createdAt,
          m.updated_at AS updatedAt,
          p.map_lot AS mapLot,
          p.address AS address,
          p.owner_name AS ownerName,
          p.zoning_district AS zoningDistrict
        FROM opportunity_parcel_matches m
        JOIN parcels p ON p.id = m.parcel_id
        ORDER BY
          m.needs_review DESC,
          m.confidence ASC,
          m.updated_at DESC
        LIMIT ?
        `,
      )
    : db.prepare(
        `
        SELECT
          m.opportunity_id AS opportunityId,
          m.parcel_id AS parcelId,
          m.match_type AS matchType,
          m.confidence AS confidence,
          m.input_value AS inputValue,
          m.matched_value AS matchedValue,
          m.needs_review AS needsReview,
          m.created_at AS createdAt,
          m.updated_at AS updatedAt,
          p.map_lot AS mapLot,
          p.address AS address,
          p.owner_name AS ownerName,
          p.zoning_district AS zoningDistrict
        FROM opportunity_parcel_matches m
        JOIN parcels p ON p.id = m.parcel_id
        WHERE m.needs_review = 1
        ORDER BY
          m.confidence ASC,
          m.updated_at DESC
        LIMIT ?
        `,
      );

  const { results } = await statement.bind(limit).all<{
    opportunityId: string;
    parcelId: number;
    matchType: Exclude<ParcelMatchType, "no_match">;
    confidence: number;
    inputValue: string | null;
    matchedValue: string | null;
    needsReview: number;
    createdAt: string;
    updatedAt: string;
    mapLot: string | null;
    address: string | null;
    ownerName: string | null;
    zoningDistrict: string | null;
  }>();

  return (results ?? []).map((row) => ({
    opportunityId: row.opportunityId,
    parcelId: row.parcelId,
    matchType: row.matchType,
    confidence: row.confidence,
    inputValue: row.inputValue,
    matchedValue: row.matchedValue,
    needsReview: Boolean(row.needsReview),
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
    parcel: {
      mapLot: row.mapLot,
      address: row.address,
      ownerName: row.ownerName,
      zoningDistrict: row.zoningDistrict,
    },
  }));
}
