import test from "node:test";
import assert from "node:assert/strict";

import {
  listParcelReviewQueue,
  matchAndPersistOpportunities,
  upsertParcels,
} from "../src/parcel-matching.ts";

class FakeD1Database {
  constructor() {
    this.nextParcelId = 1;
    this.parcels = [];
    this.aliases = [];
    this.matches = [];
  }

  prepare(query) {
    return new FakeStatement(this, query);
  }
}

class FakeStatement {
  constructor(db, query) {
    this.db = db;
    this.query = query;
    this.params = [];
  }

  bind(...params) {
    this.params = params;
    return this;
  }

  async run() {
    const query = this.query;

    if (query.includes("INSERT INTO parcels")) {
      const [mapLot, address, ownerName, zoningDistrict, landAreaSqft] = this.params;
      const existing = this.db.parcels.find((parcel) => parcel.map_lot === mapLot);

      if (existing) {
        Object.assign(existing, {
          address,
          owner_name: ownerName,
          zoning_district: zoningDistrict,
          land_area_sqft: landAreaSqft,
        });
      } else {
        this.db.parcels.push({
          id: this.db.nextParcelId++,
          map_lot: mapLot,
          address,
          owner_name: ownerName,
          zoning_district: zoningDistrict,
          land_area_sqft: landAreaSqft,
        });
      }

      return { success: true };
    }

    if (query.includes("INSERT INTO parcel_aliases")) {
      const [parcelId, aliasType, aliasValueRaw, aliasValueNorm, confidence] = this.params;
      const existing = this.db.aliases.find(
        (alias) => alias.alias_type === aliasType && alias.alias_value_norm === aliasValueNorm,
      );

      if (existing) {
        Object.assign(existing, {
          parcel_id: parcelId,
          alias_value_raw: aliasValueRaw,
          confidence,
        });
      } else {
        this.db.aliases.push({
          parcel_id: parcelId,
          alias_type: aliasType,
          alias_value_raw: aliasValueRaw,
          alias_value_norm: aliasValueNorm,
          confidence,
        });
      }

      return { success: true };
    }

    if (query.includes("INSERT INTO opportunity_parcel_matches")) {
      const [
        opportunityId,
        parcelId,
        matchType,
        confidence,
        inputValue,
        matchedValue,
        needsReview,
        rawInputJson,
      ] = this.params;

      const existing = this.db.matches.find((match) => match.opportunity_id === opportunityId);

      if (existing) {
        Object.assign(existing, {
          parcel_id: parcelId,
          match_type: matchType,
          confidence,
          input_value: inputValue,
          matched_value: matchedValue,
          needs_review: needsReview,
          raw_input_json: rawInputJson,
        });
      } else {
        this.db.matches.push({
          opportunity_id: opportunityId,
          parcel_id: parcelId,
          match_type: matchType,
          confidence,
          input_value: inputValue,
          matched_value: matchedValue,
          needs_review: needsReview,
          raw_input_json: rawInputJson,
          created_at: "2026-04-27T00:00:00.000Z",
          updated_at: "2026-04-27T00:00:00.000Z",
        });
      }

      return { success: true };
    }

    throw new Error(`Unsupported run query: ${query}`);
  }

  async first() {
    const query = this.query;
    const [value] = this.params;

    if (query.includes("SELECT id FROM parcels WHERE map_lot = ?")) {
      const parcel = this.db.parcels.find((item) => item.map_lot === value);
      return parcel ? { id: parcel.id } : null;
    }

    if (query.includes("FROM parcels") && query.includes("WHERE map_lot = ?")) {
      return this.db.parcels.find((item) => item.map_lot === value) ?? null;
    }

    if (query.includes("a.alias_type = 'address' AND a.alias_value_norm = ?")) {
      const alias = this.db.aliases.find(
        (item) => item.alias_type === "address" && item.alias_value_norm === value,
      );
      return alias
        ? this.db.parcels.find((parcel) => parcel.id === alias.parcel_id) ?? null
        : null;
    }

    if (query.includes("a.alias_type = 'owner' AND a.alias_value_norm = ?")) {
      const alias = this.db.aliases.find(
        (item) => item.alias_type === "owner" && item.alias_value_norm === value,
      );
      return alias
        ? this.db.parcels.find((parcel) => parcel.id === alias.parcel_id) ?? null
        : null;
    }

    if (query.includes("a.alias_type = 'address' AND a.alias_value_norm LIKE ? || '%'")) {
      const alias = this.db.aliases.find(
        (item) => item.alias_type === "address" && item.alias_value_norm.startsWith(value),
      );
      return alias
        ? this.db.parcels.find((parcel) => parcel.id === alias.parcel_id) ?? null
        : null;
    }

    throw new Error(`Unsupported first query: ${query}`);
  }

  async all() {
    if (!this.query.includes("FROM opportunity_parcel_matches m")) {
      throw new Error(`Unsupported all query: ${this.query}`);
    }

    const [limit] = this.params;
    const needsReviewOnly = this.query.includes("WHERE m.needs_review = 1");

    const rows = this.db.matches
      .filter((match) => !needsReviewOnly || match.needs_review === 1)
      .sort((left, right) => {
        if ((right.needs_review ?? 0) !== (left.needs_review ?? 0)) {
          return (right.needs_review ?? 0) - (left.needs_review ?? 0);
        }

        if ((left.confidence ?? 0) !== (right.confidence ?? 0)) {
          return (left.confidence ?? 0) - (right.confidence ?? 0);
        }

        return String(right.updated_at).localeCompare(String(left.updated_at));
      })
      .slice(0, limit)
      .map((match) => {
        const parcel = this.db.parcels.find((item) => item.id === match.parcel_id) ?? null;

        return {
          opportunityId: match.opportunity_id,
          parcelId: match.parcel_id,
          matchType: match.match_type,
          confidence: match.confidence,
          inputValue: match.input_value,
          matchedValue: match.matched_value,
          needsReview: match.needs_review,
          rawInputJson: match.raw_input_json,
          createdAt: match.created_at,
          updatedAt: match.updated_at,
          mapLot: parcel?.map_lot ?? null,
          address: parcel?.address ?? null,
          ownerName: parcel?.owner_name ?? null,
          zoningDistrict: parcel?.zoning_district ?? null,
        };
      });

    return { results: rows };
  }
}

test("persists exact matches into the review queue", async () => {
  const db = new FakeD1Database();

  await upsertParcels(db, [
    {
      mapLot: "12-34",
      address: "15 Maple Street",
      ownerName: "Maple Realty LLC",
      zoningDistrict: "I-1",
      landAreaSqft: 12000,
    },
  ]);

  const [result] = await matchAndPersistOpportunities(db, [
    { id: "opp-1", address: "15 Maple St." },
  ]);

  assert.equal(result.matchType, "address_exact");
  assert.equal(result.parcelId, 1);

  const review = await listParcelReviewQueue(db, { needsReviewOnly: false });
  assert.equal(review.length, 1);
  assert.equal(review[0].parcel?.address, "15 Maple Street");
  assert.equal(review[0].input.address, "15 Maple St.");
});

test("persists no-match opportunities for staff review", async () => {
  const db = new FakeD1Database();

  await upsertParcels(db, [
    {
      mapLot: "12-34",
      address: "15 Maple Street",
      ownerName: "Maple Realty LLC",
    },
  ]);

  const [result] = await matchAndPersistOpportunities(db, [
    { id: "opp-2", address: "99 Unknown Road" },
  ]);

  assert.equal(result.matchType, "no_match");
  assert.equal(result.parcelId, null);
  assert.equal(result.needsReview, true);

  const review = await listParcelReviewQueue(db);
  assert.equal(review.length, 1);
  assert.equal(review[0].matchType, "no_match");
  assert.equal(review[0].parcel, null);
  assert.deepEqual(review[0].input, {
    id: "opp-2",
    address: "99 Unknown Road",
  });
});
