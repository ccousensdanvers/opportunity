import test from "node:test";
import assert from "node:assert/strict";

import { normalizeOpenGovLocation } from "../src/index.ts";

test("normalizes JSON:API attributes shape", () => {
  const normalized = normalizeOpenGovLocation({
    id: "loc-1",
    type: "locations",
    attributes: {
      street_no: "12",
      street_name: "Maple Street",
      city: "Danvers",
      state: "MA",
      postal_code: "01923",
    },
  });

  assert.equal(normalized.streetNo, "12");
  assert.equal(normalized.streetName, "Maple Street");
  assert.equal(normalized.city, "Danvers");
  assert.equal(normalized.state, "MA");
  assert.equal(normalized.postalCode, "01923");
});

test("normalizes root-level shape", () => {
  const normalized = normalizeOpenGovLocation({
    id: "loc-2",
    streetNo: "22",
    streetName: "High Street",
    city: "Danvers",
    state: "MA",
    postalCode: "01923",
  });

  assert.equal(normalized.streetNo, "22");
  assert.equal(normalized.streetName, "High Street");
  assert.equal(normalized.city, "Danvers");
  assert.equal(normalized.state, "MA");
  assert.equal(normalized.postalCode, "01923");
});

test("normalizes nested address and parcel shape", () => {
  const normalized = normalizeOpenGovLocation({
    id: "loc-3",
    attributes: {
      address: {
        street_number: "33",
        street_name: "Oak Avenue",
        city: "Danvers",
        state: "MA",
        zip_code: "01923",
      },
      parcel: {
        parcel_id: "P-123",
        mbl: "11-22-33",
      },
    },
  });

  assert.equal(normalized.streetNo, "33");
  assert.equal(normalized.streetName, "Oak Avenue");
  assert.equal(normalized.city, "Danvers");
  assert.equal(normalized.state, "MA");
  assert.equal(normalized.postalCode, "01923");
  assert.equal(normalized.gisID, "P-123");
  assert.equal(normalized.mbl, "11-22-33");
});

test("normalizes weird casing and punctuation keys", () => {
  const normalized = normalizeOpenGovLocation({
    id: "loc-4",
    attributes: {
      "Street No": "12",
      "Street-Name": "Maple Street",
      "Zip Code": "01923",
      City: "Danvers",
      STATE: "MA",
    },
  });

  assert.equal(normalized.streetNo, "12");
  assert.equal(normalized.streetName, "Maple Street");
  assert.equal(normalized.postalCode, "01923");
  assert.equal(normalized.city, "Danvers");
  assert.equal(normalized.state, "MA");
});
