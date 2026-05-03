import test from "node:test";
import assert from "node:assert/strict";

import { normalizeOpenGovRecordType } from "../src/index.ts";

test("normalizes JSON:API record type shape", () => {
  const normalized = normalizeOpenGovRecordType({
    id: "rt-1",
    type: "record-types",
    attributes: {
      name: "Building Permit",
      description: "Primary permit",
      slug: "building-permit",
      active: true,
    },
  });

  assert.equal(normalized.id, "rt-1");
  assert.equal(normalized.type, "record-types");
  assert.equal(normalized.name, "Building Permit");
  assert.equal(normalized.description, "Primary permit");
  assert.equal(normalized.slug, "building-permit");
  assert.equal(normalized.active, true);
});

test("normalizes root-level record type shape", () => {
  const normalized = normalizeOpenGovRecordType({
    id: "rt-2",
    name: "Electrical Permit",
    category: "Inspections",
    module: "Permitting",
    enabled: true,
  });

  assert.equal(normalized.id, "rt-2");
  assert.equal(normalized.name, "Electrical Permit");
  assert.equal(normalized.category, "Inspections");
  assert.equal(normalized.module, "Permitting");
  assert.equal(normalized.active, true);
});

test("infers active from archived-only shape", () => {
  const normalized = normalizeOpenGovRecordType({
    id: "rt-3",
    name: "Old Permit",
    archived: true,
  });

  assert.equal(normalized.id, "rt-3");
  assert.equal(normalized.active, false);
});

test("handles weird casing and punctuation keys", () => {
  const normalized = normalizeOpenGovRecordType({
    id: "rt-4",
    attributes: {
      "Display Name": "Building Permit",
      "Record-Type": "building",
    },
  });

  assert.equal(normalized.id, "rt-4");
  assert.equal(normalized.name, "Building Permit");
  assert.equal(normalized.slug, "building");
});
