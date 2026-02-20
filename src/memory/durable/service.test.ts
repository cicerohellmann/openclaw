import fs from "node:fs/promises";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { makeTempWorkspace } from "../../test-helpers/workspace.js";
import { requireNodeSqlite } from "../sqlite.js";
import { DurableMemoryService } from "./service.js";

const FIXED_NOW = "2026-02-20T10:00:00.000Z";

const createdWorkspaces: string[] = [];

afterEach(async () => {
  await Promise.all(
    createdWorkspaces
      .splice(0)
      .map((workspace) => fs.rm(workspace, { recursive: true, force: true })),
  );
});

async function createService(params?: {
  nowIso?: string;
  workspaceDir?: string;
}): Promise<{ service: DurableMemoryService; workspaceDir: string }> {
  const workspaceDir =
    params?.workspaceDir ?? (await makeTempWorkspace("openclaw-durable-memory-"));
  if (!params?.workspaceDir) {
    createdWorkspaces.push(workspaceDir);
  }
  const nowIso = params?.nowIso ?? FIXED_NOW;
  const service = await DurableMemoryService.create({
    workspaceDir,
    now: () => new Date(nowIso),
  });
  return { service, workspaceDir };
}

describe("DurableMemoryService", () => {
  it("initializes durable schema at memory/durable_memory.db", async () => {
    const { service, workspaceDir } = await createService();
    const status = service.status();

    expect(status.dbPath).toBe(path.join(workspaceDir, "memory", "durable_memory.db"));
    expect(status.events).toBe(0);
    expect(status.memories).toBe(0);
    expect(status.activeMemories).toBe(0);

    service.close();
  });

  it("migrates legacy memory/gibi_memory.db to memory/durable_memory.db", async () => {
    const workspaceDir = await makeTempWorkspace("openclaw-durable-memory-legacy-");
    createdWorkspaces.push(workspaceDir);

    const memoryDir = path.join(workspaceDir, "memory");
    await fs.mkdir(memoryDir, { recursive: true });

    const legacyDbPath = path.join(memoryDir, "gibi_memory.db");
    const durableDbPath = path.join(memoryDir, "durable_memory.db");

    const { DatabaseSync } = requireNodeSqlite();
    const legacyDb = new DatabaseSync(legacyDbPath);
    legacyDb.exec("CREATE TABLE IF NOT EXISTS legacy_marker (id INTEGER PRIMARY KEY);");
    legacyDb.close();

    const { service } = await createService({ workspaceDir });
    const status = service.status();

    expect(status.dbPath).toBe(durableDbPath);
    await expect(fs.stat(legacyDbPath)).rejects.toBeDefined();
    await expect(fs.stat(durableDbPath)).resolves.toBeDefined();

    service.close();
  });

  it("migrates legacy durable schema and keeps migration idempotent", async () => {
    const workspaceDir = await makeTempWorkspace("openclaw-durable-memory-schema-");
    createdWorkspaces.push(workspaceDir);

    const memoryDir = path.join(workspaceDir, "memory");
    await fs.mkdir(memoryDir, { recursive: true });
    const legacyDbPath = path.join(memoryDir, "gibi_memory.db");

    const { DatabaseSync } = requireNodeSqlite();
    const db = new DatabaseSync(legacyDbPath);
    db.exec(`
      CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
      CREATE TABLE events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        role TEXT NOT NULL,
        text TEXT NOT NULL,
        summary TEXT,
        tags_json TEXT NOT NULL DEFAULT '[]',
        created_at TEXT NOT NULL,
        source_type TEXT,
        source_ref TEXT,
        embedding TEXT
      );
      CREATE TABLE memory_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        type TEXT NOT NULL,
        title TEXT NOT NULL,
        body TEXT NOT NULL,
        tags_json TEXT NOT NULL DEFAULT '[]',
        confidence REAL NOT NULL DEFAULT 0.5,
        importance REAL NOT NULL DEFAULT 0.5,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        last_seen_at TEXT,
        last_confirmed_at TEXT,
        time_horizon TEXT NOT NULL DEFAULT 'medium',
        valid_until TEXT,
        next_review_at TEXT,
        status TEXT NOT NULL DEFAULT 'active',
        canonical_id INTEGER
      );
      CREATE TABLE memory_evidence (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        memory_id INTEGER NOT NULL,
        event_id INTEGER,
        evidence_type TEXT NOT NULL,
        note TEXT,
        created_at TEXT NOT NULL
      );
      CREATE TABLE memory_links (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        memory_id INTEGER NOT NULL,
        link_type TEXT NOT NULL,
        link_target TEXT NOT NULL,
        label TEXT,
        created_at TEXT NOT NULL
      );
      CREATE TABLE memory_merges (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        old_memory_id INTEGER NOT NULL,
        canonical_memory_id INTEGER NOT NULL,
        reason TEXT NOT NULL,
        created_at TEXT NOT NULL
      );
      CREATE TABLE context_window_entries (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at TEXT NOT NULL
      );
      CREATE TABLE context_resets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        note TEXT NOT NULL,
        created_at TEXT NOT NULL
      );
    `);
    db.prepare("INSERT INTO meta(key, value) VALUES('durable_schema_version', '1')").run();
    db.prepare(
      "INSERT INTO events(id, role, text, summary, tags_json, created_at) VALUES (1, 'user', 'legacy event', NULL, '[]', ?)",
    ).run(FIXED_NOW);
    db.prepare(
      "INSERT INTO memory_items(id, type, title, body, tags_json, confidence, importance, created_at, updated_at, time_horizon, status) VALUES (1, 'Preference', 'Legacy preference', 'legacy body', '[]', 0.8, 0.7, ?, ?, 'medium', 'active')",
    ).run(FIXED_NOW, FIXED_NOW);
    db.prepare(
      "INSERT INTO memory_evidence(memory_id, event_id, evidence_type, note, created_at) VALUES (1, 1, 'quote', NULL, ?)",
    ).run(FIXED_NOW);
    db.prepare(
      "INSERT INTO memory_evidence(memory_id, event_id, evidence_type, note, created_at) VALUES (1, 1, 'quote', NULL, ?)",
    ).run(FIXED_NOW);
    db.close();

    const first = await createService({ workspaceDir });
    expect(first.service.status().evidenceLinks).toBe(1);
    first.service.close();

    const durableDbPath = path.join(memoryDir, "durable_memory.db");
    const migratedDb = new DatabaseSync(durableDbPath);
    expect(() =>
      migratedDb
        .prepare(
          "INSERT INTO memory_items(type, title, body, tags_json, confidence, importance, created_at, updated_at, time_horizon, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .run(
          "Preference",
          "Bad row",
          "body",
          "[]",
          1.3,
          0.5,
          FIXED_NOW,
          FIXED_NOW,
          "medium",
          "active",
        ),
    ).toThrow();
    migratedDb.close();

    const second = await createService({ workspaceDir });
    expect(second.service.status().evidenceLinks).toBe(1);
    second.service.close();
  });

  it("stores memory with evidence and links, then returns provenance", async () => {
    const { service } = await createService();
    const event = service.addEvent({
      role: "user",
      text: "Please keep guidance in tiny steps.",
      tags: ["support", "tiny-steps"],
      sourceType: "chat",
      sourceRef: "session-123",
    });

    const memory = service.addMemory({
      type: "Preference",
      title: "Support style",
      body: "User prefers tiny step-by-step guidance.",
      tags: ["support", "style"],
      confidence: 0.82,
      importance: 0.9,
      eventIds: [event.id],
    });

    service.addLink({
      memoryId: memory.id,
      linkType: "file",
      linkTarget: "memory/2026-02-20.md",
      label: "Captured preference",
    });

    const details = service.getMemory({ memoryId: memory.id, withLinks: true });
    expect(details).not.toBeNull();
    if (!details) {
      throw new Error("Expected durable memory details");
    }

    expect(details.evidence.length).toBe(1);
    expect(details.links.length).toBe(1);
    expect(details.provenance.eventIds).toContain(event.id);
    expect(details.provenance.sourcePaths).toContain("memory/2026-02-20.md");
    expect(details.stalenessScore).toBeLessThan(0.5);

    service.close();
  });

  it("consolidates near-duplicate memories into one canonical entry", async () => {
    const { service } = await createService();
    for (let i = 1; i <= 5; i++) {
      service.addMemory({
        type: "Preference",
        title: `Tiny steps preference ${i}`,
        body: "User likes tiny steps and concise progress increments.",
        tags: ["support", "tiny-steps", "pace"],
        confidence: 0.6 + i * 0.03,
        importance: 0.7,
      });
    }

    const summary = service.consolidateMemory();
    const status = service.status();

    expect(summary.mergedGroups).toBe(1);
    expect(summary.mergedItems).toBe(4);
    expect(summary.canonicalIds.length).toBe(1);
    expect(status.activeMemories).toBe(1);
    expect(status.mergedMemories).toBe(4);

    service.close();
  });

  it("clear-context preserves canonical memories and appends audit note", async () => {
    const { service } = await createService();
    service.addMemory({
      type: "Identity",
      title: "Identity continuity",
      body: "Prioritize clear context and durable identity continuity.",
      tags: ["identity"],
      confidence: 0.9,
      importance: 0.95,
    });
    service.setContextEntry("window-1", "temporary active context");

    const before = service.status();
    const result = service.clearContext();
    const after = service.status();

    expect(before.activeMemories).toBe(1);
    expect(result.scope).toBe("durable.context_window_entries");
    expect(result.memoryCountBefore).toBe(1);
    expect(result.memoryCountAfter).toBe(1);
    expect(after.activeMemories).toBe(1);
    expect(after.contextEntries).toBe(0);
    expect(after.events).toBeGreaterThanOrEqual(1);

    service.close();
  });

  it("search ranks with temporal/confidence signals and returns identity/protocol continuity", async () => {
    const workspace = await makeTempWorkspace("openclaw-durable-memory-");
    createdWorkspaces.push(workspace);

    const older = await createService({
      workspaceDir: workspace,
      nowIso: "2026-02-01T10:00:00.000Z",
    });
    older.service.addMemory({
      type: "Preference",
      title: "Support preference",
      body: "Support user with tiny steps and plain language.",
      tags: ["support", "preference"],
      confidence: 0.4,
      importance: 0.7,
    });
    older.service.close();

    const current = await createService({
      workspaceDir: workspace,
      nowIso: "2026-02-20T10:00:00.000Z",
    });
    current.service.addMemory({
      type: "Protocol",
      title: "Support protocol",
      body: "Look first, ask second. Support with tiny steps, then confirm understanding.",
      tags: ["support", "protocol"],
      confidence: 0.92,
      importance: 0.9,
      timeHorizon: "evergreen",
    });
    current.service.addMemory({
      type: "Identity",
      title: "Identity line",
      body: "Clear context, never erase identity.",
      tags: ["identity"],
      confidence: 0.95,
      importance: 0.95,
      timeHorizon: "evergreen",
    });

    const results = current.service.searchMemory({
      query: "how should I support user",
      topK: 3,
      nowMs: Date.parse("2026-02-20T10:00:00.000Z"),
    });

    expect(results.length).toBe(3);
    expect(results[0].memory.type).toBe("Protocol");
    expect(results.map((entry) => entry.memory.type)).toContain("Preference");
    expect(results.map((entry) => entry.memory.type)).toContain("Identity");
    expect(results[0].score).toBeGreaterThan(results[2].score);
    expect(results[0].rationale.length).toBeGreaterThan(0);

    current.service.close();
  });

  it("rejects invalid enum and range values for write inputs", async () => {
    const { service } = await createService();
    const memory = service.addMemory({
      type: "Preference",
      title: "Valid baseline",
      body: "Valid baseline body",
    });

    expect(() =>
      service.addMemory({
        type: "BadType" as never,
        title: "Invalid type",
        body: "Body",
      }),
    ).toThrow("Invalid memory type");

    expect(() =>
      service.addMemory({
        type: "Preference",
        title: "Invalid horizon",
        body: "Body",
        timeHorizon: "forever" as never,
      }),
    ).toThrow("Invalid time horizon");

    expect(() =>
      service.addMemory({
        type: "Preference",
        title: "Invalid confidence",
        body: "Body",
        confidence: 1.2,
      }),
    ).toThrow("Invalid confidence");

    expect(() =>
      service.addMemory({
        type: "Preference",
        title: "Invalid event ids",
        body: "Body",
        eventIds: [0],
      }),
    ).toThrow("Invalid event id");

    expect(() =>
      service.addLink({
        memoryId: memory.id,
        linkType: "invalid" as never,
        linkTarget: "memory/invalid.md",
      }),
    ).toThrow("Invalid link type");

    service.close();
  });

  it("applies DB checks and deduplicates evidence rows", async () => {
    const { service } = await createService();
    const event = service.addEvent({
      role: "user",
      text: "Capture this for dedupe coverage.",
    });
    const memory = service.addMemory({
      type: "Preference",
      title: "Dedupe target",
      body: "Keep this preference.",
      eventIds: [event.id],
    });

    service.addMemory({
      type: "Preference",
      title: "Dedupe target",
      body: "Keep this preference.",
      eventIds: [event.id],
    });

    const withEvidence = service.getMemory({ memoryId: memory.id, withLinks: true });
    expect(withEvidence).not.toBeNull();
    if (!withEvidence) {
      throw new Error("Expected memory details for dedupe check");
    }
    expect(withEvidence.evidence).toHaveLength(1);

    const { DatabaseSync } = requireNodeSqlite();
    const db = new DatabaseSync(service.getDbPath());
    const nowIso = new Date(FIXED_NOW).toISOString();

    expect(() =>
      db
        .prepare(
          "INSERT INTO memory_items(type, title, body, tags_json, confidence, importance, created_at, updated_at, time_horizon, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .run(
          "Preference",
          "Invalid confidence row",
          "Body",
          "[]",
          1.2,
          0.5,
          nowIso,
          nowIso,
          "medium",
          "active",
        ),
    ).toThrow();

    expect(() =>
      db
        .prepare(
          "INSERT INTO memory_items(type, title, body, tags_json, confidence, importance, created_at, updated_at, time_horizon, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .run(
          "NotAllowed",
          "Invalid type row",
          "Body",
          "[]",
          0.5,
          0.5,
          nowIso,
          nowIso,
          "medium",
          "active",
        ),
    ).toThrow();

    db.close();
    service.close();
  });
});
