import fs from "node:fs/promises";
import path from "node:path";
import type { DatabaseSync } from "node:sqlite";
import { createSubsystemLogger } from "../../logging/subsystem.js";
import { mmrRerank } from "../mmr.js";
import { requireNodeSqlite } from "../sqlite.js";
import {
  DURABLE_MEMORY_TYPES,
  EVIDENCE_TYPES,
  EVENT_ROLES,
  LINK_TYPES,
  MEMORY_STATUSES,
  TIME_HORIZONS,
  type AddEventInput,
  type AddLinkInput,
  type AddMemoryInput,
  type ConsolidateResult,
  type DurableEvent,
  type DurableMemoryEvidence,
  type DurableMemoryItem,
  type DurableMemoryLink,
  type DurableMemoryStatus,
  type DurableMemoryType,
  type DurableProvenance,
  type DurableSearchResult,
  type MemoryEvidenceType,
  type MemoryLinkType,
  type MemoryStatus,
  type MemoryTimeHorizon,
  type ReviewCandidate,
  type SearchMemoryInput,
  type TemporalSignals,
} from "./types.js";

const log = createSubsystemLogger("memory/durable");

const DAY_MS = 24 * 60 * 60 * 1000;
const DURABLE_SCHEMA_VERSION = "2";
const DEFAULT_TOP_K = 8;
const DEFAULT_TAGS_JSON = "[]";
const DEFAULT_CONFIDENCE = 0.6;
const DEFAULT_IMPORTANCE = 0.55;
const DEFAULT_TIME_HORIZON: MemoryTimeHorizon = "medium";
const CONTEXT_RESET_NOTE = "clutter reset performed";
const CONTEXT_RESET_SCOPE = "durable.context_window_entries";
const DURABLE_DB_FILENAME = "durable_memory.db";
const LEGACY_DURABLE_DB_FILENAME = "gibi_memory.db";
const EVIDENCE_UNIQUE_INDEX = "idx_memory_evidence_identity";

const TYPE_INTENT_TOKENS: Record<DurableMemoryType, string[]> = {
  Preference: ["prefer", "preference", "likes", "dislikes", "style", "tone"],
  Identity: ["identity", "who", "self", "values", "persona", "continuity"],
  Project: ["project", "repo", "milestone", "deliverable", "roadmap"],
  Protocol: ["protocol", "rule", "policy", "always", "never", "workflow"],
  Lesson: ["lesson", "learned", "retrospective", "mistake", "insight"],
  UnpromptedKeep: ["keep", "unprompted", "noticed", "habit", "signal"],
};

const TIME_HORIZON_RANK: Record<MemoryTimeHorizon, number> = {
  immediate: 0,
  short: 1,
  medium: 2,
  long: 3,
  evergreen: 4,
};

const legacyParseWarnings = new Set<string>();

type MemoryItemRow = {
  id: number;
  type: string;
  title: string;
  body: string;
  tags_json: string;
  confidence: number;
  importance: number;
  created_at: string;
  updated_at: string;
  last_seen_at: string | null;
  last_confirmed_at: string | null;
  time_horizon: string;
  valid_until: string | null;
  next_review_at: string | null;
  status: string;
  canonical_id: number | null;
};

type EvidenceRow = {
  id: number;
  memory_id: number;
  event_id: number | null;
  evidence_type: string;
  note: string | null;
  created_at: string;
};

type LinkRow = {
  id: number;
  memory_id: number;
  link_type: string;
  link_target: string;
  label: string | null;
  created_at: string;
};

type DurableScoreComponents = {
  semantic: number;
  keyword: number;
  typeBonus: number;
  temporal: number;
  confidenceEvidence: number;
};

type ScoredMemory = {
  memory: DurableMemoryItem;
  score: number;
  rationale: string;
  components: DurableScoreComponents;
  signals: TemporalSignals;
};

export type DurableMemoryDetails = {
  memory: DurableMemoryItem;
  evidence: DurableMemoryEvidence[];
  links: DurableMemoryLink[];
  provenance: DurableProvenance;
  stalenessScore: number;
  momentumScore: number;
};

export function resolveDurableMemoryDbPath(workspaceDir: string): string {
  return path.join(workspaceDir, "memory", DURABLE_DB_FILENAME);
}

function resolveLegacyDurableMemoryDbPath(workspaceDir: string): string {
  return path.join(workspaceDir, "memory", LEGACY_DURABLE_DB_FILENAME);
}

async function pathExists(pathname: string): Promise<boolean> {
  try {
    await fs.access(pathname);
    return true;
  } catch {
    return false;
  }
}

async function migrateLegacyDbPathIfNeeded(workspaceDir: string): Promise<void> {
  const nextDbPath = resolveDurableMemoryDbPath(workspaceDir);
  const legacyDbPath = resolveLegacyDurableMemoryDbPath(workspaceDir);
  const hasNext = await pathExists(nextDbPath);
  const hasLegacy = await pathExists(legacyDbPath);
  if (!hasLegacy || hasNext) {
    return;
  }

  await fs.rename(legacyDbPath, nextDbPath);
  for (const suffix of ["-wal", "-shm"]) {
    const legacySidecar = `${legacyDbPath}${suffix}`;
    if (!(await pathExists(legacySidecar))) {
      continue;
    }
    await fs.rename(legacySidecar, `${nextDbPath}${suffix}`);
  }
  log.info(`migrated durable memory database path: ${legacyDbPath} -> ${nextDbPath}`);
}

export class DurableMemoryService {
  static async create(params: {
    workspaceDir: string;
    now?: () => Date;
  }): Promise<DurableMemoryService> {
    const dbPath = resolveDurableMemoryDbPath(params.workspaceDir);
    await fs.mkdir(path.dirname(dbPath), { recursive: true });
    await migrateLegacyDbPathIfNeeded(params.workspaceDir);
    const service = new DurableMemoryService({
      workspaceDir: params.workspaceDir,
      dbPath,
      now: params.now,
    });
    service.ensureSchemaAndMigrations();
    return service;
  }

  private readonly workspaceDir: string;
  private readonly dbPath: string;
  private readonly db: DatabaseSync;
  private readonly now: () => Date;

  private constructor(params: { workspaceDir: string; dbPath: string; now?: () => Date }) {
    this.workspaceDir = params.workspaceDir;
    this.dbPath = params.dbPath;
    this.now = params.now ?? (() => new Date());
    const { DatabaseSync } = requireNodeSqlite();
    this.db = new DatabaseSync(this.dbPath);
    this.db.exec("PRAGMA foreign_keys = ON;");
    this.db.exec("PRAGMA journal_mode = WAL;");
  }

  close(): void {
    this.db.close();
  }

  getDbPath(): string {
    return this.dbPath;
  }

  status(): DurableMemoryStatus {
    const events = this.selectCount("events");
    const memories = this.selectCount("memory_items");
    const activeMemories = this.selectCount("memory_items", "status='active'");
    const mergedMemories = this.selectCount("memory_items", "status='merged'");
    const archivedMemories = this.selectCount("memory_items", "status='archived'");
    const evidenceLinks = this.selectCount("memory_evidence");
    const artifactLinks = this.selectCount("memory_links");
    const contextEntries = this.selectCount("context_window_entries");
    const lastReset = this.db
      .prepare("SELECT created_at FROM context_resets ORDER BY id DESC LIMIT 1")
      .get() as { created_at?: string } | undefined;

    return {
      dbPath: this.dbPath,
      events,
      memories,
      activeMemories,
      mergedMemories,
      archivedMemories,
      evidenceLinks,
      artifactLinks,
      contextEntries,
      lastResetAt: typeof lastReset?.created_at === "string" ? lastReset.created_at : null,
    };
  }

  addEvent(input: AddEventInput): DurableEvent {
    if (!EVENT_ROLES.includes(input.role)) {
      throw new Error(`Invalid event role: ${input.role}`);
    }
    const text = input.text.trim();
    if (!text) {
      throw new Error("Event text is required");
    }

    const nowIso = this.nowIso();
    const result = this.db
      .prepare(
        "INSERT INTO events(role, text, summary, tags_json, created_at, source_type, source_ref) VALUES (?, ?, ?, ?, ?, ?, ?)",
      )
      .run(
        input.role,
        text,
        trimOrNull(input.summary),
        toJsonStringArray(input.tags),
        nowIso,
        trimOrNull(input.sourceType),
        trimOrNull(input.sourceRef),
      );

    return {
      id: rowIdToNumber(result.lastInsertRowid),
      role: input.role,
      text,
      summary: trimOrNull(input.summary),
      tags: normalizeTags(input.tags),
      createdAt: nowIso,
      sourceType: trimOrNull(input.sourceType),
      sourceRef: trimOrNull(input.sourceRef),
    };
  }

  addMemory(input: AddMemoryInput): DurableMemoryItem {
    const type = parseMemoryTypeInput(input.type);
    const title = input.title.trim();
    const body = input.body.trim();
    if (!title) {
      throw new Error("Memory title is required");
    }
    if (!body) {
      throw new Error("Memory body is required");
    }

    const nowIso = this.nowIso();
    const tags = normalizeTags(input.tags);
    const confidence = parseUnitIntervalInput(input.confidence, "confidence", DEFAULT_CONFIDENCE);
    const importance = parseUnitIntervalInput(input.importance, "importance", DEFAULT_IMPORTANCE);
    const timeHorizon = parseTimeHorizonInput(input.timeHorizon ?? DEFAULT_TIME_HORIZON);
    const validUntil = normalizeIsoDate(input.validUntil ?? null);
    const nextReviewAt = normalizeIsoDate(input.nextReviewAt ?? null);
    const eventIds = parseEventIdsInput(input.eventIds);

    const existing = this.db
      .prepare(
        "SELECT * FROM memory_items WHERE status='active' AND type=? AND lower(trim(title))=lower(trim(?)) ORDER BY updated_at DESC LIMIT 1",
      )
      .get(type, title) as MemoryItemRow | undefined;

    if (existing) {
      const existingItem = this.rowToMemoryItem(existing);
      const similarity = jaccardSimilarity(
        tokenize(`${existingItem.title} ${existingItem.body}`),
        tokenize(`${title} ${body}`),
      );

      if (similarity >= 0.92) {
        const mergedBody = mergeBody(existingItem.body, body);
        const mergedTags = normalizeTags([...existingItem.tags, ...tags]);
        const nextConfidence = clamp01(Math.max(existingItem.confidence, confidence) + 0.03);
        const nextImportance = clamp01(Math.max(existingItem.importance, importance));
        const mergedValidUntil = selectMergedValidUntil([existingItem.validUntil, validUntil]);
        const mergedNextReview = selectEarliestIso([existingItem.nextReviewAt, nextReviewAt]);

        this.db
          .prepare(
            "UPDATE memory_items SET body=?, tags_json=?, confidence=?, importance=?, updated_at=?, last_confirmed_at=?, time_horizon=?, valid_until=?, next_review_at=?, status='active', canonical_id=NULL WHERE id=?",
          )
          .run(
            mergedBody,
            toJsonStringArray(mergedTags),
            nextConfidence,
            nextImportance,
            nowIso,
            nowIso,
            maxTimeHorizon(existingItem.timeHorizon, timeHorizon),
            mergedValidUntil,
            mergedNextReview,
            existingItem.id,
          );

        this.addEvidenceLinks({
          memoryId: existingItem.id,
          eventIds,
          nowIso,
          evidenceType: "quote",
        });

        const refreshed = this.readMemoryItem(existingItem.id);
        if (!refreshed) {
          throw new Error("Failed to reload updated memory item");
        }
        return refreshed;
      }
    }

    const result = this.db
      .prepare(
        "INSERT INTO memory_items(type, title, body, tags_json, confidence, importance, created_at, updated_at, last_seen_at, last_confirmed_at, time_horizon, valid_until, next_review_at, status, canonical_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, 'active', NULL)",
      )
      .run(
        type,
        title,
        body,
        toJsonStringArray(tags),
        confidence,
        importance,
        nowIso,
        nowIso,
        nowIso,
        timeHorizon,
        validUntil,
        nextReviewAt,
      );

    const id = rowIdToNumber(result.lastInsertRowid);
    this.addEvidenceLinks({
      memoryId: id,
      eventIds,
      nowIso,
      evidenceType: "quote",
    });

    const created = this.readMemoryItem(id);
    if (!created) {
      throw new Error("Failed to load created memory item");
    }
    return created;
  }

  addLink(input: AddLinkInput): DurableMemoryLink {
    const memoryId = this.resolveActiveMemoryId(input.memoryId);
    if (!memoryId) {
      throw new Error(`Memory not found: ${input.memoryId}`);
    }
    const linkType = parseLinkTypeInput(input.linkType);
    const linkTarget = input.linkTarget.trim();
    if (!linkTarget) {
      throw new Error("Link target is required");
    }

    const nowIso = this.nowIso();
    const result = this.db
      .prepare(
        "INSERT INTO memory_links(memory_id, link_type, link_target, label, created_at) VALUES (?, ?, ?, ?, ?)",
      )
      .run(memoryId, linkType, linkTarget, trimOrNull(input.label), nowIso);

    this.touchMemory(memoryId, nowIso);

    return {
      id: rowIdToNumber(result.lastInsertRowid),
      memoryId,
      linkType,
      linkTarget,
      label: trimOrNull(input.label),
      createdAt: nowIso,
    };
  }

  getMemory(params: { memoryId: number; withLinks?: boolean }): DurableMemoryDetails | null {
    const resolvedId = this.resolveActiveMemoryId(params.memoryId) ?? params.memoryId;
    const memory = this.readMemoryItem(resolvedId);
    if (!memory) {
      return null;
    }

    const nowIso = this.nowIso();
    this.touchMemory(memory.id, nowIso);

    const evidence = this.readEvidenceForMemory(memory.id);
    const links = params.withLinks ? this.readLinksForMemory(memory.id) : [];
    const provenance = this.buildProvenance({
      memoryId: memory.id,
      evidence,
      links,
    });
    const signals = deriveTemporalSignals({ memory, nowMs: this.now().getTime() });

    return {
      memory,
      evidence,
      links,
      provenance,
      stalenessScore: signals.stalenessScore,
      momentumScore: signals.momentumScore,
    };
  }

  searchMemory(input: SearchMemoryInput): DurableSearchResult[] {
    const query = input.query.trim();
    if (!query) {
      return [];
    }
    const topK = Number.isFinite(input.topK)
      ? Math.max(1, Math.floor(input.topK as number))
      : DEFAULT_TOP_K;
    const nowMs = Number.isFinite(input.nowMs) ? (input.nowMs as number) : this.now().getTime();
    const memories = this.listActiveMemories();
    if (memories.length === 0) {
      return [];
    }

    const evidenceCounts = this.readEvidenceCounts();
    const queryTokens = tokenize(query);

    const scored: ScoredMemory[] = memories.map((memory) => {
      const memoryTokens = tokenize(`${memory.title} ${memory.body} ${memory.tags.join(" ")}`);
      const semantic = jaccardSimilarity(queryTokens, memoryTokens);
      const keyword = keywordCoverage(
        queryTokens,
        `${memory.title}\n${memory.body}\n${memory.tags.join(" ")}`,
      );
      const typeBonus = typePriorityBonus(memory.type, queryTokens);
      const signals = deriveTemporalSignals({ memory, nowMs });
      const temporal =
        0.5 * signals.recencyScore +
        0.25 * (1 - signals.stalenessScore) +
        0.15 * signals.momentumScore +
        0.1 * signals.urgencyScore;
      const evidenceCount = evidenceCounts.get(memory.id) ?? 0;
      const confidenceEvidence =
        0.7 * clamp01(memory.confidence) + 0.3 * Math.min(1, evidenceCount / 5);

      const score = clamp01(
        0.32 * semantic +
          0.18 * keyword +
          0.1 * typeBonus +
          0.24 * temporal +
          0.16 * confidenceEvidence,
      );

      return {
        memory,
        score,
        rationale: buildRationale({
          queryTokens,
          memory,
          components: {
            semantic,
            keyword,
            typeBonus,
            temporal,
            confidenceEvidence,
          },
          signals,
          nowMs,
        }),
        components: { semantic, keyword, typeBonus, temporal, confidenceEvidence },
        signals,
      };
    });

    let ranked = scored.toSorted((a, b) => b.score - a.score);
    if (input.mmr) {
      const mmrItems = ranked.map((entry) => ({
        id: String(entry.memory.id),
        score: entry.score,
        content: `${entry.memory.title}\n${entry.memory.body}`,
      }));
      const reranked = mmrRerank(mmrItems, { enabled: true, lambda: 0.7 });
      const byId = new Map(ranked.map((entry) => [String(entry.memory.id), entry]));
      ranked = reranked.map((entry) => byId.get(entry.id)).filter(Boolean) as ScoredMemory[];
    }

    const selected = ranked.slice(0, topK);
    const evidenceByMemory = this.readEvidenceByMemoryIds(selected.map((entry) => entry.memory.id));
    const linksByMemory = this.readLinksByMemoryIds(selected.map((entry) => entry.memory.id));

    const nowIso = new Date(nowMs).toISOString();
    this.touchMemories(
      selected.map((entry) => entry.memory.id),
      nowIso,
    );

    return selected.map((entry) => {
      const evidence = evidenceByMemory.get(entry.memory.id) ?? [];
      const links = linksByMemory.get(entry.memory.id) ?? [];
      return {
        memory: entry.memory,
        score: entry.score,
        rationale: entry.rationale,
        provenance: this.buildProvenance({
          memoryId: entry.memory.id,
          evidence,
          links,
        }),
        stalenessScore: entry.signals.stalenessScore,
        momentumScore: entry.signals.momentumScore,
      };
    });
  }

  consolidateMemory(params?: {
    similarityThreshold?: number;
    minGroupSize?: number;
  }): ConsolidateResult {
    const similarityThreshold = clamp01(params?.similarityThreshold ?? 0.86);
    const minGroupSize = Math.max(2, Math.floor(params?.minGroupSize ?? 2));
    const memories = this.listActiveMemories();

    if (memories.length < minGroupSize) {
      return { mergedGroups: 0, mergedItems: 0, canonicalIds: [] };
    }

    const byType = new Map<DurableMemoryType, DurableMemoryItem[]>();
    for (const memory of memories) {
      const bucket = byType.get(memory.type) ?? [];
      bucket.push(memory);
      byType.set(memory.type, bucket);
    }

    const dsu = new DisjointSet(memories.map((entry) => entry.id));
    const similarityByPair = new Map<string, number>();

    for (const [, entries] of byType) {
      for (let i = 0; i < entries.length; i++) {
        for (let j = i + 1; j < entries.length; j++) {
          const left = entries[i];
          const right = entries[j];
          const similarity = jaccardSimilarity(
            tokenize(`${left.title} ${left.body}`),
            tokenize(`${right.title} ${right.body}`),
          );
          const tagOverlap = jaccardSimilarity(new Set(left.tags), new Set(right.tags));
          if (similarity >= similarityThreshold || tagOverlap >= 0.75) {
            dsu.union(left.id, right.id);
            similarityByPair.set(pairKey(left.id, right.id), Math.max(similarity, tagOverlap));
          }
        }
      }
    }

    const clustered = new Map<number, DurableMemoryItem[]>();
    for (const memory of memories) {
      const root = dsu.find(memory.id);
      const bucket = clustered.get(root) ?? [];
      bucket.push(memory);
      clustered.set(root, bucket);
    }

    const nowIso = this.nowIso();
    const canonicalIds: number[] = [];
    let mergedGroups = 0;
    let mergedItems = 0;

    this.withTransaction(() => {
      for (const [, cluster] of clustered) {
        if (cluster.length < minGroupSize) {
          continue;
        }

        const canonical = pickCanonicalMemory(cluster, Date.parse(nowIso));
        const merged = cluster.filter((entry) => entry.id !== canonical.id);
        if (merged.length === 0) {
          continue;
        }

        mergedGroups += 1;
        mergedItems += merged.length;
        canonicalIds.push(canonical.id);

        const all = [canonical, ...merged];
        const mergedTags = normalizeTags(all.flatMap((entry) => entry.tags));
        const mergedBody = mergeBodies(all.map((entry) => entry.body));
        const mergedCreatedAt = selectEarliestIso(all.map((entry) => entry.createdAt));
        const mergedLastSeenAt = selectLatestIso(all.map((entry) => entry.lastSeenAt));
        const mergedLastConfirmedAt = selectLatestIso(all.map((entry) => entry.lastConfirmedAt));
        const mergedValidUntil = selectMergedValidUntil(all.map((entry) => entry.validUntil));
        const mergedNextReviewAt = selectEarliestIso(all.map((entry) => entry.nextReviewAt));
        const mergedHorizon = all
          .map((entry) => entry.timeHorizon)
          .reduce((acc, current) => maxTimeHorizon(acc, current), canonical.timeHorizon);
        const avgConfidence =
          all.reduce((acc, entry) => acc + clamp01(entry.confidence), 0) / all.length;
        const boostedConfidence = clamp01(
          Math.max(canonical.confidence, avgConfidence) + 0.03 * merged.length,
        );
        const maxImportance = all.reduce(
          (acc, entry) => Math.max(acc, clamp01(entry.importance)),
          canonical.importance,
        );

        this.db
          .prepare(
            "UPDATE memory_items SET body=?, tags_json=?, confidence=?, importance=?, created_at=?, updated_at=?, last_seen_at=?, last_confirmed_at=?, time_horizon=?, valid_until=?, next_review_at=?, status='active', canonical_id=NULL WHERE id=?",
          )
          .run(
            mergedBody,
            toJsonStringArray(mergedTags),
            boostedConfidence,
            maxImportance,
            mergedCreatedAt,
            nowIso,
            mergedLastSeenAt,
            mergedLastConfirmedAt,
            mergedHorizon,
            mergedValidUntil,
            mergedNextReviewAt,
            canonical.id,
          );

        for (const old of merged) {
          this.db
            .prepare("UPDATE memory_evidence SET memory_id=? WHERE memory_id=?")
            .run(canonical.id, old.id);
          this.db
            .prepare("UPDATE memory_links SET memory_id=? WHERE memory_id=?")
            .run(canonical.id, old.id);
          this.db
            .prepare(
              "UPDATE memory_items SET status='merged', canonical_id=?, updated_at=? WHERE id=?",
            )
            .run(canonical.id, nowIso, old.id);
          const reason = `consolidated similarity=${(
            similarityByPair.get(pairKey(canonical.id, old.id)) ?? similarityThreshold
          ).toFixed(2)}`;
          this.db
            .prepare(
              "INSERT INTO memory_merges(old_memory_id, canonical_memory_id, reason, created_at) VALUES (?, ?, ?, ?)",
            )
            .run(old.id, canonical.id, reason, nowIso);
        }
      }
    });

    return { mergedGroups, mergedItems, canonicalIds };
  }

  reviewDueMemory(params?: { limit?: number; nowMs?: number }): ReviewCandidate[] {
    const limit = Number.isFinite(params?.limit)
      ? Math.max(1, Math.floor(params?.limit as number))
      : 10;
    const nowMs = Number.isFinite(params?.nowMs) ? (params?.nowMs as number) : this.now().getTime();
    const nowIso = new Date(nowMs).toISOString();
    const memories = this.listActiveMemories();

    const candidates: Array<ReviewCandidate & { priority: number }> = [];
    for (const memory of memories) {
      const signals = deriveTemporalSignals({ memory, nowMs });
      const daysSinceConfirmed = daysSince(memory.lastConfirmedAt ?? memory.updatedAt, nowMs);
      const daysToExpiry = daysTo(memory.validUntil, nowMs);
      const nextReviewInDays = daysTo(memory.nextReviewAt, nowMs);
      const reviewDue = nextReviewInDays !== null && nextReviewInDays <= 0;

      const reasons: string[] = [];
      let priority = 0;

      if (memory.importance >= 0.7 && signals.stalenessScore >= 0.6) {
        reasons.push("high importance + high staleness");
        priority += 4;
      }
      if (daysToExpiry !== null && daysToExpiry <= 7) {
        reasons.push(daysToExpiry < 0 ? "expired" : "near expiry");
        priority += 3;
      }
      if ((memory.type === "Identity" || memory.type === "Protocol") && daysSinceConfirmed >= 30) {
        reasons.push("unresolved identity/protocol item");
        priority += 2;
      }
      if (reviewDue) {
        reasons.push("scheduled review due");
        priority += 2;
      }

      if (reasons.length > 0) {
        candidates.push({
          memory,
          reason: reasons.join("; "),
          stalenessScore: signals.stalenessScore,
          momentumScore: signals.momentumScore,
          priority,
        });
      }
    }

    const selected = candidates
      .toSorted((a, b) => {
        if (b.priority !== a.priority) {
          return b.priority - a.priority;
        }
        return b.stalenessScore - a.stalenessScore;
      })
      .slice(0, limit);

    this.touchMemories(
      selected.map((entry) => entry.memory.id),
      nowIso,
    );

    return selected.map((entry) => ({
      memory: entry.memory,
      reason: entry.reason,
      stalenessScore: entry.stalenessScore,
      momentumScore: entry.momentumScore,
    }));
  }

  markConfirmed(memoryId: number): DurableMemoryItem | null {
    const resolvedId = this.resolveActiveMemoryId(memoryId);
    if (!resolvedId) {
      return null;
    }

    const nowIso = this.nowIso();
    this.db
      .prepare(
        "UPDATE memory_items SET last_confirmed_at=?, updated_at=?, confidence=MIN(1.0, confidence + 0.05) WHERE id=?",
      )
      .run(nowIso, nowIso, resolvedId);

    return this.readMemoryItem(resolvedId);
  }

  clearContext(): {
    scope: string;
    clearedEntries: number;
    memoryCountBefore: number;
    memoryCountAfter: number;
    auditEventId: number;
  } {
    const memoryCountBefore = this.selectCount("memory_items", "status='active'");
    const clearedEntries = this.selectCount("context_window_entries");
    const nowIso = this.nowIso();
    let auditEventId = 0;

    this.withTransaction(() => {
      this.db.exec("DELETE FROM context_window_entries");
      this.db
        .prepare("INSERT INTO context_resets(note, created_at) VALUES (?, ?)")
        .run(CONTEXT_RESET_NOTE, nowIso);
      const eventResult = this.db
        .prepare(
          "INSERT INTO events(role, text, summary, tags_json, created_at, source_type, source_ref) VALUES ('system', ?, ?, ?, ?, 'command', 'clear-context')",
        )
        .run(
          CONTEXT_RESET_NOTE,
          CONTEXT_RESET_NOTE,
          toJsonStringArray(["context", "reset"]),
          nowIso,
        );
      auditEventId = rowIdToNumber(eventResult.lastInsertRowid);
    });

    const memoryCountAfter = this.selectCount("memory_items", "status='active'");
    return {
      scope: CONTEXT_RESET_SCOPE,
      clearedEntries,
      memoryCountBefore,
      memoryCountAfter,
      auditEventId,
    };
  }

  setContextEntry(key: string, value: string): void {
    const cleanKey = key.trim();
    if (!cleanKey) {
      throw new Error("Context key is required");
    }
    this.db
      .prepare(
        "INSERT INTO context_window_entries(key, value, updated_at) VALUES (?, ?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
      )
      .run(cleanKey, value, this.nowIso());
  }

  private ensureSchemaAndMigrations(): void {
    const roleEnumSql = sqlEnumValues(EVENT_ROLES);
    const memoryTypeEnumSql = sqlEnumValues(DURABLE_MEMORY_TYPES);
    const timeHorizonEnumSql = sqlEnumValues(TIME_HORIZONS);
    const memoryStatusEnumSql = sqlEnumValues(MEMORY_STATUSES);
    const evidenceTypeEnumSql = sqlEnumValues(EVIDENCE_TYPES);
    const linkTypeEnumSql = sqlEnumValues(LINK_TYPES);

    this.db.exec(`
      CREATE TABLE IF NOT EXISTS meta (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        role TEXT NOT NULL CHECK(role IN (${roleEnumSql})),
        text TEXT NOT NULL,
        summary TEXT,
        tags_json TEXT NOT NULL DEFAULT '[]',
        created_at TEXT NOT NULL,
        source_type TEXT,
        source_ref TEXT,
        embedding TEXT
      );

      CREATE TABLE IF NOT EXISTS memory_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        type TEXT NOT NULL CHECK(type IN (${memoryTypeEnumSql})),
        title TEXT NOT NULL,
        body TEXT NOT NULL,
        tags_json TEXT NOT NULL DEFAULT '[]',
        confidence REAL NOT NULL DEFAULT ${DEFAULT_CONFIDENCE} CHECK(confidence >= 0 AND confidence <= 1),
        importance REAL NOT NULL DEFAULT ${DEFAULT_IMPORTANCE} CHECK(importance >= 0 AND importance <= 1),
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        last_seen_at TEXT,
        last_confirmed_at TEXT,
        time_horizon TEXT NOT NULL DEFAULT '${DEFAULT_TIME_HORIZON}' CHECK(time_horizon IN (${timeHorizonEnumSql})),
        valid_until TEXT,
        next_review_at TEXT,
        status TEXT NOT NULL DEFAULT 'active' CHECK(status IN (${memoryStatusEnumSql})),
        canonical_id INTEGER,
        FOREIGN KEY(canonical_id) REFERENCES memory_items(id) ON DELETE SET NULL
      );

      CREATE TABLE IF NOT EXISTS memory_evidence (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        memory_id INTEGER NOT NULL,
        event_id INTEGER,
        evidence_type TEXT NOT NULL CHECK(evidence_type IN (${evidenceTypeEnumSql})),
        note TEXT,
        created_at TEXT NOT NULL,
        FOREIGN KEY(memory_id) REFERENCES memory_items(id) ON DELETE CASCADE,
        FOREIGN KEY(event_id) REFERENCES events(id) ON DELETE SET NULL
      );

      CREATE TABLE IF NOT EXISTS memory_links (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        memory_id INTEGER NOT NULL,
        link_type TEXT NOT NULL CHECK(link_type IN (${linkTypeEnumSql})),
        link_target TEXT NOT NULL,
        label TEXT,
        created_at TEXT NOT NULL,
        FOREIGN KEY(memory_id) REFERENCES memory_items(id) ON DELETE CASCADE
      );

      CREATE TABLE IF NOT EXISTS memory_merges (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        old_memory_id INTEGER NOT NULL,
        canonical_memory_id INTEGER NOT NULL,
        reason TEXT NOT NULL,
        created_at TEXT NOT NULL,
        FOREIGN KEY(old_memory_id) REFERENCES memory_items(id) ON DELETE CASCADE,
        FOREIGN KEY(canonical_memory_id) REFERENCES memory_items(id) ON DELETE CASCADE
      );

      CREATE TABLE IF NOT EXISTS context_window_entries (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS context_resets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        note TEXT NOT NULL,
        created_at TEXT NOT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_events_created_at ON events(created_at);
      CREATE INDEX IF NOT EXISTS idx_memory_items_status ON memory_items(status);
      CREATE INDEX IF NOT EXISTS idx_memory_items_type ON memory_items(type);
      CREATE INDEX IF NOT EXISTS idx_memory_items_last_seen ON memory_items(last_seen_at);
      CREATE INDEX IF NOT EXISTS idx_memory_items_valid_until ON memory_items(valid_until);
      CREATE INDEX IF NOT EXISTS idx_memory_items_next_review ON memory_items(next_review_at);
      CREATE INDEX IF NOT EXISTS idx_memory_evidence_memory ON memory_evidence(memory_id);
      CREATE INDEX IF NOT EXISTS idx_memory_evidence_event ON memory_evidence(event_id);
      CREATE INDEX IF NOT EXISTS idx_memory_links_memory ON memory_links(memory_id);
      CREATE INDEX IF NOT EXISTS idx_memory_merges_old ON memory_merges(old_memory_id);
      CREATE INDEX IF NOT EXISTS idx_memory_merges_canonical ON memory_merges(canonical_memory_id);
    `);

    ensureColumn(this.db, "events", "tags_json", "TEXT NOT NULL DEFAULT '[]'");
    ensureColumn(this.db, "events", "created_at", "TEXT NOT NULL DEFAULT ''");
    ensureColumn(this.db, "events", "embedding", "TEXT");

    ensureColumn(this.db, "memory_items", "last_seen_at", "TEXT");
    ensureColumn(this.db, "memory_items", "last_confirmed_at", "TEXT");
    ensureColumn(this.db, "memory_items", "time_horizon", "TEXT NOT NULL DEFAULT 'medium'");
    ensureColumn(this.db, "memory_items", "valid_until", "TEXT");
    ensureColumn(this.db, "memory_items", "next_review_at", "TEXT");
    ensureColumn(this.db, "memory_items", "status", "TEXT NOT NULL DEFAULT 'active'");
    ensureColumn(this.db, "memory_items", "canonical_id", "INTEGER");
    ensureColumn(this.db, "memory_evidence", "note", "TEXT");
    ensureColumn(this.db, "memory_evidence", "created_at", "TEXT NOT NULL DEFAULT ''");
    ensureColumn(this.db, "memory_links", "created_at", "TEXT NOT NULL DEFAULT ''");

    const nowIso = this.nowIso();
    this.db
      .prepare(
        `UPDATE events SET role='system' WHERE role IS NULL OR trim(role)='' OR role NOT IN (${roleEnumSql})`,
      )
      .run();
    this.db
      .prepare("UPDATE events SET tags_json=? WHERE tags_json IS NULL OR trim(tags_json)='' ")
      .run(DEFAULT_TAGS_JSON);
    this.db
      .prepare("UPDATE events SET created_at=? WHERE created_at IS NULL OR trim(created_at)='' ")
      .run(nowIso);

    this.db
      .prepare("UPDATE memory_items SET tags_json=? WHERE tags_json IS NULL OR trim(tags_json)='' ")
      .run(DEFAULT_TAGS_JSON);
    this.db
      .prepare(
        `UPDATE memory_items SET type='Lesson' WHERE type IS NULL OR trim(type)='' OR type NOT IN (${memoryTypeEnumSql})`,
      )
      .run();
    this.db
      .prepare(
        "UPDATE memory_items SET created_at=? WHERE created_at IS NULL OR trim(created_at)=''",
      )
      .run(nowIso);
    this.db
      .prepare(
        "UPDATE memory_items SET updated_at=created_at WHERE updated_at IS NULL OR trim(updated_at)='' ",
      )
      .run();
    this.db
      .prepare(
        "UPDATE memory_items SET confidence=? WHERE confidence IS NULL OR confidence < 0 OR confidence > 1",
      )
      .run(DEFAULT_CONFIDENCE);
    this.db
      .prepare(
        "UPDATE memory_items SET importance=? WHERE importance IS NULL OR importance < 0 OR importance > 1",
      )
      .run(DEFAULT_IMPORTANCE);
    this.db
      .prepare(
        `UPDATE memory_items SET time_horizon=? WHERE time_horizon IS NULL OR trim(time_horizon)='' OR time_horizon NOT IN (${timeHorizonEnumSql})`,
      )
      .run(DEFAULT_TIME_HORIZON);
    this.db
      .prepare(
        `UPDATE memory_items SET status='active' WHERE status IS NULL OR trim(status)='' OR status NOT IN (${memoryStatusEnumSql})`,
      )
      .run();
    this.db
      .prepare(
        "UPDATE memory_items SET canonical_id=NULL WHERE canonical_id IS NOT NULL AND canonical_id NOT IN (SELECT id FROM memory_items)",
      )
      .run();
    this.db
      .prepare(
        `UPDATE memory_evidence SET evidence_type='quote' WHERE evidence_type IS NULL OR trim(evidence_type)='' OR evidence_type NOT IN (${evidenceTypeEnumSql})`,
      )
      .run();
    this.db
      .prepare("UPDATE memory_evidence SET note=NULL WHERE note IS NOT NULL AND trim(note)='' ")
      .run();
    this.db
      .prepare(
        "UPDATE memory_evidence SET created_at=? WHERE created_at IS NULL OR trim(created_at)='' ",
      )
      .run(nowIso);
    this.db
      .prepare(
        `UPDATE memory_links SET link_type='note' WHERE link_type IS NULL OR trim(link_type)='' OR link_type NOT IN (${linkTypeEnumSql})`,
      )
      .run();
    this.db
      .prepare(
        "UPDATE memory_links SET created_at=? WHERE created_at IS NULL OR trim(created_at)='' ",
      )
      .run(nowIso);

    this.dedupeEvidenceRows();
    this.ensureEvidenceUniquenessIndex();
    if (!this.hasStrictSchema()) {
      this.applyStrictSchemaMigration(nowIso);
      this.dedupeEvidenceRows();
      this.ensureEvidenceUniquenessIndex();
    }
    this.db
      .prepare(
        "INSERT INTO meta(key, value) VALUES('durable_schema_version', ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
      )
      .run(DURABLE_SCHEMA_VERSION);
  }

  private ensureEvidenceUniquenessIndex(): void {
    this.db.exec(`
      CREATE UNIQUE INDEX IF NOT EXISTS ${EVIDENCE_UNIQUE_INDEX}
      ON memory_evidence(memory_id, COALESCE(event_id, -1), evidence_type, COALESCE(note, ''));
    `);
  }

  private hasStrictSchema(): boolean {
    const roleEnumSql = sqlEnumValues(EVENT_ROLES);
    const memoryTypeEnumSql = sqlEnumValues(DURABLE_MEMORY_TYPES);
    const timeHorizonEnumSql = sqlEnumValues(TIME_HORIZONS);
    const memoryStatusEnumSql = sqlEnumValues(MEMORY_STATUSES);
    const evidenceTypeEnumSql = sqlEnumValues(EVIDENCE_TYPES);
    const linkTypeEnumSql = sqlEnumValues(LINK_TYPES);

    const eventsSql = readSchemaSql(this.db, "table", "events");
    const memoryItemsSql = readSchemaSql(this.db, "table", "memory_items");
    const memoryEvidenceSql = readSchemaSql(this.db, "table", "memory_evidence");
    const memoryLinksSql = readSchemaSql(this.db, "table", "memory_links");
    const evidenceIndexSql = readSchemaSql(this.db, "index", EVIDENCE_UNIQUE_INDEX);

    return (
      sqlHasFragment(eventsSql, `check(role in (${roleEnumSql}))`) &&
      sqlHasFragment(memoryItemsSql, `check(type in (${memoryTypeEnumSql}))`) &&
      sqlHasFragment(memoryItemsSql, "check(confidence >= 0 and confidence <= 1)") &&
      sqlHasFragment(memoryItemsSql, "check(importance >= 0 and importance <= 1)") &&
      sqlHasFragment(memoryItemsSql, `check(time_horizon in (${timeHorizonEnumSql}))`) &&
      sqlHasFragment(memoryItemsSql, `check(status in (${memoryStatusEnumSql}))`) &&
      sqlHasFragment(memoryEvidenceSql, `check(evidence_type in (${evidenceTypeEnumSql}))`) &&
      sqlHasFragment(memoryLinksSql, `check(link_type in (${linkTypeEnumSql}))`) &&
      typeof evidenceIndexSql === "string" &&
      evidenceIndexSql.toLowerCase().includes("unique")
    );
  }

  private applyStrictSchemaMigration(nowIso: string): void {
    const roleEnumSql = sqlEnumValues(EVENT_ROLES);
    const memoryTypeEnumSql = sqlEnumValues(DURABLE_MEMORY_TYPES);
    const timeHorizonEnumSql = sqlEnumValues(TIME_HORIZONS);
    const memoryStatusEnumSql = sqlEnumValues(MEMORY_STATUSES);
    const evidenceTypeEnumSql = sqlEnumValues(EVIDENCE_TYPES);
    const linkTypeEnumSql = sqlEnumValues(LINK_TYPES);

    this.db.exec("PRAGMA foreign_keys = OFF;");
    try {
      this.withTransaction(() => {
        this.db.exec(`
          DROP TABLE IF EXISTS events_v2;
          DROP TABLE IF EXISTS memory_items_v2;
          DROP TABLE IF EXISTS memory_evidence_v2;
          DROP TABLE IF EXISTS memory_links_v2;
          DROP TABLE IF EXISTS memory_merges_v2;

          CREATE TABLE events_v2 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            role TEXT NOT NULL CHECK(role IN (${roleEnumSql})),
            text TEXT NOT NULL,
            summary TEXT,
            tags_json TEXT NOT NULL DEFAULT '[]',
            created_at TEXT NOT NULL,
            source_type TEXT,
            source_ref TEXT,
            embedding TEXT
          );

          CREATE TABLE memory_items_v2 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT NOT NULL CHECK(type IN (${memoryTypeEnumSql})),
            title TEXT NOT NULL,
            body TEXT NOT NULL,
            tags_json TEXT NOT NULL DEFAULT '[]',
            confidence REAL NOT NULL DEFAULT ${DEFAULT_CONFIDENCE} CHECK(confidence >= 0 AND confidence <= 1),
            importance REAL NOT NULL DEFAULT ${DEFAULT_IMPORTANCE} CHECK(importance >= 0 AND importance <= 1),
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            last_seen_at TEXT,
            last_confirmed_at TEXT,
            time_horizon TEXT NOT NULL DEFAULT '${DEFAULT_TIME_HORIZON}' CHECK(time_horizon IN (${timeHorizonEnumSql})),
            valid_until TEXT,
            next_review_at TEXT,
            status TEXT NOT NULL DEFAULT 'active' CHECK(status IN (${memoryStatusEnumSql})),
            canonical_id INTEGER,
            FOREIGN KEY(canonical_id) REFERENCES memory_items(id) ON DELETE SET NULL
          );

          CREATE TABLE memory_evidence_v2 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            memory_id INTEGER NOT NULL,
            event_id INTEGER,
            evidence_type TEXT NOT NULL CHECK(evidence_type IN (${evidenceTypeEnumSql})),
            note TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY(memory_id) REFERENCES memory_items(id) ON DELETE CASCADE,
            FOREIGN KEY(event_id) REFERENCES events(id) ON DELETE SET NULL
          );

          CREATE TABLE memory_links_v2 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            memory_id INTEGER NOT NULL,
            link_type TEXT NOT NULL CHECK(link_type IN (${linkTypeEnumSql})),
            link_target TEXT NOT NULL,
            label TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY(memory_id) REFERENCES memory_items(id) ON DELETE CASCADE
          );

          CREATE TABLE memory_merges_v2 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            old_memory_id INTEGER NOT NULL,
            canonical_memory_id INTEGER NOT NULL,
            reason TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(old_memory_id) REFERENCES memory_items(id) ON DELETE CASCADE,
            FOREIGN KEY(canonical_memory_id) REFERENCES memory_items(id) ON DELETE CASCADE
          );
        `);

        this.db
          .prepare(`
            INSERT INTO events_v2(id, role, text, summary, tags_json, created_at, source_type, source_ref, embedding)
            SELECT
              id,
              CASE
                WHEN role IN (${roleEnumSql}) THEN role
                ELSE 'system'
              END,
              text,
              summary,
              CASE
                WHEN tags_json IS NULL OR trim(tags_json)=''
                  THEN ?
                ELSE tags_json
              END,
              CASE
                WHEN created_at IS NULL OR trim(created_at)=''
                  THEN ?
                ELSE created_at
              END,
              source_type,
              source_ref,
              embedding
            FROM events
          `)
          .run(DEFAULT_TAGS_JSON, nowIso);

        this.db
          .prepare(`
            INSERT INTO memory_items_v2(
              id,
              type,
              title,
              body,
              tags_json,
              confidence,
              importance,
              created_at,
              updated_at,
              last_seen_at,
              last_confirmed_at,
              time_horizon,
              valid_until,
              next_review_at,
              status,
              canonical_id
            )
            SELECT
              id,
              CASE
                WHEN type IN (${memoryTypeEnumSql}) THEN type
                ELSE 'Lesson'
              END,
              title,
              body,
              CASE
                WHEN tags_json IS NULL OR trim(tags_json)=''
                  THEN ?
                ELSE tags_json
              END,
              CASE
                WHEN confidence IS NULL OR confidence < 0 OR confidence > 1
                  THEN ?
                ELSE confidence
              END,
              CASE
                WHEN importance IS NULL OR importance < 0 OR importance > 1
                  THEN ?
                ELSE importance
              END,
              CASE
                WHEN created_at IS NULL OR trim(created_at)=''
                  THEN ?
                ELSE created_at
              END,
              CASE
                WHEN updated_at IS NULL OR trim(updated_at)=''
                  THEN CASE
                    WHEN created_at IS NULL OR trim(created_at)=''
                      THEN ?
                    ELSE created_at
                  END
                ELSE updated_at
              END,
              last_seen_at,
              last_confirmed_at,
              CASE
                WHEN time_horizon IN (${timeHorizonEnumSql})
                  THEN time_horizon
                ELSE '${DEFAULT_TIME_HORIZON}'
              END,
              valid_until,
              next_review_at,
              CASE
                WHEN status IN (${memoryStatusEnumSql})
                  THEN status
                ELSE 'active'
              END,
              CASE
                WHEN canonical_id IN (SELECT id FROM memory_items)
                  THEN canonical_id
                ELSE NULL
              END
            FROM memory_items
          `)
          .run(DEFAULT_TAGS_JSON, DEFAULT_CONFIDENCE, DEFAULT_IMPORTANCE, nowIso, nowIso);

        this.db
          .prepare(`
            INSERT INTO memory_evidence_v2(id, memory_id, event_id, evidence_type, note, created_at)
            SELECT
              MIN(id) AS id,
              memory_id,
              CASE
                WHEN event_id IN (SELECT id FROM events)
                  THEN event_id
                ELSE NULL
              END AS normalized_event_id,
              CASE
                WHEN evidence_type IN (${evidenceTypeEnumSql})
                  THEN evidence_type
                ELSE 'quote'
              END,
              NULLIF(trim(note), ''),
              MIN(CASE
                WHEN created_at IS NULL OR trim(created_at)=''
                  THEN ?
                ELSE created_at
              END)
            FROM memory_evidence
            WHERE memory_id IN (SELECT id FROM memory_items)
            GROUP BY
              memory_id,
              CASE
                WHEN event_id IN (SELECT id FROM events)
                  THEN event_id
                ELSE NULL
              END,
              CASE
                WHEN evidence_type IN (${evidenceTypeEnumSql})
                  THEN evidence_type
                ELSE 'quote'
              END,
              COALESCE(NULLIF(trim(note), ''), '')
          `)
          .run(nowIso);

        this.db
          .prepare(`
            INSERT INTO memory_links_v2(id, memory_id, link_type, link_target, label, created_at)
            SELECT
              id,
              memory_id,
              CASE
                WHEN link_type IN (${linkTypeEnumSql})
                  THEN link_type
                ELSE 'note'
              END,
              trim(link_target),
              NULLIF(trim(label), ''),
              CASE
                WHEN created_at IS NULL OR trim(created_at)=''
                  THEN ?
                ELSE created_at
              END
            FROM memory_links
            WHERE
              memory_id IN (SELECT id FROM memory_items)
              AND link_target IS NOT NULL
              AND trim(link_target) <> ''
          `)
          .run(nowIso);

        this.db
          .prepare(`
            INSERT INTO memory_merges_v2(id, old_memory_id, canonical_memory_id, reason, created_at)
            SELECT
              id,
              old_memory_id,
              canonical_memory_id,
              COALESCE(NULLIF(trim(reason), ''), 'consolidated'),
              CASE
                WHEN created_at IS NULL OR trim(created_at)=''
                  THEN ?
                ELSE created_at
              END
            FROM memory_merges
            WHERE
              old_memory_id IN (SELECT id FROM memory_items)
              AND canonical_memory_id IN (SELECT id FROM memory_items)
          `)
          .run(nowIso);

        this.db.exec(`
          DROP TABLE memory_evidence;
          DROP TABLE memory_links;
          DROP TABLE memory_merges;
          DROP TABLE memory_items;
          DROP TABLE events;

          ALTER TABLE events_v2 RENAME TO events;
          ALTER TABLE memory_items_v2 RENAME TO memory_items;
          ALTER TABLE memory_evidence_v2 RENAME TO memory_evidence;
          ALTER TABLE memory_links_v2 RENAME TO memory_links;
          ALTER TABLE memory_merges_v2 RENAME TO memory_merges;

          CREATE INDEX IF NOT EXISTS idx_events_created_at ON events(created_at);
          CREATE INDEX IF NOT EXISTS idx_memory_items_status ON memory_items(status);
          CREATE INDEX IF NOT EXISTS idx_memory_items_type ON memory_items(type);
          CREATE INDEX IF NOT EXISTS idx_memory_items_last_seen ON memory_items(last_seen_at);
          CREATE INDEX IF NOT EXISTS idx_memory_items_valid_until ON memory_items(valid_until);
          CREATE INDEX IF NOT EXISTS idx_memory_items_next_review ON memory_items(next_review_at);
          CREATE INDEX IF NOT EXISTS idx_memory_evidence_memory ON memory_evidence(memory_id);
          CREATE INDEX IF NOT EXISTS idx_memory_evidence_event ON memory_evidence(event_id);
          CREATE INDEX IF NOT EXISTS idx_memory_links_memory ON memory_links(memory_id);
          CREATE INDEX IF NOT EXISTS idx_memory_merges_old ON memory_merges(old_memory_id);
          CREATE INDEX IF NOT EXISTS idx_memory_merges_canonical ON memory_merges(canonical_memory_id);
        `);
      });
    } finally {
      this.db.exec("PRAGMA foreign_keys = ON;");
    }
  }

  private dedupeEvidenceRows(): void {
    this.db
      .prepare(`
        DELETE FROM memory_evidence
        WHERE id NOT IN (
          SELECT MIN(id)
          FROM memory_evidence
          GROUP BY
            memory_id,
            COALESCE(event_id, -1),
            evidence_type,
            COALESCE(NULLIF(trim(note), ''), '')
        )
      `)
      .run();
    this.db
      .prepare("UPDATE memory_evidence SET note=NULL WHERE note IS NOT NULL AND trim(note)='' ")
      .run();
  }

  private withTransaction(fn: () => void): void {
    this.db.exec("BEGIN");
    try {
      fn();
      this.db.exec("COMMIT");
    } catch (err) {
      try {
        this.db.exec("ROLLBACK");
      } catch (rollbackErr) {
        log.warn(`durable memory rollback failed: ${String(rollbackErr)}`);
      }
      throw err;
    }
  }

  private addEvidenceLinks(params: {
    memoryId: number;
    eventIds?: number[];
    nowIso: string;
    evidenceType: MemoryEvidenceType;
  }): void {
    const evidenceType = parseEvidenceTypeInput(params.evidenceType);
    const eventIds = parseEventIdsInput(params.eventIds);
    if (eventIds.length === 0) {
      return;
    }

    const uniqueIds = [...new Set(eventIds)];
    const placeholders = uniqueIds.map(() => "?").join(", ");
    const validRows = this.db
      .prepare(`SELECT id FROM events WHERE id IN (${placeholders})`)
      .all(...uniqueIds) as Array<{ id: number }>;
    if (validRows.length === 0) {
      return;
    }

    const insert = this.db.prepare(
      "INSERT OR IGNORE INTO memory_evidence(memory_id, event_id, evidence_type, note, created_at) VALUES (?, ?, ?, NULL, ?)",
    );
    for (const row of validRows) {
      insert.run(params.memoryId, row.id, evidenceType, params.nowIso);
    }
  }

  private listActiveMemories(): DurableMemoryItem[] {
    const rows = this.db
      .prepare("SELECT * FROM memory_items WHERE status='active'")
      .all() as MemoryItemRow[];
    return rows.map((row) => this.rowToMemoryItem(row));
  }

  private readMemoryItem(memoryId: number): DurableMemoryItem | null {
    const row = this.db.prepare("SELECT * FROM memory_items WHERE id=? LIMIT 1").get(memoryId) as
      | MemoryItemRow
      | undefined;
    if (!row) {
      return null;
    }
    return this.rowToMemoryItem(row);
  }

  private readEvidenceForMemory(memoryId: number): DurableMemoryEvidence[] {
    const rows = this.db
      .prepare("SELECT * FROM memory_evidence WHERE memory_id=? ORDER BY id ASC")
      .all(memoryId) as EvidenceRow[];
    return rows.map((row) => this.rowToEvidence(row));
  }

  private readLinksForMemory(memoryId: number): DurableMemoryLink[] {
    const rows = this.db
      .prepare("SELECT * FROM memory_links WHERE memory_id=? ORDER BY id ASC")
      .all(memoryId) as LinkRow[];
    return rows.map((row) => this.rowToLink(row));
  }

  private readEvidenceByMemoryIds(memoryIds: number[]): Map<number, DurableMemoryEvidence[]> {
    if (memoryIds.length === 0) {
      return new Map();
    }
    const uniqueIds = [...new Set(memoryIds)];
    const placeholders = uniqueIds.map(() => "?").join(", ");
    const rows = this.db
      .prepare(`SELECT * FROM memory_evidence WHERE memory_id IN (${placeholders}) ORDER BY id ASC`)
      .all(...uniqueIds) as EvidenceRow[];

    const grouped = new Map<number, DurableMemoryEvidence[]>();
    for (const row of rows) {
      const evidence = this.rowToEvidence(row);
      const list = grouped.get(evidence.memoryId) ?? [];
      list.push(evidence);
      grouped.set(evidence.memoryId, list);
    }
    return grouped;
  }

  private readLinksByMemoryIds(memoryIds: number[]): Map<number, DurableMemoryLink[]> {
    if (memoryIds.length === 0) {
      return new Map();
    }
    const uniqueIds = [...new Set(memoryIds)];
    const placeholders = uniqueIds.map(() => "?").join(", ");
    const rows = this.db
      .prepare(`SELECT * FROM memory_links WHERE memory_id IN (${placeholders}) ORDER BY id ASC`)
      .all(...uniqueIds) as LinkRow[];

    const grouped = new Map<number, DurableMemoryLink[]>();
    for (const row of rows) {
      const link = this.rowToLink(row);
      const list = grouped.get(link.memoryId) ?? [];
      list.push(link);
      grouped.set(link.memoryId, list);
    }
    return grouped;
  }

  private readEvidenceCounts(): Map<number, number> {
    const rows = this.db
      .prepare("SELECT memory_id, COUNT(*) AS count FROM memory_evidence GROUP BY memory_id")
      .all() as Array<{ memory_id: number; count: number }>;
    const map = new Map<number, number>();
    for (const row of rows) {
      map.set(row.memory_id, Number(row.count) || 0);
    }
    return map;
  }

  private buildProvenance(params: {
    memoryId: number;
    evidence: DurableMemoryEvidence[];
    links: DurableMemoryLink[];
  }): DurableProvenance {
    const eventRows = this.db
      .prepare(
        "SELECT me.event_id AS event_id, e.created_at AS event_created_at, me.created_at AS evidence_created_at FROM memory_evidence me LEFT JOIN events e ON e.id = me.event_id WHERE me.memory_id=? AND me.event_id IS NOT NULL ORDER BY me.id ASC",
      )
      .all(params.memoryId) as Array<{
      event_id: number;
      event_created_at: string | null;
      evidence_created_at: string;
    }>;

    const sourcePaths = params.links
      .filter(
        (entry) =>
          entry.linkType === "file" || entry.linkType === "repo" || entry.linkType === "url",
      )
      .map((entry) => entry.linkTarget);

    const evidenceTimestamps = [
      ...eventRows
        .map((entry) => entry.event_created_at)
        .filter((entry): entry is string => typeof entry === "string" && entry.length > 0),
      ...eventRows.map((entry) => entry.evidence_created_at),
    ];

    return {
      sourcePaths: uniqueStrings(sourcePaths),
      eventIds: uniqueNumbers(eventRows.map((entry) => entry.event_id)),
      evidenceTimestamps: uniqueStrings(evidenceTimestamps),
      linkTargets: uniqueStrings(params.links.map((entry) => entry.linkTarget)),
    };
  }

  private rowToMemoryItem(row: MemoryItemRow): DurableMemoryItem {
    return {
      id: Number(row.id),
      type: parseMemoryTypeForRead(row.type),
      title: row.title,
      body: row.body,
      tags: parseJsonTags(row.tags_json),
      confidence: normalizeUnitIntervalForRead(row.confidence, DEFAULT_CONFIDENCE, "confidence"),
      importance: normalizeUnitIntervalForRead(row.importance, DEFAULT_IMPORTANCE, "importance"),
      createdAt: normalizeIsoDate(row.created_at) ?? row.created_at,
      updatedAt: normalizeIsoDate(row.updated_at) ?? row.updated_at,
      lastSeenAt: normalizeIsoDate(row.last_seen_at),
      lastConfirmedAt: normalizeIsoDate(row.last_confirmed_at),
      timeHorizon: parseTimeHorizonForRead(row.time_horizon),
      validUntil: normalizeIsoDate(row.valid_until),
      nextReviewAt: normalizeIsoDate(row.next_review_at),
      status: parseMemoryStatusForRead(row.status),
      canonicalId: row.canonical_id != null ? Number(row.canonical_id) : null,
    };
  }

  private rowToEvidence(row: EvidenceRow): DurableMemoryEvidence {
    return {
      id: Number(row.id),
      memoryId: Number(row.memory_id),
      eventId: row.event_id != null ? Number(row.event_id) : null,
      evidenceType: parseEvidenceTypeForRead(row.evidence_type),
      note: trimOrNull(row.note),
      createdAt: normalizeIsoDate(row.created_at) ?? row.created_at,
    };
  }

  private rowToLink(row: LinkRow): DurableMemoryLink {
    return {
      id: Number(row.id),
      memoryId: Number(row.memory_id),
      linkType: parseLinkTypeForRead(row.link_type),
      linkTarget: row.link_target,
      label: trimOrNull(row.label),
      createdAt: normalizeIsoDate(row.created_at) ?? row.created_at,
    };
  }

  private resolveActiveMemoryId(memoryId: number): number | null {
    const visited = new Set<number>();
    let current = memoryId;

    while (!visited.has(current)) {
      visited.add(current);
      const row = this.db
        .prepare("SELECT id, status, canonical_id FROM memory_items WHERE id=? LIMIT 1")
        .get(current) as
        | {
            id: number;
            status: string;
            canonical_id: number | null;
          }
        | undefined;
      if (!row) {
        return null;
      }
      const status = parseMemoryStatusForRead(row.status);
      if (status === "active") {
        return Number(row.id);
      }
      if (row.canonical_id == null) {
        return Number(row.id);
      }
      current = Number(row.canonical_id);
    }

    return null;
  }

  private touchMemory(memoryId: number, nowIso: string): void {
    this.db
      .prepare("UPDATE memory_items SET last_seen_at=?, updated_at=updated_at WHERE id=?")
      .run(nowIso, memoryId);
  }

  private touchMemories(memoryIds: number[], nowIso: string): void {
    if (memoryIds.length === 0) {
      return;
    }
    const uniqueIds = [...new Set(memoryIds)].filter(
      (entry) => Number.isInteger(entry) && entry > 0,
    );
    if (uniqueIds.length === 0) {
      return;
    }
    const placeholders = uniqueIds.map(() => "?").join(", ");
    this.db
      .prepare(`UPDATE memory_items SET last_seen_at=? WHERE id IN (${placeholders})`)
      .run(nowIso, ...uniqueIds);
  }

  private selectCount(table: string, whereClause?: string): number {
    const clause = whereClause?.trim() ? ` WHERE ${whereClause}` : "";
    const row = this.db.prepare(`SELECT COUNT(*) as count FROM ${table}${clause}`).get() as {
      count: number;
    };
    return Number(row.count) || 0;
  }

  private nowIso(): string {
    return this.now().toISOString();
  }
}

export function deriveTemporalSignals(params: {
  memory: DurableMemoryItem;
  nowMs: number;
}): TemporalSignals {
  const lastTouch =
    params.memory.lastSeenAt ?? params.memory.lastConfirmedAt ?? params.memory.updatedAt;
  const recencyDays = daysSince(lastTouch, params.nowMs);
  const recencyScore = Math.exp(-Math.max(0, recencyDays) / 45);

  const confirmedDays = daysSince(
    params.memory.lastConfirmedAt ?? params.memory.updatedAt,
    params.nowMs,
  );
  let stalenessScore = clamp01((confirmedDays - 14) / 90);

  const validUntilDays = daysTo(params.memory.validUntil, params.nowMs);
  if (validUntilDays !== null && validUntilDays < 0) {
    stalenessScore = 1;
  }

  if (params.memory.timeHorizon === "evergreen") {
    stalenessScore = stalenessScore * 0.45;
  }
  if (params.memory.confidence >= 0.85) {
    stalenessScore = stalenessScore * 0.75;
  }

  const seenDays = daysSince(params.memory.lastSeenAt ?? params.memory.updatedAt, params.nowMs);
  const reactivation = seenDays <= 3 && confirmedDays >= 21 ? clamp01(1 - seenDays / 3) : 0;
  const freshnessMomentum = clamp01(1 - seenDays / 21);
  const momentumScore = clamp01(0.6 * freshnessMomentum + 0.4 * reactivation);

  let urgencyScore = 0;
  if (validUntilDays !== null) {
    if (validUntilDays <= 0) {
      urgencyScore = 1;
    } else if (validUntilDays <= 7) {
      urgencyScore = clamp01(1 - validUntilDays / 7);
    }
  }

  return {
    stalenessScore,
    momentumScore,
    recencyScore,
    urgencyScore,
  };
}

function buildRationale(params: {
  queryTokens: Set<string>;
  memory: DurableMemoryItem;
  components: DurableScoreComponents;
  signals: TemporalSignals;
  nowMs: number;
}): string {
  const pieces: string[] = [];

  if (params.components.keyword >= 0.25) {
    const hits = [...params.queryTokens].filter((token) => {
      const haystack = `${params.memory.title} ${params.memory.body}`.toLowerCase();
      return haystack.includes(token);
    });
    if (hits.length > 0) {
      pieces.push(`keyword match: ${hits.slice(0, 4).join(", ")}`);
    }
  }

  if (params.components.typeBonus >= 0.2) {
    pieces.push(`type priority: ${params.memory.type}`);
  }

  const lastSeenDays = daysSince(params.memory.lastSeenAt ?? params.memory.updatedAt, params.nowMs);
  if (lastSeenDays <= 14) {
    pieces.push(`recently active (${Math.round(lastSeenDays)}d)`);
  }

  const expiryDays = daysTo(params.memory.validUntil, params.nowMs);
  if (expiryDays !== null && expiryDays <= 7) {
    pieces.push(expiryDays < 0 ? "expired memory" : `near expiry (${Math.ceil(expiryDays)}d)`);
  }

  if (params.memory.confidence >= 0.8) {
    pieces.push(`high confidence (${params.memory.confidence.toFixed(2)})`);
  }

  if (pieces.length === 0) {
    pieces.push("semantic + temporal relevance");
  }

  return pieces.join("; ");
}

function ensureColumn(db: DatabaseSync, table: string, column: string, definition: string): void {
  const columns = db.prepare(`PRAGMA table_info(${table})`).all() as Array<{ name: string }>;
  if (columns.some((entry) => entry.name === column)) {
    return;
  }
  db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`);
}

function sqlEnumValues(values: readonly string[]): string {
  return values.map((entry) => `'${entry.replaceAll("'", "''")}'`).join(", ");
}

function readSchemaSql(db: DatabaseSync, kind: "table" | "index", name: string): string | null {
  const row = db
    .prepare("SELECT sql FROM sqlite_master WHERE type=? AND name=? LIMIT 1")
    .get(kind, name) as { sql?: string | null } | undefined;
  if (!row || typeof row.sql !== "string") {
    return null;
  }
  return row.sql;
}

function sqlHasFragment(sql: string | null, fragment: string): boolean {
  if (!sql) {
    return false;
  }
  const normalize = (value: string) => value.toLowerCase().replaceAll(/\s+/g, " ").trim();
  return normalize(sql).includes(normalize(fragment));
}

function warnLegacyParse(message: string): void {
  if (legacyParseWarnings.has(message)) {
    return;
  }
  legacyParseWarnings.add(message);
  log.warn(message);
}

function parseEnumInput<T extends string>(value: unknown, allowed: readonly T[], label: string): T {
  const normalized = typeof value === "string" ? value.trim() : "";
  if (allowed.includes(normalized as T)) {
    return normalized as T;
  }
  throw new Error(`Invalid ${label}: ${String(value)}. Allowed: ${allowed.join(", ")}`);
}

function parseEnumForRead<T extends string>(params: {
  value: string | null | undefined;
  allowed: readonly T[];
  fallback: T;
  label: string;
}): T {
  const raw = typeof params.value === "string" ? params.value.trim() : "";
  if (params.allowed.includes(raw as T)) {
    return raw as T;
  }
  warnLegacyParse(
    `durable memory read normalized invalid ${params.label} "${String(params.value)}" -> "${params.fallback}"`,
  );
  return params.fallback;
}

function parseMemoryTypeInput(value: string): DurableMemoryType {
  return parseEnumInput(value, DURABLE_MEMORY_TYPES, "memory type");
}

function parseMemoryTypeForRead(value: string | null | undefined): DurableMemoryType {
  return parseEnumForRead({
    value,
    allowed: DURABLE_MEMORY_TYPES,
    fallback: "Lesson",
    label: "memory type",
  });
}

function parseTimeHorizonInput(value: string): MemoryTimeHorizon {
  return parseEnumInput(value, TIME_HORIZONS, "time horizon");
}

function parseTimeHorizonForRead(value: string | null | undefined): MemoryTimeHorizon {
  return parseEnumForRead({
    value,
    allowed: TIME_HORIZONS,
    fallback: DEFAULT_TIME_HORIZON,
    label: "time horizon",
  });
}

function parseMemoryStatusForRead(value: string | null | undefined): MemoryStatus {
  return parseEnumForRead({
    value,
    allowed: MEMORY_STATUSES,
    fallback: "active",
    label: "memory status",
  });
}

function parseEvidenceTypeInput(value: string): MemoryEvidenceType {
  return parseEnumInput(value, EVIDENCE_TYPES, "evidence type");
}

function parseEvidenceTypeForRead(value: string | null | undefined): MemoryEvidenceType {
  return parseEnumForRead({
    value,
    allowed: EVIDENCE_TYPES,
    fallback: "quote",
    label: "evidence type",
  });
}

function parseLinkTypeInput(value: string): MemoryLinkType {
  return parseEnumInput(value, LINK_TYPES, "link type");
}

function parseLinkTypeForRead(value: string | null | undefined): MemoryLinkType {
  return parseEnumForRead({
    value,
    allowed: LINK_TYPES,
    fallback: "note",
    label: "link type",
  });
}

function parseUnitIntervalInput(
  value: number | undefined,
  label: string,
  fallback: number,
): number {
  if (value == null) {
    return fallback;
  }
  if (!Number.isFinite(value)) {
    throw new Error(`Invalid ${label}: ${value}. Expected a finite number in [0, 1].`);
  }
  if (value < 0 || value > 1) {
    throw new Error(`Invalid ${label}: ${value}. Expected a number in [0, 1].`);
  }
  return value;
}

function normalizeUnitIntervalForRead(value: number, fallback: number, label: string): number {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    warnLegacyParse(
      `durable memory read normalized invalid ${label} "${String(value)}" -> "${String(fallback)}"`,
    );
    return fallback;
  }
  if (numeric < 0 || numeric > 1) {
    warnLegacyParse(
      `durable memory read clamped out-of-range ${label} "${String(value)}" into [0, 1]`,
    );
  }
  return clamp01(numeric);
}

function parseEventIdsInput(values?: number[]): number[] {
  if (!values) {
    return [];
  }
  const normalized: number[] = [];
  for (const value of values) {
    if (!Number.isInteger(value) || value <= 0) {
      throw new Error(`Invalid event id: ${value}. Expected positive integers.`);
    }
    normalized.push(value);
  }
  return [...new Set(normalized)];
}

function tokenize(text: string): Set<string> {
  const matches = text.toLowerCase().match(/[a-z0-9_]+/g) ?? [];
  return new Set(matches);
}

function keywordCoverage(queryTokens: Set<string>, text: string): number {
  if (queryTokens.size === 0) {
    return 0;
  }
  const haystack = text.toLowerCase();
  let hits = 0;
  for (const token of queryTokens) {
    if (haystack.includes(token)) {
      hits += 1;
    }
  }
  return hits / queryTokens.size;
}

function typePriorityBonus(type: DurableMemoryType, queryTokens: Set<string>): number {
  const hints = TYPE_INTENT_TOKENS[type];
  if (!hints || hints.length === 0 || queryTokens.size === 0) {
    return 0;
  }
  let matches = 0;
  for (const token of queryTokens) {
    if (hints.includes(token)) {
      matches += 1;
    }
  }
  if (matches === 0) {
    return 0;
  }
  return clamp01(0.2 + 0.25 * (matches / Math.max(1, hints.length)));
}

function jaccardSimilarity(left: Set<string>, right: Set<string>): number {
  if (left.size === 0 && right.size === 0) {
    return 1;
  }
  if (left.size === 0 || right.size === 0) {
    return 0;
  }

  let intersection = 0;
  const smaller = left.size <= right.size ? left : right;
  const larger = left.size <= right.size ? right : left;

  for (const token of smaller) {
    if (larger.has(token)) {
      intersection += 1;
    }
  }

  const union = left.size + right.size - intersection;
  if (union <= 0) {
    return 0;
  }
  return intersection / union;
}

function pairKey(left: number, right: number): string {
  return left < right ? `${left}:${right}` : `${right}:${left}`;
}

function pickCanonicalMemory(entries: DurableMemoryItem[], nowMs: number): DurableMemoryItem {
  return entries
    .map((entry) => {
      const recencyDays = daysSince(entry.updatedAt, nowMs);
      const recencyScore = Math.exp(-Math.max(0, recencyDays) / 120);
      const quality = 0.5 * entry.importance + 0.3 * entry.confidence + 0.2 * recencyScore;
      return { entry, quality };
    })
    .toSorted((a, b) => b.quality - a.quality)[0].entry;
}

function mergeBody(existing: string, incoming: string): string {
  if (!incoming.trim()) {
    return existing;
  }
  const normalizedExisting = normalizeComparable(existing);
  const normalizedIncoming = normalizeComparable(incoming);
  if (normalizedExisting === normalizedIncoming) {
    return existing;
  }
  if (normalizedExisting.includes(normalizedIncoming)) {
    return existing;
  }
  if (normalizedIncoming.includes(normalizedExisting)) {
    return incoming;
  }
  return `${existing.trim()}\n\n${incoming.trim()}`.trim();
}

function mergeBodies(bodies: string[]): string {
  const unique = new Map<string, string>();
  for (const body of bodies) {
    const normalized = normalizeComparable(body);
    if (!normalized) {
      continue;
    }
    if (!unique.has(normalized)) {
      unique.set(normalized, body.trim());
    }
  }

  const values = [...unique.values()];
  if (values.length === 0) {
    return "";
  }

  const sorted = values.toSorted((a, b) => b.length - a.length);
  const chunks: string[] = [];
  let total = 0;
  for (const chunk of sorted) {
    if (total >= 2400) {
      break;
    }
    chunks.push(chunk);
    total += chunk.length;
  }

  return chunks.join("\n\n").trim();
}

function normalizeComparable(text: string): string {
  return text.toLowerCase().replaceAll(/\s+/g, " ").trim();
}

function maxTimeHorizon(left: MemoryTimeHorizon, right: MemoryTimeHorizon): MemoryTimeHorizon {
  return TIME_HORIZON_RANK[right] > TIME_HORIZON_RANK[left] ? right : left;
}

function selectEarliestIso(values: Array<string | null>): string | null {
  const valid = values
    .map((entry) => normalizeIsoDate(entry))
    .filter((entry): entry is string => typeof entry === "string");
  if (valid.length === 0) {
    return null;
  }
  return valid.toSorted((a, b) => compareIso(a, b))[0];
}

function selectLatestIso(values: Array<string | null>): string | null {
  const valid = values
    .map((entry) => normalizeIsoDate(entry))
    .filter((entry): entry is string => typeof entry === "string");
  if (valid.length === 0) {
    return null;
  }
  return valid.toSorted((a, b) => compareIso(b, a))[0];
}

function selectMergedValidUntil(values: Array<string | null>): string | null {
  const normalized = values
    .map((entry) => normalizeIsoDate(entry))
    .filter((entry): entry is string => typeof entry === "string");
  if (normalized.length === 0) {
    return null;
  }

  const nowMs = Date.now();
  const future = normalized.filter((entry) => {
    const date = Date.parse(entry);
    return Number.isFinite(date) && date >= nowMs;
  });

  if (future.length > 0) {
    return future.toSorted((a, b) => compareIso(a, b))[0];
  }

  return normalized.toSorted((a, b) => compareIso(b, a))[0];
}

function normalizeTags(tags?: string[]): string[] {
  if (!tags) {
    return [];
  }
  return [...new Set(tags.map((entry) => entry.trim()).filter(Boolean))];
}

function parseJsonTags(value: string | null | undefined): string[] {
  if (!value) {
    return [];
  }
  try {
    const parsed = JSON.parse(value) as unknown;
    if (!Array.isArray(parsed)) {
      return [];
    }
    return normalizeTags(
      parsed.filter(
        (entry): entry is string => typeof entry === "string" && entry.trim().length > 0,
      ),
    );
  } catch {
    return [];
  }
}

function toJsonStringArray(values?: string[]): string {
  return JSON.stringify(normalizeTags(values));
}

function normalizeIsoDate(value: string | null | undefined): string | null {
  const trimmed = trimOrNull(value);
  if (!trimmed) {
    return null;
  }
  const date = new Date(trimmed);
  if (Number.isNaN(date.getTime())) {
    return null;
  }
  return date.toISOString();
}

function trimOrNull(value?: string | null): string | null {
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function clamp01(value: number): number {
  if (!Number.isFinite(value)) {
    return 0;
  }
  return Math.max(0, Math.min(1, value));
}

function compareIso(left: string, right: string): number {
  const leftMs = Date.parse(left);
  const rightMs = Date.parse(right);
  if (!Number.isFinite(leftMs) && !Number.isFinite(rightMs)) {
    return 0;
  }
  if (!Number.isFinite(leftMs)) {
    return 1;
  }
  if (!Number.isFinite(rightMs)) {
    return -1;
  }
  return leftMs - rightMs;
}

function daysSince(iso: string | null, nowMs: number): number {
  if (!iso) {
    return Number.POSITIVE_INFINITY;
  }
  const value = Date.parse(iso);
  if (!Number.isFinite(value)) {
    return Number.POSITIVE_INFINITY;
  }
  return Math.max(0, nowMs - value) / DAY_MS;
}

function daysTo(iso: string | null, nowMs: number): number | null {
  if (!iso) {
    return null;
  }
  const value = Date.parse(iso);
  if (!Number.isFinite(value)) {
    return null;
  }
  return (value - nowMs) / DAY_MS;
}

function uniqueNumbers(values: number[]): number[] {
  return [...new Set(values.filter((entry) => Number.isInteger(entry) && entry > 0))];
}

function uniqueStrings(values: string[]): string[] {
  return [...new Set(values.map((entry) => entry.trim()).filter(Boolean))];
}

function rowIdToNumber(value: number | bigint | null | undefined): number {
  if (typeof value === "bigint") {
    return Number(value);
  }
  if (typeof value === "number") {
    return value;
  }
  return 0;
}

class DisjointSet {
  private readonly parent = new Map<number, number>();

  constructor(values: number[]) {
    for (const value of values) {
      this.parent.set(value, value);
    }
  }

  find(value: number): number {
    const parent = this.parent.get(value);
    if (parent == null) {
      this.parent.set(value, value);
      return value;
    }
    if (parent === value) {
      return value;
    }
    const root = this.find(parent);
    this.parent.set(value, root);
    return root;
  }

  union(left: number, right: number): void {
    const leftRoot = this.find(left);
    const rightRoot = this.find(right);
    if (leftRoot === rightRoot) {
      return;
    }
    this.parent.set(rightRoot, leftRoot);
  }
}
