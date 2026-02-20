import type { Command } from "commander";
import { resolveAgentWorkspaceDir } from "../agents/agent-scope.js";
import { resolveDefaultAgentId } from "../agents/agent-scope.js";
import type { OpenClawConfig } from "../config/config.js";
import { loadConfig } from "../config/config.js";
import {
  DURABLE_MEMORY_TYPES,
  LINK_TYPES,
  TIME_HORIZONS,
  DurableMemoryService,
  type AddMemoryInput,
  type DurableEvent,
  type DurableMemoryDetails,
  type DurableMemoryItem,
  type DurableMemoryLink,
  type DurableSearchResult,
  type ReviewCandidate,
} from "../memory/durable/index.js";
import { defaultRuntime } from "../runtime.js";

type DurableBaseOptions = {
  agent?: string;
  json?: boolean;
};

type DurableAddEventOptions = DurableBaseOptions & {
  role: "user" | "assistant" | "system";
  text: string;
  summary?: string;
  tags?: string;
  sourceType?: string;
  sourceRef?: string;
};

type DurableAddMemoryOptions = DurableBaseOptions & {
  type: AddMemoryInput["type"];
  title: string;
  body: string;
  tags?: string;
  confidence?: number;
  importance?: number;
  timeHorizon?: AddMemoryInput["timeHorizon"];
  validUntil?: string;
  nextReviewAt?: string;
  eventIds?: string;
};

type DurableAddLinkOptions = DurableBaseOptions & {
  memoryId: number;
  linkType: "file" | "url" | "repo" | "note" | "chat";
  target: string;
  label?: string;
};

type DurableGetMemoryOptions = DurableBaseOptions & {
  memoryId: number;
  withLinks?: boolean;
};

type DurableSearchOptions = DurableBaseOptions & {
  query: string;
  topK?: number;
  mmr?: boolean;
};

type DurableConsolidateOptions = DurableBaseOptions & {
  similarityThreshold?: number;
  minGroupSize?: number;
};

type DurableReviewOptions = DurableBaseOptions & {
  limit?: number;
};

type DurableMarkConfirmedOptions = DurableBaseOptions & {
  memoryId: number;
};

export function registerDurableMemoryCli(memory: Command): void {
  const durable = memory
    .command("durable")
    .description("Durable identity memory (events, canonical memories, provenance, consolidation)");

  durable
    .command("init")
    .description("Initialize durable memory database")
    .option("--agent <id>", "Agent id (default: default agent)")
    .option("--json", "Print JSON")
    .action(async (opts: DurableBaseOptions) => {
      await runWithDurableService(opts, async ({ service, workspaceDir }, asJson) => {
        const status = service.status();
        const payload = {
          ok: true,
          dbPath: status.dbPath,
          workspaceDir,
          events: status.events,
          memories: status.memories,
        };
        printPayload(payload, asJson, formatInitPayload);
      });
    });

  durable
    .command("status")
    .description("Show durable memory status")
    .option("--agent <id>", "Agent id (default: default agent)")
    .option("--json", "Print JSON")
    .action(async (opts: DurableBaseOptions) => {
      await runWithDurableService(opts, async ({ service }, asJson) => {
        const payload = service.status();
        if (asJson) {
          printPayload(payload, true);
          return;
        }
        defaultRuntime.log(
          [
            "Durable Memory",
            `DB: ${payload.dbPath}`,
            `Events: ${payload.events}`,
            `Memories: ${payload.memories} (active: ${payload.activeMemories}, merged: ${payload.mergedMemories}, archived: ${payload.archivedMemories})`,
            `Evidence links: ${payload.evidenceLinks}`,
            `Artifact links: ${payload.artifactLinks}`,
            `Context entries: ${payload.contextEntries}`,
            `Last reset: ${payload.lastResetAt ?? "never"}`,
          ].join("\n"),
        );
      });
    });

  durable
    .command("add-event")
    .description("Add an event to the raw timeline")
    .requiredOption("--role <role>", "user|assistant|system")
    .requiredOption("--text <text>", "Event text")
    .option("--summary <summary>", "Summary")
    .option("--tags <csv>", "Comma-separated tags")
    .option("--source-type <type>", "Event source type")
    .option("--source-ref <ref>", "Event source reference")
    .option("--agent <id>", "Agent id (default: default agent)")
    .option("--json", "Print JSON")
    .action(async (opts: DurableAddEventOptions) => {
      await runWithDurableService(opts, async ({ service }, asJson) => {
        const role = parseRole(opts.role);
        const event = service.addEvent({
          role,
          text: opts.text,
          summary: opts.summary,
          tags: parseCsv(opts.tags),
          sourceType: opts.sourceType,
          sourceRef: opts.sourceRef,
        });
        printPayload({ event }, asJson, ({ event: createdEvent }) =>
          formatEventPayload(createdEvent),
        );
      });
    });

  durable
    .command("add-memory")
    .description("Add or reconfirm a canonical durable memory")
    .requiredOption("--type <type>", `Type (${DURABLE_MEMORY_TYPES.join("|")})`)
    .requiredOption("--title <title>", "Memory title")
    .requiredOption("--body <body>", "Memory body")
    .option("--tags <csv>", "Comma-separated tags")
    .option("--confidence <n>", "Confidence [0..1]", parseFiniteNumber)
    .option("--importance <n>", "Importance [0..1]", parseFiniteNumber)
    .option("--time-horizon <horizon>", `Time horizon (${TIME_HORIZONS.join("|")})`)
    .option("--valid-until <iso>", "ISO-8601 timestamp")
    .option("--next-review-at <iso>", "ISO-8601 timestamp")
    .option("--event-ids <csv>", "Comma-separated event ids")
    .option("--agent <id>", "Agent id (default: default agent)")
    .option("--json", "Print JSON")
    .action(async (opts: DurableAddMemoryOptions) => {
      await runWithDurableService(opts, async ({ service }, asJson) => {
        const type = ensureEnum(opts.type, DURABLE_MEMORY_TYPES, "memory type");
        const timeHorizon = opts.timeHorizon
          ? ensureEnum(opts.timeHorizon, TIME_HORIZONS, "time horizon")
          : undefined;
        const memory = service.addMemory({
          type,
          title: opts.title,
          body: opts.body,
          tags: parseCsv(opts.tags),
          confidence: opts.confidence,
          importance: opts.importance,
          timeHorizon,
          validUntil: opts.validUntil,
          nextReviewAt: opts.nextReviewAt,
          eventIds: parseNumericCsv(opts.eventIds),
        });
        printPayload({ memory }, asJson, ({ memory: storedMemory }) =>
          formatMemoryPayload(storedMemory),
        );
      });
    });

  durable
    .command("add-link")
    .description("Attach artifact provenance to a memory")
    .requiredOption("--memory-id <id>", "Memory id", parseInteger)
    .requiredOption("--link-type <type>", `Link type (${LINK_TYPES.join("|")})`)
    .requiredOption("--target <target>", "Link target")
    .option("--label <label>", "Optional label")
    .option("--agent <id>", "Agent id (default: default agent)")
    .option("--json", "Print JSON")
    .action(async (opts: DurableAddLinkOptions) => {
      await runWithDurableService(opts, async ({ service }, asJson) => {
        const linkType = ensureEnum(opts.linkType, LINK_TYPES, "link type");
        const link = service.addLink({
          memoryId: opts.memoryId,
          linkType,
          linkTarget: opts.target,
          label: opts.label,
        });
        printPayload({ link }, asJson, ({ link: createdLink }) => formatLinkPayload(createdLink));
      });
    });

  durable
    .command("get-memory")
    .description("Get a canonical memory with optional links/evidence")
    .requiredOption("--memory-id <id>", "Memory id", parseInteger)
    .option("--with-links", "Include links and evidence")
    .option("--agent <id>", "Agent id (default: default agent)")
    .option("--json", "Print JSON")
    .action(async (opts: DurableGetMemoryOptions) => {
      await runWithDurableService(opts, async ({ service }, asJson) => {
        const result = service.getMemory({
          memoryId: opts.memoryId,
          withLinks: Boolean(opts.withLinks),
        });
        if (!result) {
          throw new Error(`Memory not found: ${opts.memoryId}`);
        }
        printPayload(result, asJson, formatMemoryDetailsPayload);
      });
    });

  durable
    .command("search-memory")
    .description("Search canonical durable memories with temporal-aware ranking")
    .requiredOption("--query <text>", "Search query")
    .option("--top-k <n>", "Max results", parseInteger)
    .option("--mmr", "Apply MMR diversification")
    .option("--agent <id>", "Agent id (default: default agent)")
    .option("--json", "Print JSON")
    .action(async (opts: DurableSearchOptions) => {
      await runWithDurableService(opts, async ({ service }, asJson) => {
        const results = service.searchMemory({
          query: opts.query,
          topK: opts.topK,
          mmr: Boolean(opts.mmr),
        });
        printPayload({ results }, asJson, ({ results: rows }) => formatSearchPayload(rows));
      });
    });

  durable
    .command("consolidate-memory")
    .description("Merge near-duplicate memories into canonical records")
    .option("--similarity-threshold <n>", "Similarity threshold", parseFiniteNumber)
    .option("--min-group-size <n>", "Minimum duplicate group size", parseInteger)
    .option("--agent <id>", "Agent id (default: default agent)")
    .option("--json", "Print JSON")
    .action(async (opts: DurableConsolidateOptions) => {
      await runWithDurableService(opts, async ({ service }, asJson) => {
        const summary = service.consolidateMemory({
          similarityThreshold: opts.similarityThreshold,
          minGroupSize: opts.minGroupSize,
        });
        printPayload(summary, asJson, formatConsolidatePayload);
      });
    });

  durable
    .command("review-due-memory")
    .description("List review candidates based on staleness, expiry, and identity/protocol rules")
    .option("--limit <n>", "Max review candidates", parseInteger)
    .option("--agent <id>", "Agent id (default: default agent)")
    .option("--json", "Print JSON")
    .action(async (opts: DurableReviewOptions) => {
      await runWithDurableService(opts, async ({ service }, asJson) => {
        const candidates = service.reviewDueMemory({ limit: opts.limit });
        printPayload({ candidates }, asJson, ({ candidates: rows }) => formatReviewPayload(rows));
      });
    });

  durable
    .command("mark-confirmed")
    .description("Mark a memory as reconfirmed")
    .requiredOption("--memory-id <id>", "Memory id", parseInteger)
    .option("--agent <id>", "Agent id (default: default agent)")
    .option("--json", "Print JSON")
    .action(async (opts: DurableMarkConfirmedOptions) => {
      await runWithDurableService(opts, async ({ service }, asJson) => {
        const memory = service.markConfirmed(opts.memoryId);
        if (!memory) {
          throw new Error(`Memory not found: ${opts.memoryId}`);
        }
        printPayload({ memory }, asJson, ({ memory: confirmedMemory }) =>
          formatMemoryPayload(confirmedMemory),
        );
      });
    });

  durable
    .command("clear-context")
    .description(
      "Clear durable context-window entries (module-local) while preserving canonical memories",
    )
    .option("--agent <id>", "Agent id (default: default agent)")
    .option("--json", "Print JSON")
    .action(async (opts: DurableBaseOptions) => {
      await runWithDurableService(opts, async ({ service }, asJson) => {
        const summary = service.clearContext();
        printPayload(summary, asJson, formatClearContextPayload);
      });
    });
}

async function runWithDurableService(
  opts: DurableBaseOptions,
  run: (
    context: {
      service: DurableMemoryService;
      agentId: string;
      workspaceDir: string;
    },
    asJson: boolean,
  ) => Promise<void>,
): Promise<void> {
  try {
    const cfg = loadConfig();
    const agentId = resolveAgentId(cfg, opts.agent);
    const workspaceDir = resolveAgentWorkspaceDir(cfg, agentId);
    const service = await DurableMemoryService.create({ workspaceDir });
    try {
      await run({ service, agentId, workspaceDir }, Boolean(opts.json));
    } finally {
      service.close();
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    defaultRuntime.error(`Durable memory command failed: ${message}`);
    process.exitCode = 1;
  }
}

function resolveAgentId(cfg: OpenClawConfig, override?: string): string {
  const trimmed = override?.trim();
  if (trimmed) {
    return trimmed;
  }
  return resolveDefaultAgentId(cfg);
}

function parseCsv(value?: string): string[] {
  if (!value) {
    return [];
  }
  return value
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
}

function parseNumericCsv(value?: string): number[] {
  if (!value) {
    return [];
  }
  const values = parseCsv(value).map((entry) => Number(entry));
  const invalid = values.find(
    (entry) => !Number.isFinite(entry) || !Number.isInteger(entry) || entry <= 0,
  );
  if (invalid != null) {
    throw new Error("Event ids must be positive integers");
  }
  return values;
}

function parseFiniteNumber(value: string): number {
  const number = Number(value);
  if (!Number.isFinite(number)) {
    throw new Error(`Invalid number: ${value}`);
  }
  return number;
}

function parseInteger(value: string): number {
  const number = Number(value);
  if (!Number.isInteger(number)) {
    throw new Error(`Invalid integer: ${value}`);
  }
  return number;
}

function ensureEnum<T extends string>(value: string, allowed: readonly T[], label: string): T {
  if (allowed.includes(value as T)) {
    return value as T;
  }
  throw new Error(`Invalid ${label}: ${value}. Allowed: ${allowed.join(", ")}`);
}

function parseRole(value: string): "user" | "assistant" | "system" {
  const normalized = value.trim();
  if (normalized === "user" || normalized === "assistant" || normalized === "system") {
    return normalized;
  }
  throw new Error(`Invalid role: ${value}. Allowed: user, assistant, system`);
}

function formatInitPayload(payload: {
  dbPath: string;
  workspaceDir: string;
  events: number;
  memories: number;
}): string {
  return [
    "Durable memory initialized.",
    `Workspace: ${payload.workspaceDir}`,
    `DB: ${payload.dbPath}`,
    `Events: ${payload.events}`,
    `Memories: ${payload.memories}`,
  ].join("\n");
}

function formatMemoryPayload(memory: DurableMemoryItem): string {
  return [
    `Memory #${memory.id} stored.`,
    `${memory.type}: ${memory.title}`,
    `Status: ${memory.status}`,
    `Confidence: ${memory.confidence.toFixed(2)} | Importance: ${memory.importance.toFixed(2)}`,
    `Time horizon: ${memory.timeHorizon}`,
    `Tags: ${memory.tags.length > 0 ? memory.tags.join(", ") : "none"}`,
  ].join("\n");
}

function formatEventPayload(event: DurableEvent): string {
  const source = [event.sourceType, event.sourceRef].filter(Boolean).join(":");
  return [
    `Event #${event.id} recorded (${event.role}).`,
    `Created: ${event.createdAt}`,
    `Tags: ${event.tags.length > 0 ? event.tags.join(", ") : "none"}`,
    `Source: ${source || "none"}`,
    `Text: ${event.text}`,
  ].join("\n");
}

function formatLinkPayload(link: DurableMemoryLink): string {
  return [
    `Link #${link.id} added to memory #${link.memoryId}.`,
    `Type: ${link.linkType}`,
    `Target: ${link.linkTarget}`,
    `Label: ${link.label ?? "none"}`,
  ].join("\n");
}

function formatMemoryDetailsPayload(details: DurableMemoryDetails): string {
  return [
    `Memory #${details.memory.id} (${details.memory.type})`,
    `Title: ${details.memory.title}`,
    `Status: ${details.memory.status}`,
    `Confidence: ${details.memory.confidence.toFixed(2)} | Importance: ${details.memory.importance.toFixed(2)}`,
    `Time horizon: ${details.memory.timeHorizon}`,
    `Evidence: ${details.evidence.length} | Links: ${details.links.length}`,
    `Body: ${details.memory.body}`,
  ].join("\n");
}

function formatSearchPayload(results: DurableSearchResult[]): string {
  if (results.length === 0) {
    return "No durable memories matched the query.";
  }
  const rows = results.map((entry, index) => {
    return `${index + 1}. #${entry.memory.id} [${entry.memory.type}] ${entry.memory.title} (score ${entry.score.toFixed(3)})\n   ${entry.rationale}`;
  });
  return [`Search results (${results.length}):`, ...rows].join("\n");
}

function formatConsolidatePayload(summary: {
  mergedGroups: number;
  mergedItems: number;
  canonicalIds: number[];
}): string {
  return [
    "Consolidation complete.",
    `Merged groups: ${summary.mergedGroups}`,
    `Merged items: ${summary.mergedItems}`,
    `Canonical ids: ${summary.canonicalIds.length > 0 ? summary.canonicalIds.join(", ") : "none"}`,
  ].join("\n");
}

function formatReviewPayload(candidates: ReviewCandidate[]): string {
  if (candidates.length === 0) {
    return "No review candidates are currently due.";
  }
  const rows = candidates.map((entry, index) => {
    return `${index + 1}. #${entry.memory.id} [${entry.memory.type}] ${entry.memory.title}\n   ${entry.reason}`;
  });
  return [`Review candidates (${candidates.length}):`, ...rows].join("\n");
}

function formatClearContextPayload(payload: {
  scope: string;
  clearedEntries: number;
  memoryCountBefore: number;
  memoryCountAfter: number;
  auditEventId: number;
}): string {
  return [
    "Cleared durable context-window entries (module-local only).",
    `Scope: ${payload.scope}`,
    `Cleared entries: ${payload.clearedEntries}`,
    `Canonical memories before/after: ${payload.memoryCountBefore}/${payload.memoryCountAfter}`,
    `Audit event id: ${payload.auditEventId}`,
  ].join("\n");
}

function printPayload<T>(payload: T, asJson: boolean, formatter?: (payload: T) => string): void {
  if (asJson) {
    defaultRuntime.log(JSON.stringify(payload, null, 2));
    return;
  }
  if (formatter) {
    defaultRuntime.log(formatter(payload));
    return;
  }
  defaultRuntime.log(JSON.stringify(payload, null, 2));
}
