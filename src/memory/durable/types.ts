export const DURABLE_MEMORY_TYPES = [
  "Preference",
  "Identity",
  "Project",
  "Protocol",
  "Lesson",
  "UnpromptedKeep",
] as const;

export type DurableMemoryType = (typeof DURABLE_MEMORY_TYPES)[number];

export const TIME_HORIZONS = ["immediate", "short", "medium", "long", "evergreen"] as const;

export type MemoryTimeHorizon = (typeof TIME_HORIZONS)[number];

export const MEMORY_STATUSES = ["active", "merged", "archived"] as const;

export type MemoryStatus = (typeof MEMORY_STATUSES)[number];

export const EVIDENCE_TYPES = ["quote", "behavior", "decision", "artifact"] as const;

export type MemoryEvidenceType = (typeof EVIDENCE_TYPES)[number];

export const LINK_TYPES = ["file", "url", "repo", "note", "chat"] as const;

export type MemoryLinkType = (typeof LINK_TYPES)[number];

export const EVENT_ROLES = ["user", "assistant", "system"] as const;

export type MemoryEventRole = (typeof EVENT_ROLES)[number];

export type DurableEvent = {
  id: number;
  role: MemoryEventRole;
  text: string;
  summary: string | null;
  tags: string[];
  createdAt: string;
  sourceType: string | null;
  sourceRef: string | null;
};

export type DurableMemoryItem = {
  id: number;
  type: DurableMemoryType;
  title: string;
  body: string;
  tags: string[];
  confidence: number;
  importance: number;
  createdAt: string;
  updatedAt: string;
  lastSeenAt: string | null;
  lastConfirmedAt: string | null;
  timeHorizon: MemoryTimeHorizon;
  validUntil: string | null;
  nextReviewAt: string | null;
  status: MemoryStatus;
  canonicalId: number | null;
};

export type DurableMemoryEvidence = {
  id: number;
  memoryId: number;
  eventId: number | null;
  evidenceType: MemoryEvidenceType;
  note: string | null;
  createdAt: string;
};

export type DurableMemoryLink = {
  id: number;
  memoryId: number;
  linkType: MemoryLinkType;
  linkTarget: string;
  label: string | null;
  createdAt: string;
};

export type DurableMemoryStatus = {
  dbPath: string;
  events: number;
  memories: number;
  activeMemories: number;
  mergedMemories: number;
  archivedMemories: number;
  evidenceLinks: number;
  artifactLinks: number;
  contextEntries: number;
  lastResetAt: string | null;
};

export type TemporalSignals = {
  stalenessScore: number;
  momentumScore: number;
  recencyScore: number;
  urgencyScore: number;
};

export type DurableProvenance = {
  sourcePaths: string[];
  eventIds: number[];
  evidenceTimestamps: string[];
  linkTargets: string[];
};

export type DurableSearchResult = {
  memory: DurableMemoryItem;
  score: number;
  rationale: string;
  provenance: DurableProvenance;
  stalenessScore: number;
  momentumScore: number;
};

export type AddEventInput = {
  role: MemoryEventRole;
  text: string;
  summary?: string;
  tags?: string[];
  sourceType?: string;
  sourceRef?: string;
};

export type AddMemoryInput = {
  type: DurableMemoryType;
  title: string;
  body: string;
  tags?: string[];
  confidence?: number;
  importance?: number;
  timeHorizon?: MemoryTimeHorizon;
  validUntil?: string | null;
  nextReviewAt?: string | null;
  eventIds?: number[];
};

export type AddLinkInput = {
  memoryId: number;
  linkType: MemoryLinkType;
  linkTarget: string;
  label?: string;
};

export type SearchMemoryInput = {
  query: string;
  topK?: number;
  mmr?: boolean;
  nowMs?: number;
};

export type ConsolidateResult = {
  mergedGroups: number;
  mergedItems: number;
  canonicalIds: number[];
};

export type ReviewCandidate = {
  memory: DurableMemoryItem;
  reason: string;
  stalenessScore: number;
  momentumScore: number;
};
