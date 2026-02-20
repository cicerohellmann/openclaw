import { Command } from "commander";
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";

const loadConfig = vi.fn(() => ({}));
const resolveDefaultAgentId = vi.fn(() => "main");
const resolveAgentWorkspaceDir = vi.fn(() => "/tmp/workspace");
const createService = vi.fn();

vi.mock("../config/config.js", () => ({
  loadConfig,
}));

vi.mock("../agents/agent-scope.js", () => ({
  resolveDefaultAgentId,
  resolveAgentWorkspaceDir,
}));

vi.mock("../memory/durable/index.js", () => ({
  DURABLE_MEMORY_TYPES: [
    "Preference",
    "Identity",
    "Project",
    "Protocol",
    "Lesson",
    "UnpromptedKeep",
  ],
  LINK_TYPES: ["file", "url", "repo", "note", "chat"],
  TIME_HORIZONS: ["immediate", "short", "medium", "long", "evergreen"],
  DurableMemoryService: {
    create: createService,
  },
}));

let registerDurableMemoryCli: typeof import("./memory-durable-cli.js").registerDurableMemoryCli;
let defaultRuntime: typeof import("../runtime.js").defaultRuntime;

beforeAll(async () => {
  ({ registerDurableMemoryCli } = await import("./memory-durable-cli.js"));
  ({ defaultRuntime } = await import("../runtime.js"));
});

afterEach(() => {
  vi.restoreAllMocks();
  process.exitCode = undefined;
  createService.mockReset();
});

describe("memory durable cli", () => {
  async function run(args: string[]) {
    const program = new Command();
    const memory = program.command("memory");
    registerDurableMemoryCli(memory);
    await program.parseAsync(["memory", "durable", ...args], { from: "user" });
  }

  it("runs init and prints json payload", async () => {
    const close = vi.fn();
    createService.mockResolvedValue({
      status: () => ({
        dbPath: "/tmp/workspace/memory/durable_memory.db",
        events: 0,
        memories: 0,
      }),
      close,
    });

    const log = vi.spyOn(defaultRuntime, "log").mockImplementation(() => {});
    await run(["init", "--json"]);

    expect(loadConfig).toHaveBeenCalled();
    expect(resolveDefaultAgentId).toHaveBeenCalled();
    expect(resolveAgentWorkspaceDir).toHaveBeenCalledWith({}, "main");
    expect(createService).toHaveBeenCalledWith({ workspaceDir: "/tmp/workspace" });
    expect(log).toHaveBeenCalledWith(
      expect.stringContaining('"dbPath": "/tmp/workspace/memory/durable_memory.db"'),
    );
    expect(close).toHaveBeenCalled();
  });

  it("runs clear-context and keeps process healthy", async () => {
    const close = vi.fn();
    createService.mockResolvedValue({
      clearContext: () => ({
        scope: "durable.context_window_entries",
        clearedEntries: 2,
        memoryCountBefore: 1,
        memoryCountAfter: 1,
        auditEventId: 44,
      }),
      close,
    });

    const log = vi.spyOn(defaultRuntime, "log").mockImplementation(() => {});
    await run(["clear-context", "--json"]);

    expect(log).toHaveBeenCalledWith(expect.stringContaining('"memoryCountAfter": 1'));
    expect(process.exitCode).toBeUndefined();
    expect(close).toHaveBeenCalled();
  });

  it("prints scope-aware clear-context text output", async () => {
    const close = vi.fn();
    createService.mockResolvedValue({
      clearContext: () => ({
        scope: "durable.context_window_entries",
        clearedEntries: 2,
        memoryCountBefore: 1,
        memoryCountAfter: 1,
        auditEventId: 44,
      }),
      close,
    });

    const log = vi.spyOn(defaultRuntime, "log").mockImplementation(() => {});
    await run(["clear-context"]);

    expect(log).toHaveBeenCalledWith(expect.stringContaining("module-local only"));
    expect(log).toHaveBeenCalledWith(
      expect.stringContaining("Scope: durable.context_window_entries"),
    );
    expect(process.exitCode).toBeUndefined();
    expect(close).toHaveBeenCalled();
  });

  it("surfaces command failures", async () => {
    createService.mockRejectedValue(new Error("db unavailable"));

    const error = vi.spyOn(defaultRuntime, "error").mockImplementation(() => {});
    await run(["status"]);

    expect(error).toHaveBeenCalledWith(expect.stringContaining("Durable memory command failed"));
    expect(process.exitCode).toBe(1);
  });
});
