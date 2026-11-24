import { join } from "node:path";
import type { CliContext } from "./context.ts";
import { fetchMessage, fetchThread, toCompactMessage } from "../slack/messages.ts";
import type { SlackMessageSummary } from "../slack/messages.ts";
import { enrichFiles } from "../slack/message-api-parsing.ts";
import {
  baselineSnapshot,
  compareSlackTs,
  emptySnapshot,
  type ThreadSnapshot,
} from "../slack/thread-diff.ts";
import { pruneEmpty } from "../lib/compact-json.ts";
import { getAppDir } from "../lib/app-dir.ts";
import { readJsonFile, writeJsonFile } from "../lib/fs.ts";
import { parseMsgTarget } from "./targets.ts";
import { resolveChannelId } from "../slack/channels.ts";
import { downloadMessageFiles } from "./message-file-downloads.ts";
import { warnOnTruncatedSlackUrl } from "./message-url-warning.ts";
import { toThreadListMessage } from "./message-thread-info.ts";
import {
  parseDurationMs,
  runWatch,
  type WatchBatch,
  type WatchConfig,
  type WatchedThread,
  type WatchResult,
} from "./message-watch.ts";

export type MessageWatchOptions = {
  workspace?: string;
  threadTs?: string;
  ts?: string;
  since?: string;
  interval: string;
  maxInterval: string;
  settle: string;
  once?: boolean;
  timeout?: string;
  includeSelf?: boolean;
  edits: boolean;
  state: boolean;
  fresh?: boolean;
  maxBodyChars: string;
};

const MIN_INTERVAL_MS = 5000;
const MAX_SETTLE_MS = 2 * 60_000;
const FULL_READ_EVERY = 10;
const STATE_VERSION = 1;
const MAX_BEFORE_CHARS = 500;

type ThreadTarget = WatchedThread & { workspaceUrl: string; resumed: boolean };

function stateFilePath(input: {
  workspaceUrl: string;
  channelId: string;
  threadTs: string;
}): string {
  const { workspaceUrl, channelId, threadTs } = input;
  const host = workspaceUrl.replace(/^https?:\/\//, "").replace(/[^A-Za-z0-9.-]/g, "_");
  return join(getAppDir(), "watch", `${host}_${channelId}_${threadTs}.json`);
}

export function parseWatchConfig(options: MessageWatchOptions): WatchConfig {
  const baseIntervalMs = parseDurationMs(options.interval, "--interval");
  if (baseIntervalMs < MIN_INTERVAL_MS) {
    throw new Error("--interval must be at least 5s");
  }
  const maxIntervalMs = Math.max(
    parseDurationMs(options.maxInterval, "--max-interval"),
    baseIntervalMs,
  );
  const settleMs = parseDurationMs(options.settle, "--settle");
  return {
    trackEdits: options.edits,
    baseIntervalMs,
    maxIntervalMs,
    settleMs,
    maxSettleMs: Math.max(MAX_SETTLE_MS, settleMs),
    fullReadEvery: FULL_READ_EVERY,
    once: Boolean(options.once),
    timeoutMs: options.timeout ? parseDurationMs(options.timeout, "--timeout") : undefined,
  };
}

async function resolveTarget(input: {
  ctx: CliContext;
  targetInput: string;
  options: MessageWatchOptions;
  single: boolean;
}): Promise<{ workspaceUrl: string; channelId: string; rootTs: string }> {
  const { ctx, options } = input;
  const target = parseMsgTarget(input.targetInput);
  if (target.kind === "user") {
    throw new Error("message watch needs a message URL, or a channel with --thread-ts/--ts.");
  }
  const flagWorkspace = ctx.effectiveWorkspaceUrl(options.workspace);
  const workspaceHint = target.kind === "url" ? target.ref.workspace_url : flagWorkspace;
  return ctx.withAutoRefresh({
    workspaceUrl: workspaceHint,
    work: async () => {
      const { client, workspace_url } = await ctx.getClientForWorkspace(workspaceHint);
      const workspaceUrl = workspace_url ?? workspaceHint ?? "";
      if (target.kind === "url") {
        warnOnTruncatedSlackUrl(target.ref);
        const msg = await fetchMessage(client, { ref: target.ref });
        return { workspaceUrl, channelId: target.ref.channel_id, rootTs: msg.thread_ts ?? msg.ts };
      }
      if (!input.single) {
        throw new Error("Channel targets need --thread-ts/--ts, so watch one at a time.");
      }
      await ctx.assertWorkspaceSpecifiedForChannelNames({
        workspaceUrl: flagWorkspace,
        channels: [target.channel],
      });
      const channelId = await resolveChannelId(client, target.channel);
      const threadTs = options.threadTs?.trim();
      if (threadTs) {
        return { workspaceUrl, channelId, rootTs: threadTs };
      }
      const ts = options.ts?.trim();
      if (!ts) {
        throw new Error("When targeting a channel, pass --thread-ts or --ts.");
      }
      const msg = await fetchMessage(client, {
        ref: { workspace_url: workspaceUrl, channel_id: channelId, message_ts: ts, raw: ts },
      });
      return { workspaceUrl, channelId, rootTs: msg.thread_ts ?? msg.ts };
    },
  });
}

function pollThread(ctx: CliContext, input: { t: ThreadTarget; oldest?: string }) {
  const { t, oldest } = input;
  return ctx.withAutoRefresh({
    workspaceUrl: t.workspaceUrl,
    work: async () => {
      const { client } = await ctx.getClientForWorkspace(t.workspaceUrl);
      return fetchThread(client, {
        channelId: t.snapshot.channel_id,
        threadTs: t.snapshot.thread_ts,
        oldest,
        enrichFiles: false,
      });
    },
  });
}

async function setupThreads(input: {
  ctx: CliContext;
  targets: string[];
  options: MessageWatchOptions;
}): Promise<ThreadTarget[]> {
  const { ctx, options } = input;
  const selfIds = new Map<string, string | undefined>();
  const out: ThreadTarget[] = [];
  for (const targetInput of input.targets) {
    const { workspaceUrl, channelId, rootTs } = await resolveTarget({
      ctx,
      targetInput,
      options,
      single: input.targets.length === 1,
    });
    const key = stateFilePath({ workspaceUrl, channelId, threadTs: rootTs });
    if (out.some((t) => t.key === key)) {
      continue;
    }
    const since = options.since?.trim();
    // --since means "report from here", so it starts fresh rather than resuming.
    const saved =
      options.state && !options.fresh && !since
        ? await readJsonFile<{ version: number; snapshot: ThreadSnapshot }>(key)
        : null;
    let snapshot: ThreadSnapshot;
    const resumed = saved?.version === STATE_VERSION;
    if (resumed) {
      ({ snapshot } = saved);
    } else {
      snapshot = emptySnapshot({ channelId, threadTs: rootTs, cursor: since });
      const t: ThreadTarget = { key, snapshot, polls: 0, workspaceUrl, resumed };
      const existing = await pollThread(ctx, { t });
      // With --since, replies after it stay unknown so the first poll reports them.
      snapshot = baselineSnapshot(
        snapshot,
        since ? existing.filter((m) => compareSlackTs(m.ts, since) <= 0) : existing,
      );
      if (since) {
        snapshot.cursor = since;
      }
    }
    if (!options.includeSelf && !selfIds.has(workspaceUrl)) {
      const who = await ctx.withAutoRefresh({
        workspaceUrl,
        work: async () =>
          (await ctx.getClientForWorkspace(workspaceUrl)).client.api("auth.test", {}),
      });
      selfIds.set(workspaceUrl, typeof who.user_id === "string" ? who.user_id : undefined);
    }
    out.push({
      key,
      snapshot,
      polls: 0,
      workspaceUrl,
      resumed,
      skipUserId: options.includeSelf ? undefined : selfIds.get(workspaceUrl),
    });
  }
  return out;
}

async function renderBatch(input: {
  ctx: CliContext;
  batch: WatchBatch;
  maxBodyChars: number;
}): Promise<Record<string, unknown>> {
  const threads = [];
  for (const { thread, changes } of input.batch) {
    const t = thread as ThreadTarget;
    const { client, auth } = await input.ctx.getClientForWorkspace(t.workspaceUrl);
    const enrich = async (m: SlackMessageSummary) =>
      m.files?.length ? { ...m, files: await enrichFiles(client, m.files) } : m;
    const added = await Promise.all(changes.added.map(enrich));
    const edited = await Promise.all(
      changes.edited.map(async (e) => ({ msg: await enrich(e.msg), before: e.before })),
    );
    const downloadedPaths = await downloadMessageFiles({
      auth,
      messages: [...added, ...edited.map((e) => e.msg)],
    });
    const compact = (m: SlackMessageSummary) =>
      toThreadListMessage(
        toCompactMessage(m, { maxBodyChars: input.maxBodyChars, downloadedPaths }),
      );
    const clip = (s: string) =>
      s.length > MAX_BEFORE_CHARS ? `${s.slice(0, MAX_BEFORE_CHARS)}...` : s;
    threads.push({
      channel_id: t.snapshot.channel_id,
      thread_ts: t.snapshot.thread_ts,
      new: added.map(compact),
      edited: edited.map((e) => ({ ...compact(e.msg), before: clip(e.before) })),
      deleted: changes.deleted.map((d) => ({ ts: d.ts, user: d.user, before: clip(d.before) })),
    });
  }
  return pruneEmpty({ event: "update", threads }) as Record<string, unknown>;
}

export async function handleMessageWatch(input: {
  ctx: CliContext;
  targets: string[];
  options: MessageWatchOptions;
  writeLine: (line: string) => void;
  writeError: (line: string) => void;
}): Promise<WatchResult> {
  const { ctx, options } = input;
  const cfg = parseWatchConfig(options);
  const maxBodyChars = Number.parseInt(options.maxBodyChars, 10);
  if ((options.threadTs || options.ts) && input.targets.length > 1) {
    throw new Error("--thread-ts/--ts apply to a single channel target.");
  }

  const threads = await setupThreads({ ctx, targets: input.targets, options });
  if (options.state) {
    // Save a fresh baseline now, so a watch killed before its first change still resumes from here.
    for (const t of threads.filter((x) => !x.resumed)) {
      await writeJsonFile(t.key, { version: STATE_VERSION, snapshot: t.snapshot });
    }
  }
  input.writeLine(
    JSON.stringify({
      event: "watching",
      threads: threads.map((t) => ({
        channel_id: t.snapshot.channel_id,
        thread_ts: t.snapshot.thread_ts,
        cursor: t.snapshot.cursor,
        resumed: t.resumed,
      })),
    }),
  );

  const result = await runWatch({
    threads,
    cfg,
    poll: (t, opts) => pollThread(ctx, { t: t as ThreadTarget, oldest: opts.oldest }),
    flush: async (batch) => {
      input.writeLine(JSON.stringify(await renderBatch({ ctx, batch, maxBodyChars })));
    },
    persist: async (t) => {
      if (options.state) {
        await writeJsonFile(t.key, { version: STATE_VERSION, snapshot: t.snapshot });
      }
    },
    onError: (t, err) =>
      input.writeError(
        `poll failed for ${t.snapshot.channel_id}/${t.snapshot.thread_ts}, retrying: ${ctx.errorMessage(err)}`,
      ),
  });

  input.writeLine(JSON.stringify({ event: "done", ...result }));
  return result;
}
