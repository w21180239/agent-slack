import type { CliContext } from "./context.ts";
import { fetchMessage, fetchThread, toCompactMessage } from "../slack/messages.ts";
import type { SlackMessageSummary } from "../slack/messages.ts";
import { pruneEmpty } from "../lib/compact-json.ts";
import { parseMsgTarget } from "./targets.ts";
import { resolveChannelId } from "../slack/channels.ts";
import { downloadMessageFiles } from "./message-file-downloads.ts";
import { warnOnTruncatedSlackUrl } from "./message-url-warning.ts";
import { toThreadListMessage } from "./message-thread-info.ts";

export type MessageWatchOptions = {
  workspace?: string;
  threadTs?: string;
  ts?: string;
  since?: string;
  interval: string;
  count?: string;
  timeout?: string;
  includeSelf?: boolean;
  maxBodyChars: string;
};

const MIN_INTERVAL_SECONDS = 5;
const MAX_BACKOFF_MS = 5 * 60_000;

/** Compares Slack "seconds.micros" timestamps exactly (floats lose the micros). */
export function compareSlackTs(a: string, b: string): number {
  const [aSec = "0", aFrac = ""] = a.split(".");
  const [bSec = "0", bFrac = ""] = b.split(".");
  const secDiff = BigInt(aSec) - BigInt(bSec);
  if (secDiff !== 0n) {
    return secDiff > 0n ? 1 : -1;
  }
  const width = Math.max(aFrac.length, bFrac.length);
  const fa = aFrac.padEnd(width, "0");
  const fb = bFrac.padEnd(width, "0");
  return fa === fb ? 0 : fa > fb ? 1 : -1;
}

/** Parses "90", "90s", "30m", "2h" or "1d" into milliseconds. */
export function parseDurationMs(value: string, flag: string): number {
  const m = /^(\d+)\s*([smhd]?)$/i.exec(value.trim());
  if (!m) {
    throw new Error(`${flag} must be a number of seconds or a duration like 30m, 2h, 1d`);
  }
  const n = Number.parseInt(m[1]!, 10);
  const unit = (m[2] || "s").toLowerCase();
  const mult = unit === "d" ? 86_400_000 : unit === "h" ? 3_600_000 : unit === "m" ? 60_000 : 1000;
  return n * mult;
}

export type WatchResult = { reason: "count" | "timeout"; last_ts: string; replies: number };

/**
 * Polls a thread and emits each reply newer than `sinceTs`, oldest first.
 * Transient poll errors are reported via `onError` and retried with backoff.
 */
export async function watchThread(input: {
  rootTs: string;
  sinceTs: string;
  fetchReplies: (oldest: string) => Promise<SlackMessageSummary[]>;
  emit: (msg: SlackMessageSummary) => Promise<void> | void;
  onError?: (err: unknown) => void;
  skipUserId?: string;
  intervalMs: number;
  maxReplies?: number;
  timeoutMs?: number;
  sleep?: (ms: number) => Promise<void>;
  now?: () => number;
}): Promise<WatchResult> {
  const sleep = input.sleep ?? ((ms: number) => new Promise((r) => setTimeout(r, ms)));
  const now = input.now ?? Date.now;
  const deadline = input.timeoutMs !== undefined ? now() + input.timeoutMs : undefined;
  let lastTs = input.sinceTs;
  let emitted = 0;
  let failures = 0;

  for (;;) {
    let waitMs = input.intervalMs;
    try {
      const messages = await input.fetchReplies(lastTs);
      failures = 0;
      const fresh = messages
        .filter((m) => m.ts && m.ts !== input.rootTs && compareSlackTs(m.ts, lastTs) > 0)
        .sort((a, b) => compareSlackTs(a.ts, b.ts));
      for (const m of fresh) {
        lastTs = m.ts;
        if (input.skipUserId && m.user === input.skipUserId) {
          continue;
        }
        await input.emit(m);
        emitted++;
      }
      // Finish the whole batch before stopping so no reply is silently dropped.
      if (input.maxReplies !== undefined && emitted >= input.maxReplies) {
        return { reason: "count", last_ts: lastTs, replies: emitted };
      }
    } catch (err: unknown) {
      failures++;
      input.onError?.(err);
      waitMs = Math.min(input.intervalMs * 2 ** failures, MAX_BACKOFF_MS);
    }

    if (deadline !== undefined) {
      const remaining = deadline - now();
      if (remaining <= 0) {
        return { reason: "timeout", last_ts: lastTs, replies: emitted };
      }
      waitMs = Math.min(waitMs, remaining);
    }
    await sleep(waitMs);
  }
}

function parsePositiveInt(value: string | undefined, flag: string): number | undefined {
  if (value === undefined) {
    return undefined;
  }
  const n = Number.parseInt(value, 10);
  if (!Number.isFinite(n) || n < 1 || String(n) !== value.trim()) {
    throw new Error(`${flag} must be a positive integer`);
  }
  return n;
}

export async function handleMessageWatch(input: {
  ctx: CliContext;
  targetInput: string;
  options: MessageWatchOptions;
  writeLine: (line: string) => void;
  writeError: (line: string) => void;
}): Promise<WatchResult> {
  const { ctx, options } = input;
  const intervalMs = parseDurationMs(options.interval, "--interval");
  if (intervalMs < MIN_INTERVAL_SECONDS * 1000) {
    throw new Error(`--interval must be at least ${MIN_INTERVAL_SECONDS}s`);
  }
  const maxReplies = parsePositiveInt(options.count, "--count");
  const timeoutMs = options.timeout ? parseDurationMs(options.timeout, "--timeout") : undefined;
  const maxBodyChars = Number.parseInt(options.maxBodyChars, 10);

  const target = parseMsgTarget(input.targetInput);
  if (target.kind === "user") {
    throw new Error("message watch needs a message URL, or a channel with --thread-ts/--ts.");
  }
  const flagWorkspace = ctx.effectiveWorkspaceUrl(options.workspace);
  const workspaceUrl = target.kind === "url" ? target.ref.workspace_url : flagWorkspace;

  // Resolve the thread root, the starting cursor and the caller's own user ID once.
  const setup = await ctx.withAutoRefresh({
    workspaceUrl,
    work: async () => {
      const { client, workspace_url } = await ctx.getClientForWorkspace(workspaceUrl);
      let channelId: string;
      let rootTs: string;
      if (target.kind === "url") {
        warnOnTruncatedSlackUrl(target.ref);
        channelId = target.ref.channel_id;
        const msg = await fetchMessage(client, { ref: target.ref });
        rootTs = msg.thread_ts ?? msg.ts;
      } else {
        await ctx.assertWorkspaceSpecifiedForChannelNames({
          workspaceUrl: flagWorkspace,
          channels: [target.channel],
        });
        channelId = await resolveChannelId(client, target.channel);
        const threadTs = options.threadTs?.trim();
        const ts = options.ts?.trim();
        if (threadTs) {
          rootTs = threadTs;
        } else if (ts) {
          const msg = await fetchMessage(client, {
            ref: {
              workspace_url: workspace_url ?? workspaceUrl ?? "",
              channel_id: channelId,
              message_ts: ts,
              raw: input.targetInput,
            },
          });
          rootTs = msg.thread_ts ?? msg.ts;
        } else {
          throw new Error("When targeting a channel, pass --thread-ts or --ts.");
        }
      }

      let sinceTs = options.since?.trim();
      if (!sinceTs) {
        const existing = await fetchThread(client, { channelId, threadTs: rootTs });
        sinceTs = existing.reduce(
          (max, m) => (m.ts && compareSlackTs(m.ts, max) > 0 ? m.ts : max),
          rootTs,
        );
      }

      let selfUserId: string | undefined;
      if (!options.includeSelf) {
        const who = await client.api("auth.test", {});
        selfUserId = typeof who.user_id === "string" ? who.user_id : undefined;
      }
      return { channelId, rootTs, sinceTs, selfUserId };
    },
  });

  input.writeLine(
    JSON.stringify({
      event: "watching",
      channel_id: setup.channelId,
      thread_ts: setup.rootTs,
      since: setup.sinceTs,
    }),
  );

  const result = await watchThread({
    rootTs: setup.rootTs,
    sinceTs: setup.sinceTs,
    skipUserId: setup.selfUserId,
    intervalMs,
    maxReplies,
    timeoutMs,
    fetchReplies: (oldest) =>
      ctx.withAutoRefresh({
        workspaceUrl,
        work: async () => {
          const { client } = await ctx.getClientForWorkspace(workspaceUrl);
          return fetchThread(client, {
            channelId: setup.channelId,
            threadTs: setup.rootTs,
            oldest,
          });
        },
      }),
    emit: async (msg) => {
      const { auth } = await ctx.getClientForWorkspace(workspaceUrl);
      const downloadedPaths = await downloadMessageFiles({ auth, messages: [msg] });
      const compact = toThreadListMessage(toCompactMessage(msg, { maxBodyChars, downloadedPaths }));
      input.writeLine(JSON.stringify(pruneEmpty({ event: "reply", ...compact })));
    },
    onError: (err) => input.writeError(`poll failed, retrying: ${ctx.errorMessage(err)}`),
  });

  input.writeLine(JSON.stringify({ event: "done", ...result }));
  return result;
}
