import { describe, expect, test } from "bun:test";
import { compareSlackTs, parseDurationMs, watchThread } from "../src/cli/message-watch.ts";
import { fetchThread, type SlackMessageSummary } from "../src/slack/messages.ts";

function msg(ts: string, user = "U2"): SlackMessageSummary {
  return { channel_id: "C1", ts, thread_ts: "100.000001", user, text: "hi", markdown: "hi" };
}

describe("compareSlackTs", () => {
  test("compares micros exactly", () => {
    expect(compareSlackTs("1700000000.000002", "1700000000.000001")).toBe(1);
    expect(compareSlackTs("1700000000.000001", "1700000000.000001")).toBe(0);
    expect(compareSlackTs("999.9", "1000.0")).toBe(-1);
    expect(compareSlackTs("1.5", "1.500000")).toBe(0);
  });
});

describe("parseDurationMs", () => {
  test("accepts bare seconds and unit suffixes", () => {
    expect(parseDurationMs("90", "--x")).toBe(90_000);
    expect(parseDurationMs("30m", "--x")).toBe(1_800_000);
    expect(parseDurationMs("2h", "--x")).toBe(7_200_000);
    expect(parseDurationMs("1d", "--x")).toBe(86_400_000);
  });
  test("rejects garbage", () => {
    expect(() => parseDurationMs("soon", "--timeout")).toThrow("--timeout");
  });
});

describe("watchThread", () => {
  test("emits only new non-self replies in order and stops at count after the batch", async () => {
    const polls: SlackMessageSummary[][] = [
      [msg("100.000001", "U1"), msg("101.000000")],
      [msg("100.000001", "U1"), msg("103.000000"), msg("102.000000", "SELF"), msg("104.000000")],
    ];
    const oldestSeen: string[] = [];
    const emitted: string[] = [];
    const result = await watchThread({
      rootTs: "100.000001",
      sinceTs: "101.000000",
      skipUserId: "SELF",
      intervalMs: 5000,
      maxReplies: 1,
      fetchReplies: async (oldest) => {
        oldestSeen.push(oldest);
        return polls.shift() ?? [];
      },
      emit: (m) => {
        emitted.push(m.ts);
      },
      sleep: async () => {},
    });
    expect(emitted).toEqual(["103.000000", "104.000000"]);
    expect(oldestSeen).toEqual(["101.000000", "101.000000"]);
    expect(result).toEqual({ reason: "count", last_ts: "104.000000", replies: 2 });
  });

  test("self replies advance the cursor without counting", async () => {
    const polls = [[msg("102.000000", "SELF")], [msg("103.000000")]];
    const oldestSeen: string[] = [];
    const result = await watchThread({
      rootTs: "100.000001",
      sinceTs: "101.000000",
      skipUserId: "SELF",
      intervalMs: 5000,
      maxReplies: 1,
      fetchReplies: async (oldest) => {
        oldestSeen.push(oldest);
        return polls.shift() ?? [];
      },
      emit: () => {},
      sleep: async () => {},
    });
    expect(oldestSeen).toEqual(["101.000000", "102.000000"]);
    expect(result.replies).toBe(1);
  });

  test("times out and retries errors with backoff", async () => {
    let clock = 0;
    const sleeps: number[] = [];
    const errors: unknown[] = [];
    let calls = 0;
    const result = await watchThread({
      rootTs: "100.000001",
      sinceTs: "100.000001",
      intervalMs: 10_000,
      timeoutMs: 60_000,
      fetchReplies: async () => {
        calls++;
        if (calls === 1) {
          throw new Error("ratelimited");
        }
        return [];
      },
      emit: () => {},
      onError: (e) => errors.push(e),
      now: () => clock,
      sleep: async (ms) => {
        sleeps.push(ms);
        clock += ms;
      },
    });
    expect(errors).toHaveLength(1);
    expect(sleeps[0]).toBe(20_000);
    expect(sleeps.reduce((a, b) => a + b, 0)).toBe(60_000);
    expect(result).toEqual({ reason: "timeout", last_ts: "100.000001", replies: 0 });
  });
});

describe("fetchThread oldest", () => {
  test("passes an exclusive oldest bound to conversations.replies", async () => {
    const params: Record<string, unknown>[] = [];
    const client = {
      api: async (_m: string, p: Record<string, unknown>) => {
        params.push(p);
        return { messages: [] };
      },
    };
    await fetchThread(client as never, { channelId: "C1", threadTs: "1.0", oldest: "2.0" });
    expect(params[0]).toMatchObject({ oldest: "2.0", inclusive: false });
  });
});

describe("watchThread self filter", () => {
  test("skips own API posts but emits own replies typed in a Slack client", async () => {
    const typed = { ...msg("103.000000", "SELF"), client_msg_id: "7BD1505C-2D48-4E7B" };
    const polls = [[msg("102.000000", "SELF"), typed, msg("104.000000")]];
    const emitted: string[] = [];
    await watchThread({
      rootTs: "100.000001",
      sinceTs: "101.000000",
      skipUserId: "SELF",
      intervalMs: 5000,
      maxReplies: 1,
      fetchReplies: async () => polls.shift() ?? [],
      emit: (m) => {
        emitted.push(m.ts);
      },
      sleep: async () => {},
    });
    expect(emitted).toEqual(["103.000000", "104.000000"]);
  });
});
