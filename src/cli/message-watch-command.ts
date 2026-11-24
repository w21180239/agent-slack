import type { Command } from "commander";
import type { CliContext } from "./context.ts";
import { handleMessageWatch, type MessageWatchOptions } from "./message-watch.ts";

export function registerMessageWatchCommand(input: { messageCmd: Command; ctx: CliContext }): void {
  const { messageCmd } = input;
  messageCmd
    .command("watch")
    .description(
      "Follow a thread: poll for new replies and print each as one JSON line (NDJSON) until --count or --timeout",
    )
    .argument("<target>", "Slack message URL (root or any reply), #channel, or channel ID")
    .option(
      "--workspace <url>",
      "Workspace selector (full URL or unique substring; needed when using #channel/channel id across multiple workspaces)",
    )
    .option("--thread-ts <ts>", "Thread root ts (when targeting a channel)")
    .option("--ts <ts>", "Message ts (resolve message to its thread; when targeting a channel)")
    .option(
      "--since <ts>",
      "Only report replies after this ts (default: the newest reply when the watch starts)",
    )
    .option("--interval <duration>", "Poll interval, e.g. 30, 30s, 2m (min 5s)", "30s")
    .option("--count <n>", "Exit after this many new replies (default: keep watching)")
    .option("--timeout <duration>", "Exit after this long, e.g. 30m, 2h, 1d (default: none)")
    .option(
      "--include-self",
      "Also report your own API-posted replies (agent-slack sends); replies you type in Slack always report",
    )
    .option(
      "--max-body-chars <n>",
      "Max content characters to include (default 8000, -1 for unlimited)",
      "8000",
    )
    .action(async (...args) => {
      const [targetInput, options] = args as [string, MessageWatchOptions];
      try {
        await handleMessageWatch({
          ctx: input.ctx,
          targetInput,
          options,
          writeLine: (line) => console.log(line),
          writeError: (line) => console.error(line),
        });
      } catch (err: unknown) {
        console.error(input.ctx.errorMessage(err));
        process.exitCode = 1;
      }
    });
}
