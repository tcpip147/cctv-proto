import path from "node:path";
import { downloadZip } from "./download";
import { spawn } from "child_process";
import { ChildProcess } from "node:child_process";

const targetHost = "127.0.0.1";

const childProcesses: ChildProcess[] = [];

(async () => {
  await downloadZip({
    url: "https://github.com/BtbN/FFmpeg-Builds/releases/download/latest/ffmpeg-master-latest-win64-gpl.zip",
    targetFiles: ["bin/ffmpeg.exe", "bin/ffprobe.exe"],
    outputPaths: [path.resolve("ffmpeg.exe"), path.resolve("ffprobe.exe")],
  });

  const ports = [];
  for (let i = 0; i < 20; i++) {
    ports.push(25000 + i);
  }
  sendSrc(ports);

  process.on("SIGINT", () => {
    killAllProcesses();
    process.exit(0);
  });

  process.on("SIGTERM", () => {
    killAllProcesses();
    process.exit(0);
  });

  process.on("uncaughtException", (err) => {
    killAllProcesses();
    process.exit(1);
  });

  process.on("exit", () => {
    killAllProcesses();
  });
})();

async function sendSrc(ports: number[]) {
  const file = path.resolve("ffmpeg.exe");
  const command = [file];
  command.push("-loglevel", "error");
  command.push("-re");
  command.push("-f", "lavfi");
  command.push("-i", "testsrc2=size=1280x720:rate=30");

  const numOutputs = ports.length;

  const videoLabels = Array.from({ length: numOutputs }, (_, i) => `v${i}`);
  const outLabels = Array.from({ length: numOutputs }, (_, i) => `out${i}`);

  let filterComplexStr = `[0:v]split=${numOutputs}${videoLabels.map((l) => `[${l}]`).join("")}; `;

  const drawtextFilters = ports.map((port, i) => {
    return `[${videoLabels[i]}]drawtext=text='PORT ${port}':x=(w-tw)/2:y=(h-th)/2:fontcolor=white:fontsize=200:box=1:boxcolor=black@0.5[${outLabels[i]}]`;
  });
  filterComplexStr += drawtextFilters.join("; ");

  command.push("-filter_complex", filterComplexStr);

  ports.forEach((port, i) => {
    command.push("-map", `[${outLabels[i]}]`);
    command.push("-c:v", "libx264");
    command.push("-pix_fmt", "yuv420p");
    command.push("-preset", "ultrafast");
    command.push("-tune", "zerolatency");
    command.push("-b:v", "1500k");
    command.push("-maxrate", "2000k");
    command.push("-minrate", "1000k");
    command.push("-bufsize", "1000k");
    command.push("-g", "30");
    command.push("-ssrc", "1000");
    command.push("-payload_type", "96");
    command.push("-f", "rtp");
    command.push(`rtp://${targetHost}:${port}?pkt_size=1050`);
  });

  console.log(command.join(" "));

  const p = spawn(command[0], command.slice(1), {
    detached: false,
  });

  childProcesses.push(p);

  p.stderr.on("data", (data) => {
    console.error(`stderr: ${data}`);
  });

  p.on("close", (code) => {
    console.log(`child process exited with code ${code}`);
  });
}

function killAllProcesses() {
  console.log("Killing all processes...");
  for (const p of childProcesses) {
    if (!p.killed) {
      p.kill("SIGKILL");
    }
  }
}
