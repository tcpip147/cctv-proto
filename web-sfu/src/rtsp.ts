import { ChildProcess, spawn } from "node:child_process";
import path from "node:path";

interface RtspRequest {
  rtspUrl: string;
  rtpUrl: string;
}

class Rtsp {
  private requests: RtspRequest[];
  private childProcesses: ChildProcess[] = [];

  public static create(options: RtspRequest[]) {
    return new Rtsp(options);
  }

  constructor(options: RtspRequest[]) {
    this.requests = options;
  }

  public async start({ groupSize }: { groupSize: number }) {
    process.on("SIGINT", () => {
      this.stop();
      process.exit(0);
    });

    process.on("SIGTERM", () => {
      this.stop();
      process.exit(0);
    });

    process.on("uncaughtException", (err) => {
      this.stop();
      process.exit(1);
    });

    process.on("exit", () => {
      this.stop();
    });

    const groups = [];
    for (let i = 0; i < this.requests.length; i += groupSize) {
      groups.push(this.requests.slice(i, i + groupSize));
    }

    for (const group of groups) {
      const file = path.resolve("ffmpeg.exe");
      const command = [file];
      command.push("-loglevel", "error");

      let i = 0;
      for (const request of group) {
        command.push("-rtsp_transport", "tcp");
        command.push("-i", request.rtspUrl);
        command.push("-map", `${i++}:v:0`);
        command.push("-c:v", "copy");
        command.push("-f", "rtp");
        command.push("-payload_type", "96");
        command.push("-ssrc", "1000");
        command.push(request.rtpUrl);
      }

      console.log(command.join(" "));

      const p = spawn(command[0], command.slice(1), {
        detached: false,
      });

      this.childProcesses.push(p);

      p.stderr.on("data", (data) => {
        console.error(`stderr: ${data}`);
      });

      p.on("close", (code) => {
        console.log(`child process exited with code ${code}`);
      });
    }
  }

  public stop() {
    for (const p of this.childProcesses) {
      if (!p.killed) {
        p.kill("SIGKILL");
      }
    }
  }
}

export { Rtsp };
