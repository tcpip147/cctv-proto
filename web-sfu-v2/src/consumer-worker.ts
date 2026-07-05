import * as mediasoup from "mediasoup";
import { Router, RouterRtpCodecCapability, Worker } from "mediasoup/types";
import { execSync } from "node:child_process";
import logger from "./logger.js";
import { MediaWorker } from "./media-worker.js";

interface WorkerOptions {
  id: string;
  useCore: number;
  portRange: [number, number];
  availableCodecs: RouterRtpCodecCapability[];
}

class ConsumerWorker extends MediaWorker {
  private worker!: Worker;
  private router!: Router;
  private isStarted = false;

  public static async create(options: WorkerOptions) {
    const instance = new ConsumerWorker(options);
    logger.info(`Consumer worker ${options.id} starting...`);
    await instance.start();
    logger.info(`Consumer worker ${options.id} started.`);
    return instance;
  }

  constructor(private options: WorkerOptions) {
    super();
  }

  public async start() {
    this.worker = await mediasoup.createWorker({
      rtcMinPort: this.options.portRange[0],
      rtcMaxPort: this.options.portRange[1],
    });
    this.worker.on("subprocessclose", () => {
      this.stop();
      logger.info(`Consuer worker ${this.options.id} stopped.`);
    });
    this.fixWorkerToCore();
    this.router = await this.worker.createRouter({
      mediaCodecs: this.options.availableCodecs,
    });
  }

  private fixWorkerToCore() {
    let command;
    if (process.platform === "win32") {
      command = `powershell -Command "(Get-Process -Id ${this.worker.pid}).ProcessorAffinity = ${this.options.useCore}"`;
    } else {
      command = `taskset -pc ${this.options.useCore} ${this.worker.pid}`;
    }
    execSync(command);
  }

  public getId(): string {
    return this.options.id;
  }

  public override async createPipeTransport() {
    const transport = await this.router.createPipeTransport({
      listenIp: { ip: "127.0.0.1" },
    });
    return transport;
  }

  public stop() {
    this.worker.close();
    this.isStarted = false;
  }
}

export { ConsumerWorker };

