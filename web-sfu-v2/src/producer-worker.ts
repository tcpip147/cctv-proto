import * as mediasoup from "mediasoup";
import {
  PlainTransport,
  Producer,
  Router,
  RouterRtpCodecCapability,
  RtpCodecParameters,
  Worker,
  WorkerEvents,
} from "mediasoup/types";
import { execSync } from "node:child_process";
import logger from "./logger.js";
import { MediaWorker } from "./media-worker.js";
import { Proxy } from "./proxy.js";

interface WorkerOptions {
  id: string;
  useCore: number;
  portRange: [number, number];
  availableCodecs: RouterRtpCodecCapability[];
  producerOptions: ProducerOptions[];
}

interface ProducerOptions {
  ip: string;
  port: number;
  videoCodecs: RtpCodecParameters[];
  audioCodecs?: RtpCodecParameters[];
}

class ProducerWorker extends MediaWorker {
  private worker!: Worker;
  private router!: Router;
  private transports = new Map<number, PlainTransport>();
  private proxies: Proxy[] = [];
  private producers = new Map<number, Producer>();
  private isStarted = false;

  public static async create(options: WorkerOptions) {
    const instance = new ProducerWorker(options);
    logger.info(`Producer worker ${options.id} starting...`);
    await instance.start();
    logger.info(`Producer worker ${options.id} started.`);
    return instance;
  }

  private constructor(private options: WorkerOptions) {
    super();
  }

  public async start() {
    this.worker = await mediasoup.createWorker({
      rtcMinPort: this.options.portRange[0],
      rtcMaxPort: this.options.portRange[1],
    });
    this.worker.on("subprocessclose", () => {
      this.proxies.forEach((proxy) => {
        proxy.stop();
      });
      this.stop();
      logger.info(`Producer worker ${this.options.id} stopped.`);
    });
    this.fixWorkerToCore();
    this.router = await this.worker.createRouter({
      mediaCodecs: this.options.availableCodecs,
    });

    for (const producerOptions of this.options.producerOptions) {
      const transport = await this.router.createPlainTransport({
        listenIp: { ip: "127.0.0.1" },
        rtcpMux: true,
        comedia: true,
      });

      this.transports.set(producerOptions.port, transport);

      const proxy = new Proxy(
        producerOptions.ip,
        producerOptions.port,
        "127.0.0.1",
        transport.tuple.localPort,
      );
      this.proxies.push(proxy);
      await proxy.start();

      const producer = await transport.produce({
        kind: "video",
        rtpParameters: {
          codecs: producerOptions.videoCodecs,
          encodings: [
            {
              ssrc: 1000,
            },
          ],
        },
      });

      this.producers.set(producerOptions.port, producer);

      logger.info(`Producer created on port ${producerOptions.port}.`);

      this.isStarted = true;

      /*
      setInterval(async () => {
        const stats = await transport.getStats();
        console.log(stats[0].bytesReceived);
      }, 3000);
      */

      /*
      setInterval(async () => {
        const stats = await producer.getStats();
        console.log(stats);
      }, 3000);
      */
    }
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

  public override getId() {
    return this.options.id;
  }

  public override async createPipeTransport() {
    const transport = await this.router.createPipeTransport({
      listenIp: { ip: "127.0.0.1" },
    });
    return transport;
  }

  public getPid() {
    return this.worker.pid;
  }

  public stop() {
    this.worker.close();
    this.isStarted = false;
  }

  public isRunning() {
    return this.isStarted;
  }

  public getProducers() {
    return this.producers;
  }

  public on(event: keyof WorkerEvents, listener: (...args: any[]) => void) {
    this.worker.on(event, listener);
  }
}

export { ProducerWorker };

