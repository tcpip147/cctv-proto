import * as mediasoup from "mediasoup";
import {
  Producer,
  Router,
  RouterRtpCodecCapability,
  RtpCodecParameters,
  Worker,
} from "mediasoup/types";
import { execSync } from "node:child_process";
import logger from "./logger.js";
import { Proxy } from "./proxy.js";

interface ProducerWorkerOptions {
  id: string;
  useCore: number;
  role: ["producer", "consumer"] | ["consumer", "producer"] | ["producer"];
  portRange: [number, number];
  availableCodecs: RouterRtpCodecCapability[];
  producerOptions: ProducerOptions[];
  broadcast: boolean;
}

interface ProducerOptions {
  ip: string;
  port: number;
  videoCodecs: RtpCodecParameters[];
  audioCodecs?: RtpCodecParameters[];
}

interface ConsumerWorkerOptions {
  id: string;
  useCore: number;
  role: ["consumer"];
  portRange: [number, number];
  availableCodecs: RouterRtpCodecCapability[];
  producerOptions?: never;
  broadcast?: never;
}

class MediaWorker {
  private worker!: Worker;
  private router!: Router;
  private proxies: Proxy[] = [];
  private producers = new Map<number, Producer>();
  private pipeProducers: Producer[] = [];
  private isStarted = false;

  public static async create(
    options: ProducerWorkerOptions | ConsumerWorkerOptions,
  ) {
    const instance = new MediaWorker(options);
    await instance.start();
    return instance;
  }

  private constructor(
    private options: ProducerWorkerOptions | ConsumerWorkerOptions,
  ) {}

  public async start() {
    logger.info(`Worker ${this.options.id} starting...`);

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

    if (this.hasProducer()) {
      const options = this.options as ProducerWorkerOptions;
      for (const producerOptions of options.producerOptions) {
        const transport = await this.router.createPlainTransport({
          listenIp: { ip: "127.0.0.1" },
          rtcpMux: true,
          comedia: true,
        });

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

        logger.info(`Producer created on port ${producerOptions.port}`);

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

    logger.info(`Worker ${this.options.id} started`);
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

  public stop() {
    this.worker.close();
    this.isStarted = false;
  }

  public getId() {
    return this.options.id;
  }

  public hasProducer() {
    return this.options.producerOptions !== undefined;
  }

  public async createPipeTransport() {
    const transport = await this.router.createPipeTransport({
      listenIp: { ip: "127.0.0.1" },
    });
    return transport;
  }

  public getProducers() {
    return this.producers;
  }

  public addPipeProducer(producer: Producer) {
    this.pipeProducers.push(producer);
  }

  public getPipeProducers() {
    return this.pipeProducers;
  }

  public canBroadcast() {
    return this.options.broadcast;
  }

  async getRouterRtpCapabilities() {
    return this.router.rtpCapabilities;
  }
}

export { MediaWorker };
