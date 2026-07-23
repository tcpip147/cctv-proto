import * as mediasoup from "mediasoup";
import {
  Consumer,
  DtlsParameters,
  IceState,
  PlainTransport,
  Producer,
  Router,
  RouterRtpCodecCapability,
  RtpCapabilities,
  RtpCodecParameters,
  WebRtcServer,
  WebRtcTransport,
  Worker,
} from "mediasoup/types";
import logger from "./logger.js";
import { Proxy } from "./proxy.js";

interface HubOption {
  id: string;
  publicIp: string;
  portRange: [number, number];
  availableCodecs: RouterRtpCodecCapability[];
}

interface ProducerHubOption extends HubOption {
  role: ["producer"];
  producerOptions: ProducerOption[];
}

interface CombinedHubOption extends HubOption {
  role: ["producer", "consumer"] | ["consumer", "producer"];
  producerOptions: ProducerOption[];
  consumerOption: ConsumerOption;
}

interface ProducerOption {
  id: string;
  ip: string;
  port: number;
  videoCodecs: RtpCodecParameters[];
}

interface ConsumerHubOption extends HubOption {
  role: ["consumer"];
  consumerOption: ConsumerOption;
}

interface ConsumerOption {
  port: number;
}

class Hub {
  public id: string;
  public role: string[];
  public publicIp: string;
  public portRange: [number, number];
  public availableCodecs: RouterRtpCodecCapability[];
  public consumerOption?: ConsumerOption;
  public producerOptions?: ProducerOption[];
  public isStopped = true;

  private worker!: Worker;
  private router!: Router;
  private webRtcServer!: WebRtcServer;
  private plainTransports = new Map<string, PlainTransport>();
  private producers = new Map<string, Producer>();
  private proxies: Proxy[] = [];
  private pipeProducers = new Map<string, Producer[]>();
  private onCloseListeners: ((hubId: string) => void)[] = [];
  private webRtcTransports = new Map<string, WebRtcTransport>();
  private consumers = new Map<string, Consumer[]>();

  public static create(
    options: CombinedHubOption | ProducerHubOption | ConsumerHubOption,
  ) {
    return new Hub(options);
  }

  private constructor(
    options: CombinedHubOption | ProducerHubOption | ConsumerHubOption,
  ) {
    this.id = options.id;
    this.role = options.role;
    this.publicIp = options.publicIp;
    this.portRange = options.portRange;
    this.availableCodecs = options.availableCodecs;
    if ("producerOptions" in options) {
      this.producerOptions = options.producerOptions;
    }
    if ("consumerOption" in options) {
      this.consumerOption = options.consumerOption;
    }
  }

  public async start() {
    logger.info(`Hub ${this.id} starting...`);

    this.worker = await mediasoup.createWorker({
      rtcMinPort: this.portRange[0],
      rtcMaxPort: this.portRange[1],
    });

    this.worker.on("subprocessclose", () => {
      this.stop();
      this.onCloseListeners.forEach((listener) => listener(this.id));
    });

    this.router = await this.worker.createRouter({
      mediaCodecs: this.availableCodecs,
    });

    if (this.role.includes("consumer") && this.consumerOption) {
      this.webRtcServer = await this.worker.createWebRtcServer({
        listenInfos: [
          {
            protocol: "udp",
            ip: this.publicIp,
            port: this.consumerOption.port,
          },
        ],
      });
    }

    if (this.role.includes("producer") && this.producerOptions) {
      for (const producerOptions of this.producerOptions) {
        const transport = await this.router.createPlainTransport({
          listenIp: { ip: "127.0.0.1" },
          rtcpMux: true,
          comedia: true,
        });

        this.plainTransports.set(producerOptions.id, transport);

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

        this.producers.set(producerOptions.id, producer);

        /*
        setInterval(async () => {
          const status = await transport.getStats();
          console.log(status[0].bytesReceived);
        }, 3000);
        */

        logger.info(
          `Producer ${producerOptions.id} created on port ${producerOptions.port}`,
        );
      }
    }

    this.isStopped = false;

    logger.info(`Hub ${this.id} started`);
  }

  public stop() {
    logger.info(`Hub ${this.id} stopping...`);
    this.proxies.forEach((proxy) => {
      proxy.stop();
    });
    this.proxies.length = 0;
    this.producers.clear();
    this.pipeProducers.clear();
    this.consumers.clear();
    this.webRtcTransports.clear();
    this.worker!.close();
    this.isStopped = true;
    logger.info(`Hub ${this.id} stopped.`);
  }

  public async pipeToRouter(toHub: Hub) {
    logger.info(`Hub ${this.id} pipe to ${toHub.id}`);
    for (const producer of this.producers.values()) {
      const { pipeProducer } = await this.router.pipeToRouter({
        producerId: producer.id,
        router: toHub.router,
      });
      const videoId = this.getVideoId(producer);
      if (videoId) {
        if (!toHub.pipeProducers.has(this.id)) {
          toHub.pipeProducers.set(this.id, []);
        }
        toHub.pipeProducers.get(this.id)!.push(pipeProducer!);
        toHub.producers.set(videoId, pipeProducer!);
      }
    }
  }

  public removePipe(fromHub: string) {
    const pipeProducers = this.pipeProducers.get(fromHub);
    if (pipeProducers) {
      for (const pipeProducer of pipeProducers) {
        this.producers.delete(this.getVideoId(pipeProducer)!);
      }
    }
  }

  private getVideoId(producer: Producer) {
    for (const videoId of this.producers.keys()) {
      const videoProducer = this.producers.get(videoId)!;
      if (videoProducer.id === producer.id) {
        return videoId;
      }
    }
    return null;
  }

  public addOnCloseListener(listener: (hubId: string) => void) {
    this.onCloseListeners.push(listener);
  }

  public removeOnCloseListener(listener: (hubId: string) => void) {
    this.onCloseListeners = this.onCloseListeners.filter((l) => l !== listener);
  }

  public getConsumerCount() {
    return this.consumers.size;
  }

  public getRouterRtpCapabilities() {
    return this.router.rtpCapabilities;
  }

  public async createWebRtcTransport() {
    const webRtcTransport = await this.router.createWebRtcTransport({
      webRtcServer: this.webRtcServer,
      enableUdp: true,
      enableTcp: true,
      preferUdp: true,
      iceConsentTimeout: 10,
    });

    webRtcTransport.on("icestatechange", (iceState: IceState) => {
      if (iceState === "disconnected") {
        webRtcTransport.close();
        this.consumers.delete(webRtcTransport.id);
        this.webRtcTransports.delete(webRtcTransport.id);
      }
    });

    this.webRtcTransports.set(webRtcTransport.id, webRtcTransport);

    return webRtcTransport;
  }

  public async createConsumer({
    videoId,
    transportId,
    rtpCapabilities,
  }: {
    videoId: string;
    transportId: string;
    rtpCapabilities: RtpCapabilities;
  }) {
    const producer = this.producers.get(videoId);
    const canConsume = this.router.canConsume({
      producerId: producer!.id,
      rtpCapabilities,
    });
    if (canConsume) {
      const webRtcTransport = this.webRtcTransports.get(transportId)!;
      const consumer = await webRtcTransport.consume({
        producerId: producer!.id,
        rtpCapabilities,
        paused: true,
      });

      if (!this.consumers.has(webRtcTransport.id)) {
        this.consumers.set(webRtcTransport.id, []);
      }
      this.consumers.get(webRtcTransport.id)!.push(consumer);

      return {
        id: consumer.id,
        producerId: consumer.producerId,
        kind: consumer.kind,
        rtpParameters: consumer.rtpParameters,
      };
    }

    return null;
  }

  public async connectWebRtcTransport({
    transportId,
    dtlsParameters,
  }: {
    transportId: string;
    dtlsParameters: DtlsParameters;
  }) {
    const webRtcTransport = this.webRtcTransports.get(transportId);
    await webRtcTransport!.connect({ dtlsParameters });
  }

  public async resumeConsumer({
    transportId,
    consumerId,
  }: {
    transportId: string;
    consumerId: string;
  }) {
    const consumers = this.consumers.get(transportId);
    if (consumers) {
      const consumer = consumers.find((c) => c.id === consumerId);
      await consumer!.resume();
    }
  }
}

export { Hub };
