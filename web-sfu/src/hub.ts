import * as mediasoup from "mediasoup";
import {
  Consumer,
  DtlsParameters,
  PlainTransport,
  Producer,
  Router,
  RouterRtpCodecCapability,
  RtpCapabilities,
  RtpCodecParameters,
  WebRtcTransport,
  Worker,
} from "mediasoup/types";
import logger from "./logger.js";
import { Proxy } from "./proxy.js";

interface HubOptions {
  id: string;
  portRange: [number, number];
  availableCodecs: RouterRtpCodecCapability[];
}

interface ProducerHubOptions extends HubOptions {
  role: ["producer", "consumer"] | ["consumer", "producer"] | ["producer"];
  producerOptions: ProducerOptions[];
}

interface ConsumerHubOptions extends HubOptions {
  role: ["consumer"];
}

interface ProducerOptions {
  id: string;
  ip: string;
  port: number;
  videoCodecs: RtpCodecParameters[];
}

class Hub {
  public id: string;
  public role: string[];
  public portRange: [number, number];
  public availableCodecs: RouterRtpCodecCapability[];
  public producerOptions?: ProducerOptions[];
  public isStopped = true;

  private worker!: Worker;
  private router!: Router;
  private plainTransports = new Map<string, PlainTransport>();
  private producers = new Map<string, Producer>();
  private proxies: Proxy[] = [];
  private pipeProducers = new Map<string, Producer[]>();
  private onCloseListeners: ((hubId: string) => void)[] = [];
  private webRtcTransports = new Map<string, WebRtcTransport>();
  private consumers = new Map<string, Consumer>();

  public static create(options: ProducerHubOptions | ConsumerHubOptions) {
    return new Hub(options);
  }

  private constructor(options: ProducerHubOptions | ConsumerHubOptions) {
    this.id = options.id;
    this.role = options.role;
    this.portRange = options.portRange;
    this.availableCodecs = options.availableCodecs;
    if ("producerOptions" in options) {
      this.producerOptions = options.producerOptions;
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
      listenIps: [
        {
          ip: "127.0.0.1",
        },
      ],
      enableUdp: true,
      enableTcp: true,
      preferUdp: true,
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
      const webRtcTransport = this.webRtcTransports.get(transportId);
      const consumer = await webRtcTransport!.consume({
        producerId: producer!.id,
        rtpCapabilities,
        paused: true,
      });

      this.consumers.set(consumer.id, consumer);

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

  public async resumeConsumer({ consumerId }: { consumerId: string }) {
    const consumer = this.consumers.get(consumerId);
    await consumer!.resume();
  }
}

export { Hub };
