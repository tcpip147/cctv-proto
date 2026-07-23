import * as mediasoup from "mediasoup";
import {
  Consumer,
  DataConsumer,
  DataProducer,
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
  enableSctp?: boolean;
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
  private videoProducers = new Map<string, Producer>();
  private dataProducers = new Map<string, DataProducer>();
  private proxies: Proxy[] = [];
  private pipeVideoProducers = new Map<string, Producer[]>();
  private pipeDataProducers = new Map<string, DataProducer[]>();
  private onCloseListeners: ((hubId: string) => void)[] = [];
  private webRtcTransports = new Map<string, WebRtcTransport>();
  private videoConsumers = new Map<string, Consumer[]>();
  private dataConsumers = new Map<string, DataConsumer[]>();

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
      for (const producerOption of this.producerOptions) {
        const transport = await this.router.createPlainTransport({
          listenIp: { ip: "127.0.0.1" },
          rtcpMux: true,
          comedia: true,
          enableSctp: producerOption.enableSctp,
        });

        this.plainTransports.set(producerOption.id, transport);

        const proxy = new Proxy(
          producerOption.ip,
          producerOption.port,
          "127.0.0.1",
          transport.tuple.localPort,
        );
        this.proxies.push(proxy);
        await proxy.start();

        const videoProducer = await transport.produce({
          kind: "video",
          rtpParameters: {
            codecs: producerOption.videoCodecs,
            encodings: [
              {
                ssrc: 1000,
              },
            ],
          },
        });

        this.videoProducers.set(producerOption.id, videoProducer);

        const dataProducer = await transport.produceData({
          label: producerOption.id,
          sctpStreamParameters: {
            streamId: 0,
            ordered: false,
          },
        });

        this.dataProducers.set(dataProducer.id, dataProducer);

        /*
        setInterval(async () => {
          const status = await transport.getStats();
          console.log(status[0].bytesReceived);
        }, 3000);
        */

        logger.info(
          `Producer ${producerOption.id} created on port ${producerOption.port}`,
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
    this.videoProducers.clear();
    this.dataProducers.clear();
    this.pipeVideoProducers.clear();
    this.pipeDataProducers.clear();
    this.videoConsumers.clear();
    this.dataConsumers.clear();
    this.webRtcTransports.clear();
    this.worker!.close();
    this.isStopped = true;
    logger.info(`Hub ${this.id} stopped.`);
  }

  public async pipeToRouter(toHub: Hub) {
    logger.info(`Hub ${this.id} pipe to ${toHub.id}`);
    for (const producer of this.videoProducers.values()) {
      const { pipeProducer } = await this.router.pipeToRouter({
        producerId: producer.id,
        router: toHub.router,
      });
      const videoId = this.getVideoId(producer);
      if (videoId) {
        if (!toHub.pipeVideoProducers.has(this.id)) {
          toHub.pipeVideoProducers.set(this.id, []);
        }
        toHub.pipeVideoProducers.get(this.id)!.push(pipeProducer!);
        toHub.videoProducers.set(videoId, pipeProducer!);
      }
    }

    for (const producer of this.dataProducers.values()) {
      const { pipeDataProducer } = await this.router.pipeToRouter({
        producerId: producer.id,
        router: toHub.router,
      });
      const videoId = producer.label;
      if (videoId) {
        if (!toHub.pipeDataProducers.has(this.id)) {
          toHub.pipeDataProducers.set(this.id, []);
        }
        toHub.pipeDataProducers.get(this.id)!.push(pipeDataProducer!);
        toHub.dataProducers.set(videoId, pipeDataProducer!);
      }
    }
  }

  public removePipe(fromHub: string) {
    const pipeVideoProducers = this.pipeVideoProducers.get(fromHub);
    if (pipeVideoProducers) {
      for (const pipeVideoProducer of pipeVideoProducers) {
        this.videoProducers.delete(this.getVideoId(pipeVideoProducer)!);
      }
    }

    const pipeDataProducers = this.pipeDataProducers.get(fromHub);
    if (pipeDataProducers) {
      for (const pipeDataProducer of pipeDataProducers) {
        this.videoProducers.delete(pipeDataProducer.label);
      }
    }
  }

  private getVideoId(producer: Producer) {
    for (const videoId of this.videoProducers.keys()) {
      const videoProducer = this.videoProducers.get(videoId)!;
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
    return this.videoConsumers.size;
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
        this.videoConsumers.delete(webRtcTransport.id);
        this.dataConsumers.delete(webRtcTransport.id);
        this.webRtcTransports.delete(webRtcTransport.id);
      }
    });

    this.webRtcTransports.set(webRtcTransport.id, webRtcTransport);

    return webRtcTransport;
  }

  public async createVideoConsumer({
    videoId,
    transportId,
    rtpCapabilities,
  }: {
    videoId: string;
    transportId: string;
    rtpCapabilities: RtpCapabilities;
  }) {
    const producer = this.videoProducers.get(videoId);
    const canConsume = this.router.canConsume({
      producerId: producer!.id,
      rtpCapabilities,
    });
    if (canConsume) {
      const webRtcTransport = this.webRtcTransports.get(transportId)!;
      const videoConsumer = await webRtcTransport.consume({
        producerId: producer!.id,
        rtpCapabilities,
        paused: true,
      });

      if (!this.videoConsumers.has(webRtcTransport.id)) {
        this.videoConsumers.set(webRtcTransport.id, []);
      }
      this.videoConsumers.get(webRtcTransport.id)!.push(videoConsumer);

      return {
        id: videoConsumer.id,
        producerId: videoConsumer.producerId,
        kind: videoConsumer.kind,
        rtpParameters: videoConsumer.rtpParameters,
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
    const consumers = this.videoConsumers.get(transportId);
    if (consumers) {
      const consumer = consumers.find((c) => c.id === consumerId);
      await consumer!.resume();
    }
  }
}

export { Hub };
