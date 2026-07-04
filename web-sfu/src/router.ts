import * as mediasoup from "mediasoup";
import {
  Consumer,
  Producer,
  Router,
  WebRtcTransport,
  Worker,
} from "mediasoup/types";
import dgram from "dgram";

class MediaRouter {
  private worker!: Worker;
  private router!: Router;

  private producers = new Map<number, Producer>();
  private webRtcTransports = new Map<string, WebRtcTransport>();
  private consumers = new Map<string, Consumer>();

  async init() {
    this.worker = await mediasoup.createWorker({
      rtcMinPort: 40000,
      rtcMaxPort: 50000,
    });
    this.router = await this.worker.createRouter({
      mediaCodecs: [
        {
          kind: "video",
          mimeType: "video/H264",
          clockRate: 90000,
          rtcpFeedback: [
            { type: "nack" },
            { type: "nack", parameter: "pli" },
            { type: "transport-cc" },
          ],
          parameters: {
            "packetization-mode": 1,
            "profile-level-id": "42e01f",
            "level-asymmetry-allowed": 1,
          },
        },
      ],
    });
  }

  async listenProducer(host: string, port: number) {
    const nicHost = "127.0.0.1";
    const transport = await this.router.createPlainTransport({
      listenIp: { ip: nicHost },
      rtcpMux: true,
      comedia: true,
    });
    await setProxy(host, port, nicHost, transport.tuple.localPort);

    /*
    setInterval(async () => {
      const stats = await transport.getStats();
      console.log(stats[0].bytesReceived);
    }, 3000);
    */

    const producer = await transport.produce({
      kind: "video",
      rtpParameters: {
        codecs: [
          {
            mimeType: "video/H264",
            payloadType: 96,
            clockRate: 90000,
            rtcpFeedback: [
              { type: "nack" },
              { type: "nack", parameter: "pli" },
              { type: "transport-cc" },
            ],
            parameters: {
              "packetization-mode": 1,
              "profile-level-id": "42e01f",
              "level-asymmetry-allowed": 1,
            },
          },
        ],
        encodings: [
          {
            ssrc: Number("1000" + port),
          },
        ],
      },
    });

    this.producers.set(port, producer);

    /*
    setInterval(async () => {
      const stats = await producer.getStats();
      console.log(stats);
    }, 3000);
    */
  }

  async getRouterRtpCapabilities() {
    return this.router.rtpCapabilities;
  }

  async createWebRtcTransport() {
    const transport = await this.router.createWebRtcTransport({
      listenIps: [
        {
          ip: "127.0.0.1",
        },
      ],
      enableUdp: true,
      enableTcp: true,
      preferUdp: true,
    });

    this.webRtcTransports.set(transport.id, transport);

    return {
      id: transport.id,
      iceParameters: transport.iceParameters,
      iceCandidates: transport.iceCandidates,
      dtlsParameters: transport.dtlsParameters,
    };
  }

  async createConsumer(
    port: number,
    transportId: string,
    rtpCapabilities: any,
  ) {
    const producer = this.producers.get(port);
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
    return {};
  }

  async connect(transportId: string, dtlsParameters: any) {
    const transport = this.webRtcTransports.get(transportId);
    await transport!.connect({ dtlsParameters });
  }

  async resumeConsumer(consumerId: string) {
    const consumer = this.consumers.get(consumerId);
    await consumer!.resume();
  }
}

async function setProxy(
  srcHost: string,
  srcPort: number,
  destHost: string,
  destPort: number,
) {
  return new Promise<void>((resolve, reject) => {
    const srcSocket = dgram.createSocket("udp4");
    const destSocket = dgram.createSocket("udp4");
    srcSocket.bind(srcPort, srcHost, () => {
      console.log(
        `Proxy started : ${srcHost}:${srcPort} -> ${destHost}:${destPort}`,
      );
      resolve();
    });
    srcSocket.on("error", (err) => {
      console.error(
        `Proxy start error : ${srcHost}:${srcPort} -> ${destHost}:${destPort}`,
      );
      reject(err);
    });
    srcSocket.on("message", (msg) => {
      destSocket.send(msg, destPort, destHost);
    });
  });
}

const router = new MediaRouter();

export default router;
