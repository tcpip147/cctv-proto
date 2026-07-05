import { RouterRtpCodecCapability, RtpCodecParameters } from "mediasoup/types";
import { ConsumerWorker } from "./consumer-worker.js";
import { MediaCluster } from "./media-cluster.js";
import { ProducerWorker } from "./producer-worker.js";

const portRange: [number, number] = [40000, 40100];

const availableCodecs: RouterRtpCodecCapability[] = [
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
];

const videoCodecs: RtpCodecParameters[] = [
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
];

const producer0 = await ProducerWorker.create({
  id: "producer-0",
  useCore: 2,
  portRange,
  availableCodecs,
  producerOptions: [
    { ip: "127.0.0.1", port: 25000, videoCodecs },
    { ip: "127.0.0.1", port: 25001, videoCodecs },
  ],
});

const consumer0 = await ConsumerWorker.create({
  id: "consumer-0",
  useCore: 4,
  portRange,
  availableCodecs,
});

const consumer1 = await ConsumerWorker.create({
  id: "consumer-1",
  useCore: 6,
  portRange,
  availableCodecs,
});

const consumer2 = await ConsumerWorker.create({
  id: "consumer-2",
  useCore: 8,
  portRange,
  availableCodecs,
});

const cluster = MediaCluster.getInstance();
cluster.registerWorker(producer0);
cluster.registerWorker(consumer0);
cluster.registerWorker(consumer1);
cluster.registerWorker(consumer2);

const worker = cluster.getFreeWorker();
