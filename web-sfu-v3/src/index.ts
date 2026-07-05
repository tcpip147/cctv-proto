import { RouterRtpCodecCapability, RtpCodecParameters } from "mediasoup/types";
import logger from "./logger.js";
import { MediaWorker } from "./media-worker.js";
import { MediaCluster } from "./media-cluster.js";
import { ApiServer } from "./api-server.js";

logger.info("SFU Streaming Server Starting...");

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

const portRange: [number, number] = [40000, 40100];

const worker0 = await MediaWorker.create({
  id: "worker-0",
  useCore: 2,
  role: ["producer", "consumer"],
  portRange,
  availableCodecs,
  producerOptions: [
    { ip: "127.0.0.1", port: 25000, videoCodecs },
    { ip: "127.0.0.1", port: 25001, videoCodecs },
  ],
  broadcast: true,
});

const worker1 = await MediaWorker.create({
  id: "worker-1",
  useCore: 4,
  role: ["consumer"],
  portRange,
  availableCodecs,
});

const worker2 = await MediaWorker.create({
  id: "worker-2",
  useCore: 6,
  role: ["consumer"],
  portRange,
  availableCodecs,
});

const cluster = MediaCluster.getInstance();
cluster.registerWorker(worker0);
cluster.registerWorker(worker1);
cluster.registerWorker(worker2);

new ApiServer(3000).start();
