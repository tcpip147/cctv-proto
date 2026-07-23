import { RtpCodecParameters } from "mediasoup/types";
import { ApiServer } from "./api-server.js";
import logger from "./logger.js";
import { Topology } from "./topology.js";
import { Rtsp } from "./rtsp.js";

logger.info("SFU Streaming Server Starting...");

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

const topology = Topology.create({
  publicIp: "127.0.0.1",
  portRange: [40000, 40100],
  availableCodecs: [
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

topology.createHub({
  id: "hub0",
  role: ["producer"],
  producerOptions: [
    { id: "video0", ip: "127.0.0.1", port: 25000, videoCodecs },
    { id: "video1", ip: "127.0.0.1", port: 25001, videoCodecs },
  ],
});

topology.createHub({
  id: "hub1",
  role: ["consumer"],
  consumerOption: {
    port: 40000,
  },
});

topology.createHub({
  id: "hub2",
  role: ["consumer"],
  consumerOption: {
    port: 40001,
  },
});

topology.createPipe("hub0", "hub1");
topology.createPipe("hub0", "hub2");

await topology.start();

const rtsp = Rtsp.create([
  {
    rtspUrl: "rtsp://210.99.70.120:1935/live/cctv001.stream",
    rtpUrl: "rtp://127.0.0.1:25000?pkt_size=1200",
  },
  {
    rtspUrl: "rtsp://210.99.70.120:1935/live/cctv002.stream",
    rtpUrl: "rtp://127.0.0.1:25001?pkt_size=1200",
  },
]);

await rtsp.start({
  groupSize: 12,
});

const server = new ApiServer(3000, topology);
server.start();

process.on("SIGINT", () => process.exit(0));
process.on("SIGTERM", () => process.exit(0));
process.on("SIGHUP", () => process.exit(0));
