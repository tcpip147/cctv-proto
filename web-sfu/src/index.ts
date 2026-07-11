import { RtpCodecParameters } from "mediasoup/types";
import { ApiServer } from "./api-server.js";
import logger from "./logger.js";
import { Topology } from "./topology.js";

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
});

topology.createHub({
  id: "hub2",
  role: ["consumer"],
});

topology.createPipe("hub0", "hub1");
topology.createPipe("hub0", "hub2");

await topology.start();

const server = new ApiServer(3000, topology);
server.start();



process.on("SIGINT", () => process.exit(0));
process.on("SIGTERM", () => process.exit(0));
process.on("SIGHUP", () => process.exit(0));
