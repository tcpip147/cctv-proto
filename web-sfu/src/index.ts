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
    { id: "video0", ip: "127.0.0.1", port: 25000, enableSctp: true, videoCodecs },
    { id: "video1", ip: "127.0.0.1", port: 25001, enableSctp: true, videoCodecs },
    { id: "video2", ip: "127.0.0.1", port: 25002, enableSctp: true, videoCodecs },
    { id: "video3", ip: "127.0.0.1", port: 25003, enableSctp: true, videoCodecs },
    { id: "video4", ip: "127.0.0.1", port: 25004, enableSctp: true, videoCodecs },
    { id: "video5", ip: "127.0.0.1", port: 25005, enableSctp: true, videoCodecs },
    { id: "video6", ip: "127.0.0.1", port: 25006, enableSctp: true, videoCodecs },
    { id: "video7", ip: "127.0.0.1", port: 25007, enableSctp: true, videoCodecs },
    { id: "video8", ip: "127.0.0.1", port: 25008, enableSctp: true, videoCodecs },
    { id: "video9", ip: "127.0.0.1", port: 25009, enableSctp: true, videoCodecs },
    { id: "video10", ip: "127.0.0.1", port: 25010, enableSctp: true, videoCodecs },
    { id: "video11", ip: "127.0.0.1", port: 25011, enableSctp: true, videoCodecs },
    { id: "video12", ip: "127.0.0.1", port: 25012, enableSctp: true, videoCodecs },
    { id: "video13", ip: "127.0.0.1", port: 25013, enableSctp: true, videoCodecs },
    { id: "video14", ip: "127.0.0.1", port: 25014, enableSctp: true, videoCodecs },
    { id: "video15", ip: "127.0.0.1", port: 25015, enableSctp: true, videoCodecs },
    { id: "video16", ip: "127.0.0.1", port: 25016, enableSctp: true, videoCodecs },
    { id: "video17", ip: "127.0.0.1", port: 25017, enableSctp: true, videoCodecs },
    { id: "video18", ip: "127.0.0.1", port: 25018, enableSctp: true, videoCodecs },
    { id: "video19", ip: "127.0.0.1", port: 25019, enableSctp: true, videoCodecs },
  ],
});

topology.createHub({
  id: "hub1",
  role: ["consumer"],
  consumerOption: {
    port: 40000,
  },
});

topology.createPipe("hub0", "hub1");

await topology.start();

const rtspRequests = [];

for (let i = 0; i < 12; i++) {
  const cctv = String(i + 1).padStart(2, "0");
  const port = String(i).padStart(2, "0");
  rtspRequests.push({
    rtspUrl: `rtsp://210.99.70.120:1935/live/cctv0${cctv}.stream`,
    rtpUrl: `rtp://127.0.0.1:250${port}?pkt_size=1200`,
  });
}

const rtsp = Rtsp.create(rtspRequests);

await rtsp.start({
  groupSize: 1,
});

const server = new ApiServer(3000, topology);
server.start();

process.on("SIGINT", () => process.exit(0));
process.on("SIGTERM", () => process.exit(0));
process.on("SIGHUP", () => process.exit(0));
