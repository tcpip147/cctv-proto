import { Device } from "mediasoup-client";
import { useEffect, useRef, useState } from "react";
import "./App.css";
import { WebSocketSync } from "./common/WebSocketSync";
import type {
  RtpCapabilities,
  Transport,
  TransportOptions,
} from "mediasoup-client/types";

const device = new Device();
const cctvList = Array.from({ length: 2 }, (_, i) => `video${i}`);
let hubId: string;
let recvTransport: Transport;

function App() {
  const videoRefs = useRef<{ [key: string]: HTMLVideoElement | null }>({});
  const socketRef = useRef<WebSocket | null>(null);
  const [isConnectedWebRtc, setIsConnectedWebRtc] = useState(false);

  useEffect(() => {
    connectWebSocket();
    return () => {
      if (socketRef.current) {
        socketRef.current.close();
      }
    };
  }, []);

  const connectWebRtc = async (ws: WebSocketSync) => {
    hubId = (
      (await ws.sendSync({
        type: "getLeastLoadedConsumerHub",
      })) as { hubId: string }
    ).hubId;

    const routerRtpCapabilities = (await ws.sendSync({
      type: "getRouterRtpCapabilities",
      payload: { hubId },
    })) as RtpCapabilities;

    await device.load({
      routerRtpCapabilities: routerRtpCapabilities,
    });

    const webRtcTransport = (await ws.sendSync({
      type: "createWebRtcTransport",
      payload: { hubId },
    })) as TransportOptions;

    recvTransport = device.createRecvTransport(webRtcTransport) as Transport;
    recvTransport.on("connect", async ({ dtlsParameters }, callback) => {
      await ws.sendSync({
        type: "connectWebRtcTransport",
        payload: { hubId, transportId: recvTransport.id, dtlsParameters },
      });
      callback();
    });

    setIsConnectedWebRtc(true);
  };

  const consumeVideo = async (ws: WebSocketSync, videoId: string) => {
    const consumerInfo = (await ws.sendSync({
      type: "createConsumer",
      payload: {
        hubId,
        videoId,
        transportId: recvTransport.id,
        rtpCapabilities: device.recvRtpCapabilities,
      },
    })) as {
      id: string;
      producerId: string;
      kind: "video";
      rtpParameters: any;
    };

    const consumer = await recvTransport.consume({
      id: consumerInfo.id,
      producerId: consumerInfo.producerId,
      kind: consumerInfo.kind,
      rtpParameters: consumerInfo.rtpParameters,
    });

    const videoElement = videoRefs.current[videoId];
    if (videoElement) {
      const newStream = new MediaStream([consumer.track]);
      videoElement.srcObject = newStream;
      videoElement.play().catch(console.error);

      await ws.sendSync({
        type: "resumeConsumer",
        payload: {
          hubId,
          consumerId: consumer.id,
        },
      });
    }
  };

  const connectWebSocket = () => {
    const wsUrl = "ws://localhost:8080/signal";
    const ws = new WebSocketSync(wsUrl);
    socketRef.current = ws;

    ws.onopen = async () => {
      if (!isConnectedWebRtc) {
        await connectWebRtc(ws);
        for (const videoId of cctvList) {
          consumeVideo(ws, videoId);
        }
      }
    };

    ws.onmessage = async (event) => {
      const message = JSON.parse(event.data);
    };

    ws.onclose = async () => {
      setTimeout(() => {
        connectWebSocket();
      }, 3000);
    };

    ws.onerror = async () => {};
  };

  return (
    <div className="video-grid">
      {cctvList.map((cctvId) => (
        <video
          key={cctvId}
          ref={(el) => {
            videoRefs.current[cctvId] = el;
          }}
          autoPlay
          muted
          playsInline
          style={{ width: "200px", margin: "5px" }}
        />
      ))}
    </div>
  );
}

export default App;
