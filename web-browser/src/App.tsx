import { useEffect, useRef } from "react";
import "./App.css";
import { Device } from "mediasoup-client";
import { WebSocketSync } from "./common/WebSocketSync";

function App() {
  const videoRefs = useRef<{ [key: string]: HTMLVideoElement | null }>({});
  const cctvList = Array.from({ length: 20 }, (_, i) => `cctv_${i + 1}`);

  const deviceRef = useRef<Device | null>(null);
  const socketRef = useRef<WebSocket | null>(null);
  useEffect(() => {
    connectWebSocket();
    return () => {
      if (socketRef.current) {
        socketRef.current.close();
      }
    };
  }, []);

  const connectWebSocket = () => {
    const wsUrl = "ws://localhost:3000";
    const ws = new WebSocketSync(wsUrl);
    socketRef.current = ws;

    ws.onopen = async () => {};

    ws.onmessage = async (event) => {
      const message = JSON.parse(event.data);
      if (message.type == "routerRtpCapabilities") {
        deviceRef.current = new Device();
        await deviceRef.current.load({
          routerRtpCapabilities: message.data,
        });
        const webRtcTransport = await ws.sendSync({
          type: "createWebRtcTransport",
        });
        const recvTransport =
          deviceRef.current?.createRecvTransport(webRtcTransport);
        recvTransport?.on(
          "connect",
          async ({ dtlsParameters }, callback) => {
            await ws.sendSync({
              type: "connectionCallback",
              transportId: recvTransport.id,
              dtlsParameters,
            });
            callback();
          },
        );

        for (let i = 0; i < 20; i++) {
          const consumerInfo = await ws.sendSync({
            type: "createConsumer",
            video: 25000 + i,
            transportId: recvTransport.id,
            rtpCapabilities: deviceRef.current.recvRtpCapabilities,
          });

          const consumer = await recvTransport.consume({
            id: consumerInfo.id,
            producerId: consumerInfo.producerId,
            kind: consumerInfo.kind,
            rtpParameters: consumerInfo.rtpParameters,
          });

          await ws.sendSync({
            type: "resumeConsumer",
            consumerId: consumer.id,
          });

          const videoElement = videoRefs.current["cctv_" + (i + 1)];
          if (videoElement) {
            const newStream = new MediaStream([consumer.track]);
            videoElement.srcObject = newStream;
            videoElement.play().catch(console.error);
          }
        }
      }
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
