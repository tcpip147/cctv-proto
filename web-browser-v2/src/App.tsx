import { Device } from "mediasoup-client";
import { useEffect, useRef } from "react";
import "./App.css";
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

    ws.onopen = async () => {
      const mediaSession = ws.sendSync(
        JSON.stringify({ type: "createMediaSession" }),
      );
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
