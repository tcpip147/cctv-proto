import { useEffect, useRef } from "react";
import { useWebSocket } from "./useWebSocket";

function useMediaConnection(videos: string[]) {
  const hubIdRef = useRef<string>("");
  const { reconnect } = useWebSocket("ws://localhost:8080/signal");

  useEffect(() => {}, [videos]);
}

export { useMediaConnection };
