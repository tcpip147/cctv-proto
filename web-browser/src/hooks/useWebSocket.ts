import { useCallback, useEffect, useRef, useState } from "react";
import { v4 as uuidv4 } from "uuid";

interface UseWebSocketOptions {
  onOpen?: () => void;
  onMessage?: (event: MessageEvent) => void;
  onClose?: () => void;
  onError?: () => void;
}

interface UseWebSocketRequest {
  type: string;
  payload?: { [key: string]: any };
}

interface UseWebSocketResponse {
  payload?: { [key: string]: any };
  error?: { [key: string]: any };
}

interface PendingRequest {
  resolve: (val: any) => void;
  reject: (err: any) => void;
  timeout: any;
}

function useWebSocket(url: string, options?: UseWebSocketOptions) {
  const socketRef = useRef<WebSocket | null>(null);
  const optionsRef = useRef(options);
  const pendingRequestsRef = useRef<Map<string, PendingRequest>>(new Map());
  const [isConnected, setIsConnected] = useState(false);
  const [isConnecting, setIsConnecting] = useState(false);

  optionsRef.current = options;

  useEffect(() => {
    return () => {
      if (socketRef.current) {
        socketRef.current.close();
        socketRef.current = null;
      }
    };
  }, [url]);

  const connect = useCallback(() => {
    setIsConnecting(true);

    if (socketRef.current) {
      socketRef.current.onopen = null;
      socketRef.current.onmessage = null;
      socketRef.current.onerror = null;
      socketRef.current.onclose = null;
      socketRef.current.close();
    }

    const socket = new WebSocket(url);
    socketRef.current = socket;

    socket.onopen = () => {
      setIsConnected(true);
      setIsConnecting(false);
      optionsRef.current?.onOpen?.();
    };

    socket.onmessage = (event) => {
      const message = JSON.parse(event.data);
      if (
        message.requestId &&
        pendingRequestsRef.current.has(message.requestId)
      ) {
        const { resolve, reject, timeout } = pendingRequestsRef.current.get(
          message.requestId,
        )!;

        clearTimeout(timeout);

        if (message.error) {
          reject(message.error);
        } else {
          resolve(message.payload);
        }
        pendingRequestsRef.current.delete(message.requestId);
      }
      optionsRef.current?.onMessage?.(event);
    };

    socket.onclose = () => {
      setIsConnected(false);
      setIsConnecting(false);
      pendingRequestsRef.current.forEach(({ reject, timeout }) => {
        clearTimeout(timeout);
        reject(new Error("WebSocket is closed before receiving response"));
      });
      pendingRequestsRef.current.clear();

      optionsRef.current?.onClose?.();
    };

    socket.onerror = () => {
      setIsConnected(false);
      setIsConnecting(false);
      optionsRef.current?.onError?.();
    };
  }, []);

  const close = useCallback(() => {
    if (socketRef.current) {
      socketRef.current.close();
      socketRef.current = null;
    }
  }, []);

  const send = useCallback((data: string | BufferSource | Blob) => {
    if (socketRef.current && socketRef.current.readyState === WebSocket.OPEN) {
      socketRef.current.send(data);
    } else {
      console.warn("WebSocket is not connected");
    }
  }, []);

  const sendAndWait = useCallback(
    (message: UseWebSocketRequest): Promise<UseWebSocketResponse> => {
      return new Promise((resolve, reject) => {
        if (
          socketRef.current &&
          socketRef.current.readyState === WebSocket.OPEN
        ) {
          const requestId = uuidv4();

          const timeout = setTimeout(() => {
            if (pendingRequestsRef.current.has(requestId)) {
              pendingRequestsRef.current.delete(requestId);
              reject(
                new Error(`Timeout waiting for response from WebSocket server`),
              );
            }
          }, 5000);

          pendingRequestsRef.current.set(requestId, {
            resolve,
            reject,
            timeout,
          });

          socketRef.current.send(JSON.stringify({ requestId, ...message }));
        } else {
          reject(new Error("WebSocket is not connected"));
        }
      });
    },
    [],
  );

  return {
    connect,
    isConnected,
    isConnecting,
    close,
    send,
    sendAndWait,
  };
}

export { useWebSocket };
