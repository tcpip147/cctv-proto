import { useEffect, useRef, useState } from "react";
import { useWebSocket } from "./useWebSocket";
import { Device } from "mediasoup-client";
import type {
  RtpCapabilities,
  Transport,
  TransportOptions,
} from "mediasoup-client/types";

type StateFn = () => Promise<StateFn | null>;
type Event = "connectWebSocket" | "connectWebRtc";

class StateCoordinator {
  private queue: string[] = [];
  private processing: boolean = false;
  private events = new Map<string, StateFn>();

  constructor(events: { event: Event; state: StateFn }[]) {
    for (const { event, state } of events) {
      this.events.set(event, state);
    }
  }

  public start(event: Event) {
    this.queue.push(event);
    this.processQueue();
  }

  private async processQueue() {
    if (this.queue.length === 0) {
      return;
    }
    if (this.processing) {
      return;
    }
    this.processing = true;

    const event = this.queue.shift();
    if (event) {
      await this.handleEvent(event);
    }

    this.processing = false;
    this.processQueue();
  }

  private async handleEvent(event: string) {
    const stateFn = this.events.get(event);
    if (stateFn) {
      let current: StateFn | null = stateFn;
      while (current) {
        console.log(`[State] ${current.name}`);
        current = await current();
      }
    }
  }
}

function useMediasoupConnection(url: string, videoIds: string[]) {
  const stateCoordinatorRef = useRef<StateCoordinator | null>(null);
  const onOpenRef = useRef<() => void | null>(null);
  const onCloseRef = useRef<() => void | null>(null);
  const ws = useWebSocket(url, {
    onOpen: () => onOpenRef.current?.(),
    onClose: () => onCloseRef.current?.(),
  });

  const hubIdRef = useRef<string>("");
  const deviceRef = useRef<Device | null>(null);
  const recvTransportRef = useRef<Transport | null>(null);

  const [streams, setStreams] = useState<{ [key: string]: MediaStream | null }>(
    {},
  );

  useEffect(() => {
    let alive = true;

    async function webSocketConnectingState() {
      return new Promise<StateFn | null>((resolve) => {
        onOpenRef.current = () => resolve(webSocketConnectedState);
        onCloseRef.current = () => {
          setTimeout(() => {
            if (!alive) {
              return;
            }
            console.log("WebSocket closed, retrying connection...");
            resolve(null);
            stateCoordinatorRef.current!.start("connectWebSocket");
          }, 3000);
        };
        ws.connect();
      });
    }

    async function webSocketConnectedState() {
      return new Promise<StateFn | null>((resolve) => {
        if (hubIdRef.current) {
          console.log(
            "WebSocket is connected, but transport is already created.",
          );
          resolve(null);
          return;
        }
        resolve(transportCreatingState);
      });
    }

    async function transportCreatingState() {
      return new Promise<StateFn | null>(async (resolve) => {
        try {
          hubIdRef.current = (
            (await ws.sendAndWait({
              type: "getLeastLoadedConsumerHub",
            })) as { hubId: string }
          ).hubId;

          const routerRtpCapabilities = (await ws.sendAndWait({
            type: "getRouterRtpCapabilities",
            payload: { hubId: hubIdRef.current },
          })) as RtpCapabilities;

          deviceRef.current = new Device();
          await deviceRef.current.load({ routerRtpCapabilities });

          const webRtcTransport = (await ws.sendAndWait({
            type: "createWebRtcTransport",
            payload: { hubId: hubIdRef.current },
          })) as TransportOptions;

          if (recvTransportRef.current) {
            recvTransportRef.current.close();
          }

          recvTransportRef.current = deviceRef.current.createRecvTransport(
            webRtcTransport,
          ) as Transport;

          recvTransportRef.current.on(
            "connect",
            async ({ dtlsParameters }, callback, errorback) => {
              try {
                await ws.sendAndWait({
                  type: "connectWebRtcTransport",
                  payload: {
                    hubId: hubIdRef.current,
                    transportId: recvTransportRef.current!.id,
                    dtlsParameters,
                  },
                });
                callback();
              } catch (error: any) {
                errorback(error);
              }
            },
          );

          recvTransportRef.current.on("connectionstatechange", (state) => {
            if (state === "disconnected") {
              hubIdRef.current = "";
              console.log("Transport disconnected, retrying connection...");
              resolve(null);
              stateCoordinatorRef.current?.start("connectWebSocket");
            }
          });

          resolve(transportCreatedState);
        } catch (error) {
          hubIdRef.current = "";
          console.error("Error during transport creation: ", error);
          resolve(null);
          stateCoordinatorRef.current?.start("connectWebSocket");
        }
      });
    }

    async function transportCreatedState() {
      return new Promise<StateFn | null>(async (resolve) => {
        const streams: { [key: string]: MediaStream | null } = {};
        try {
          for (const videoId of videoIds) {
            const consumerInfo = (await ws.sendAndWait({
              type: "createVideoConsumer",
              payload: {
                hubId: hubIdRef.current,
                videoId,
                transportId: recvTransportRef.current!.id,
                rtpCapabilities: deviceRef.current!.recvRtpCapabilities,
              },
            })) as {
              id: string;
              producerId: string;
              kind: "video";
              rtpParameters: any;
            };

            const consumer = await recvTransportRef.current!.consume({
              id: consumerInfo.id,
              producerId: consumerInfo.producerId,
              kind: consumerInfo.kind,
              rtpParameters: consumerInfo.rtpParameters,
            });

            await ws.sendAndWait({
              type: "resumeConsumer",
              payload: {
                hubId: hubIdRef.current,
                transportId: recvTransportRef.current!.id,
                consumerId: consumer.id,
              },
            });

            const stream = new MediaStream([consumer.track]);
            streams[videoId] = stream;
          }
          setStreams(streams);
          resolve(null);
        } catch (error) {
          hubIdRef.current = "";
          console.error("Error during transport creation: ", error);
          resolve(null);
          stateCoordinatorRef.current?.start("connectWebRtc");
        }
      });
    }

    stateCoordinatorRef.current = new StateCoordinator([
      { event: "connectWebSocket", state: webSocketConnectingState },
      { event: "connectWebRtc", state: transportCreatingState },
    ]);

    stateCoordinatorRef.current.start("connectWebSocket");

    return () => {
      alive = false;
      onOpenRef.current = null;
      onCloseRef.current = null;
      ws.close();
      stateCoordinatorRef.current = null;
    };
  }, []);

  return {
    streams,
  };
}

export { useMediasoupConnection };
