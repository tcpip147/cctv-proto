import http, { Server } from "http";
import "reflect-metadata";
import WebSocket, { WebSocketServer } from "ws";
import { Topology } from "./topology.js";
import logger from "./logger.js";

class WebSocketHandler {
  private topology: Topology;

  constructor(topology: Topology) {
    this.topology = topology;
  }

  @OnMessage("getLeastLoadedConsumerHub")
  async getLeastLoadedConsumerHub(ws: WebSocket, message: WebSocketRequest) {
    const hub = this.topology.getLeastLoadedConsumerHub();
    if (!hub) {
      throw new Error("hub is null");
    }
    sendMessage(ws, {
      requestId: message.requestId,
      payload: { hubId: hub?.id },
    });
  }

  @OnMessage("getRouterRtpCapabilities")
  async getRouterRtpCapabilities(ws: WebSocket, message: WebSocketRequest) {
    const hub = this.topology.getHub(message.payload!.hubId);
    const routerRtpCapabilities = hub?.getRouterRtpCapabilities();
    if (!routerRtpCapabilities) {
      throw new Error("routerRtpCapabilities is null");
    }
    sendMessage(ws, {
      requestId: message.requestId,
      payload: routerRtpCapabilities,
    });
  }

  @OnMessage("createWebRtcTransport")
  async createWebRtcTransport(ws: WebSocket, message: WebSocketRequest) {
    const hub = this.topology.getHub(message.payload!.hubId);
    const webRtcTransport = await hub?.createWebRtcTransport()!;
    if (!webRtcTransport) {
      throw new Error("webRtcTransport is null");
    }
    sendMessage(ws, {
      requestId: message.requestId,
      payload: {
        requestId: message.requestId,
        id: webRtcTransport.id,
        iceParameters: webRtcTransport.iceParameters,
        iceCandidates: webRtcTransport.iceCandidates,
        dtlsParameters: webRtcTransport.dtlsParameters,
      },
    });
  }

  @OnMessage("createConsumer")
  async createConsumer(ws: WebSocket, message: WebSocketRequest) {
    const hub = this.topology.getHub(message.payload!.hubId);
    const consumer = await hub?.createConsumer({
      videoId: message.payload!.videoId,
      transportId: message.payload!.transportId,
      rtpCapabilities: message.payload!.rtpCapabilities,
    });
    if (!consumer) {
      throw new Error("consumer is null");
    }
    sendMessage(ws, {
      requestId: message.requestId,
      payload: {
        id: consumer!.id,
        producerId: consumer!.producerId,
        kind: consumer!.kind,
        rtpParameters: consumer!.rtpParameters,
      },
    });
  }

  @OnMessage("connectWebRtcTransport")
  async connectWebRtcTransport(ws: WebSocket, message: WebSocketRequest) {
    const hub = this.topology.getHub(message.payload!.hubId);
    await hub?.connectWebRtcTransport({
      transportId: message.payload!.transportId,
      dtlsParameters: message.payload!.dtlsParameters,
    });
    sendMessage(ws, {
      requestId: message.requestId,
    });
  }

  @OnMessage("resumeConsumer")
  async resumeConsumer(ws: WebSocket, message: WebSocketRequest) {
    const hub = this.topology.getHub(message.payload!.hubId);
    await hub?.resumeConsumer({
      consumerId: message.payload!.consumerId,
    });
    sendMessage(ws, {
      requestId: message.requestId,
    });
  }
}

interface WebSocketRequest {
  requestId: string;
  type: string;
  payload?: { [key: string]: any };
}

interface WebSocketResponse {
  requestId: string;
  payload?: { [key: string]: any };
  error?: { [key: string]: any };
}

class ApiServer {
  private server!: Server;
  private wss!: WebSocketServer;
  private handler!: WebSocketHandler;
  private port: number;

  constructor(port: number, topology: Topology) {
    this.port = port;
    this.handler = new WebSocketHandler(topology);
  }

  public async start() {
    this.server = http.createServer();
    this.wss = new WebSocketServer({ server: this.server });

    const prototype = Object.getPrototypeOf(this.handler);
    const messageRoutes = new Map<string, Function>();
    Object.getOwnPropertyNames(prototype).forEach((methodName) => {
      const action = Reflect.getMetadata("OnMessage", prototype, methodName);
      if (action) {
        messageRoutes.set(action, prototype[methodName].bind(this.handler));
      }
    });

    this.wss.on("connection", (ws) => {
      ws.on("message", async (data) => {
        const message: WebSocketRequest = JSON.parse(data.toString());
        const messageRoute = messageRoutes.get(message.type)!;
        if (messageRoute) {
          try {
            await messageRoute(ws, message);
            return;
          } catch (err) {
            logger.error(err);
          }
        }

        sendMessage(ws, {
          requestId: message.requestId,
          error: { message: "Error" },
        });
      });
    });

    this.server.listen(this.port, () => {
      logger.info(`WebSocket server is listening on port ${this.port}`);
    });
  }
}

function OnMessage(action: string) {
  return function (target: any, propertyKey: string) {
    Reflect.defineMetadata("OnMessage", action, target, propertyKey);
  };
}

function sendMessage(ws: WebSocket, message: WebSocketResponse) {
  ws.send(
    JSON.stringify({
      requestId: message.requestId,
      payload: message.payload,
      error: message.error,
    }),
  );
}

export { ApiServer };
