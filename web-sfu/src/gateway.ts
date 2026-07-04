import http, { Server } from "http";
import { WebSocketServer } from "ws";
import "reflect-metadata";
import router from "./router";

class WebSocketHandler {
  @OnMessage("createWebRtcTransport")
  async createWebRtcTransport(ws: WebSocket, message: any) {
    const transport = await router.createWebRtcTransport();
    ws.send(
      JSON.stringify({
        id: message.id,
        type: "createdWebRtcTransport",
        data: transport,
      }),
    );
  }

  @OnMessage("createConsumer")
  async createConsumer(ws: WebSocket, message: any) {
    const consumer = await router.createConsumer(
      message.data.video,
      message.data.transportId,
      message.data.rtpCapabilities,
    );

    ws.send(
      JSON.stringify({
        id: message.id,
        type: "createdConsumer",
        data: {
          id: consumer.id,
          producerId: consumer.producerId,
          kind: consumer.kind,
          rtpParameters: consumer.rtpParameters,
        },
      }),
    );
  }

  @OnMessage("connectionCallback")
  async connectionCallback(ws: WebSocket, message: any) {
    await router.connect(message.data.transportId, message.data.dtlsParameters);
    ws.send(
      JSON.stringify({
        id: message.id,
        type: "connectedCallback",
      }),
    );
  }

  @OnMessage("resumeConsumer")
  async resumeConsumer(ws: WebSocket, message: any) {
    await router.resumeConsumer(message.data.consumerId);
    ws.send(
      JSON.stringify({
        id: message.id,
        type: "resumedConsumer",
      }),
    );
  }
}

interface WebSocketMessage {
  type: string;
  data: any;
}

class Gateway {
  private server!: Server;
  private wss!: WebSocketServer;
  private handler!: WebSocketHandler;

  async init() {
    this.server = http.createServer();
    this.wss = new WebSocketServer({ server: this.server });
    this.handler = new WebSocketHandler();

    const prototype = Object.getPrototypeOf(this.handler);
    const messageRoutes = new Map<string, Function>();
    Object.getOwnPropertyNames(prototype).forEach((methodName) => {
      const action = Reflect.getMetadata("OnMessage", prototype, methodName);
      if (action) {
        messageRoutes.set(action, prototype[methodName].bind(this.handler));
      }
    });

    this.wss.on("connection", (ws) => {
      router.getRouterRtpCapabilities().then((capabilities) => {
        ws.send(
          JSON.stringify({ type: "routerRtpCapabilities", data: capabilities }),
        );
      });

      ws.on("message", (data) => {
        const message: WebSocketMessage = JSON.parse(data.toString());
        if (message.data) {
          const messageRoute = messageRoutes.get(message.data.type)!;
          if (messageRoute) {
            messageRoute(ws, message);
            return;
          }
        }
        console.error("No type matched");
      });

      ws.on("close", () => {});
    });
    this.server.listen(3000, () => {
      console.log("WebSocket server is listening on port 3000");
    });
  }
}

function OnMessage(action: string) {
  return function (target: any, propertyKey: string) {
    Reflect.defineMetadata("OnMessage", action, target, propertyKey);
  };
}

const gateway = new Gateway();

export default gateway;
