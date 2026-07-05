import http, { Server } from "http";
import "reflect-metadata";
import { WebSocketServer } from "ws";

interface WebSocketMessage {
  id: string;
  type: string;
  data: any;
}

class WebSocketHandler {
  @OnMessage("createMediaSession")
  async createMediaSession(ws: WebSocket, message: any) {
    console.log(message);
  }
}

class ApiServer {
  private server!: Server;
  private wss!: WebSocketServer;
  private handler!: WebSocketHandler;
  private port: number;

  constructor(port: number) {
    this.port = port;
  }

  public async start() {
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
      ws.on("message", (data) => {
        const message: WebSocketMessage = JSON.parse(data.toString());
        const messageRoute = messageRoutes.get(message.type)!;
        if (messageRoute) {
          messageRoute(ws, message);
          return;
        }
        console.error("No type matched");
      });
    });

    this.server.listen(this.port, () => {
      console.log(`WebSocket server is listening on port ${this.port}`);
    });
  }
}

function OnMessage(action: string) {
  return function (target: any, propertyKey: string) {
    Reflect.defineMetadata("OnMessage", action, target, propertyKey);
  };
}

export { ApiServer };
