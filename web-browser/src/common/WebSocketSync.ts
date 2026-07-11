import { v4 as uuidv4 } from "uuid";

interface WebSocketRequest {
  type: string;
  payload?: { [key: string]: any };
}

interface WebSocketResponse {
  payload?: { [key: string]: any };
  error?: { [key: string]: any };
}

class WebSocketSync extends WebSocket {
  private _onmessage: ((this: WebSocket, ev: any) => any) | null = null;
  private pendingRequests = new Map<
    string,
    { resolve: (value: any) => void; reject: (reason: any) => void }
  >();

  constructor(url: string) {
    super(url);
    this.addOnMessage();
  }

  private addOnMessage() {
    super.onmessage = (event) => {
      const message = JSON.parse(event.data);
      if (message.requestId && this.pendingRequests.has(message.requestId)) {
        const { resolve, reject } = this.pendingRequests.get(
          message.requestId,
        )!;
        this.pendingRequests.delete(message.requestId);
        if (message.error) {
          reject(message.error);
        } else {
          resolve(message.payload);
        }
      }

      if (this._onmessage) {
        this._onmessage.call(this, event);
      }
    };
  }

  public set onmessage(callback: ((this: WebSocket, ev: any) => any) | null) {
    this._onmessage = callback;
    this.addOnMessage();
  }

  public get onmessage() {
    return this._onmessage;
  }

  public sendSync(message: WebSocketRequest): Promise<WebSocketResponse> {
    return new Promise((resolve, reject) => {
      const requestId = uuidv4();
      this.pendingRequests.set(requestId, { resolve, reject });
      this.send(JSON.stringify({ requestId, ...message }));
    });
  }
}

export { WebSocketSync };
