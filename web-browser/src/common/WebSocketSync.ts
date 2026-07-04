import { v4 as uuidv4 } from "uuid";

class WebSocketSync extends WebSocket {
  private _onmessage: ((this: WebSocket, ev: any) => any) | null = null;
  private pendingRequests = new Map<string, (value: any) => void>();

  constructor(url: string) {
    super(url);
    this.addOnMessage();
  }

  private addOnMessage() {
    super.onmessage = (event) => {
      const message = JSON.parse(event.data);
      if (message.id && this.pendingRequests.has(message.id)) {
        const resolve = this.pendingRequests.get(message.id)!;
        this.pendingRequests.delete(message.id);
        resolve(message.data);
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

  public sendSync(message: any): Promise<any> {
    return new Promise((resolve) => {
      const id = uuidv4();
      this.pendingRequests.set(id, resolve);
      this.send(JSON.stringify({ id, data: message }));
    });
  }
}

export { WebSocketSync };
