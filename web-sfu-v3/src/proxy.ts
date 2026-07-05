import dgram from "dgram";
import logger from "./logger.js";

class Proxy {
  private srcSocket!: dgram.Socket;
  private destSocket!: dgram.Socket;

  constructor(
    private srcHost: string,
    private srcPort: number,
    private destHost: string,
    private destPort: number,
  ) {}

  public start() {
    return new Promise<void>((resolve, reject) => {
      this.srcSocket = dgram.createSocket("udp4");
      this.destSocket = dgram.createSocket("udp4");
      this.srcSocket.bind(this.srcPort, this.srcHost, () => {
        logger.info(
          `Proxy started : ${this.srcHost}:${this.srcPort} -> ${this.destHost}:${this.destPort}`,
        );
        resolve();
      });
      this.srcSocket.on("error", (err) => {
        logger.error(
          `Proxy start error : ${this.srcHost}:${this.srcPort} -> ${this.destHost}:${this.destPort}`,
        );
        reject(err);
      });
      this.srcSocket.on("message", (msg) => {
        this.destSocket.send(msg, this.destPort, this.destHost);
      });
    });
  }

  public stop() {
    this.srcSocket.close();
    this.destSocket.close();
    logger.info(
      `Proxy stopped : ${this.srcHost}:${this.srcPort} -> ${this.destHost}:${this.destPort}`,
    );
  }
}

export { Proxy };

