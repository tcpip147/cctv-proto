import { PipeTransport, Producer } from "mediasoup/types";

abstract class MediaWorker {
  private pipeProducers: Producer[] = [];

  public abstract getId(): string;

  public abstract createPipeTransport(): Promise<PipeTransport>;

  public addPipeProducer(producer: Producer) {
    this.pipeProducers.push(producer);
  }

  public getPipeProducers() {
    return this.pipeProducers;
  }
}

export { MediaWorker };
