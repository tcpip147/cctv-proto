import { PipeTransport } from "mediasoup/types";
import logger from "./logger.js";
import { MediaWorker } from "./media-worker.js";

class MediaCluster {
  private static instance: MediaCluster;

  private linked = new Set<string>();
  private workers: MediaWorker[] = [];

  private constructor() {}

  public static getInstance(): MediaCluster {
    if (!MediaCluster.instance) {
      MediaCluster.instance = new MediaCluster();
    }
    return MediaCluster.instance;
  }

  public async registerWorker(worker: MediaWorker) {
    this.workers.push(worker);
    await this.validateBroadcast();
  }

  private async validateBroadcast() {
    for (const workerA of this.workers) {
      for (const workerB of this.workers) {
        const idA = workerA.getId();
        const idB = workerB.getId();
        if (idA === idB) {
          continue;
        }
        if (!workerA.canBroadcast() && !workerB.canBroadcast()) {
          continue;
        }
        const minId = idA < idB ? idA : idB;
        const maxId = idA > idB ? idA : idB;
        if (this.linked.has(minId + ":" + maxId)) {
          continue;
        }
        this.linked.add(minId + ":" + maxId);
        const transportA = await workerA.createPipeTransport();
        const transportB = await workerB.createPipeTransport();
        await Promise.all([
          transportA.connect({
            ip: transportB.tuple.localIp,
            port: transportB.tuple.localPort,
          }),
          transportB.connect({
            ip: transportA.tuple.localIp,
            port: transportA.tuple.localPort,
          }),
        ]);
        logger.info(
          `Bridge created between ${workerA.getId()} and ${workerB.getId()}`,
        );

        if (workerA.canBroadcast()) {
          await this.openPipe(workerA, transportA, workerB, transportB);
        }
        if (workerB.canBroadcast()) {
          await this.openPipe(workerB, transportB, workerA, transportA);
        }
      }
    }
  }

  private async openPipe(
    workerA: MediaWorker,
    transportA: PipeTransport,
    workerB: MediaWorker,
    transportB: PipeTransport,
  ) {
    const producers = workerA.getProducers();
    for (const producer of producers.values()) {
      const pipeConsumer = await transportA.consume({
        producerId: producer.id,
      });
      const pipeProducer = await transportB.produce({
        id: producer.id,
        kind: pipeConsumer.kind,
        rtpParameters: pipeConsumer.rtpParameters,
      });
      workerB.addPipeProducer(pipeProducer);
    }
  }

  private async createMediaSession() {
    return new MediaSession();
  }
}

export { MediaCluster };
