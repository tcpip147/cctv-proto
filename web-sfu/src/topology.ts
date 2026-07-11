import { RouterRtpCodecCapability, RtpCodecParameters } from "mediasoup/types";
import { Hub } from "./hub.js";

interface TopologyOptions {
  portRange: [number, number];
  availableCodecs: RouterRtpCodecCapability[];
}

interface HubOptions {
  id: string;
}

interface ProducerHubOptions extends HubOptions {
  role: ["producer", "consumer"] | ["consumer", "producer"] | ["producer"];
  producerOptions: ProducerOptions[];
}

interface ConsumerHubOptions extends HubOptions {
  role: ["consumer"];
}

interface ProducerOptions {
  id: string;
  ip: string;
  port: number;
  videoCodecs: RtpCodecParameters[];
}

class Topology {
  private portRange: [number, number];
  private availableCodecs: RouterRtpCodecCapability[];
  private hubs = new Map<string, Hub>();
  private pipes: [string, string][] = [];

  public static create(options: TopologyOptions) {
    return new Topology(options);
  }

  constructor(options: TopologyOptions) {
    this.portRange = options.portRange;
    this.availableCodecs = options.availableCodecs;
  }

  public createHub(options: ProducerHubOptions | ConsumerHubOptions) {
    const hub = Hub.create({
      ...options,
      portRange: this.portRange,
      availableCodecs: this.availableCodecs,
    });
    if (this.hubs.has(hub.id)) {
      throw new Error(`Hub ${hub.id} already exists.`);
    }
    this.hubs.set(hub.id, hub);
  }

  public createPipe(fromId: string, toId: string) {
    if (!this.hubs.has(fromId)) {
      throw new Error(`Hub ${fromId} not found.`);
    }
    if (!this.hubs.has(toId)) {
      throw new Error(`Hub ${toId} not found.`);
    }
    if (this.pipes.some((pipe) => pipe[0] === fromId && pipe[1] === toId)) {
      throw new Error(`Pipe from ${fromId} to ${toId} already exists.`);
    }
    this.pipes.push([fromId, toId]);
  }

  public async start() {
    for (const hub of this.hubs.values()) {
      await hub.start();
      hub.removeOnCloseListener(this.onCloseHub.bind(this));
      hub.addOnCloseListener(this.onCloseHub.bind(this));
    }
    for (const [fromId, toId] of this.pipes) {
      const fromHub = this.hubs.get(fromId)!;
      const toHub = this.hubs.get(toId)!;
      await fromHub.pipeToRouter(toHub);
    }
  }

  private async onCloseHub(hubId: string) {
    try {
      for (const hub of this.hubs.values()) {
        if (hub.id !== hubId) {
          hub.removePipe(hubId);
        }
      }
      await this.recoveryHub(hubId);
    } catch (err) {
      throw err;
    }
  }

  private async recoveryHub(hubId: string) {
    const hub = this.hubs.get(hubId)!;
    await hub.start();
    for (const [fromId, toId] of this.pipes) {
      if (fromId === hubId || toId === hubId) {
        const fromHub = this.hubs.get(fromId)!;
        const toHub = this.hubs.get(toId)!;
        await fromHub.pipeToRouter(toHub);
      }
    }
  }

  public getHub(id: string) {
    return this.hubs.get(id);
  }

  public getLeastLoadedConsumerHub() {
    let min = Number.MAX_SAFE_INTEGER;
    let leastLoadedHub: Hub | null = null;
    for (const hub of this.hubs.values()) {
      if (hub.isStopped || !hub.role.includes("consumer")) {
        continue;
      }
      if (hub.getConsumerCount() < min) {
        min = hub.getConsumerCount();
        leastLoadedHub = hub;
      }
    }
    return leastLoadedHub;
  }
}

export { Topology };
