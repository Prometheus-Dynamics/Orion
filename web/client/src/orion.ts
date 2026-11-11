// Orion in a browser.
//
// A page opens a WebSocket straight to a board's comm core and speaks the same frames as
// every other program: it subscribes to topics, publishes its own, and runs actions. There
// is no server in between and nothing here but this file.
//
//   const orion = await Orion.connect("ws://board.local:9300/orion", "panel");
//   const level = await orion.subscribe("tank.level", (sample) => draw(sample));
//   const result = await orion.run({ name: "pump.start", resource: "pump" });
//
// A browser cannot map a board's memory, so it is a subscriber and a publisher "without a
// ring": samples reach it, and leave it, as messages. The board sends a page only what the
// page's connection has room for, and after a loss skips to the next sample that stands on
// its own, so a slow page hurts nobody else.

/** The WebSocket subprotocol that carries Orion's frames. */
export const PROTOCOL = "orion.v1";

const PROTOCOL_VERSION = 1;
const INTERFACE_REVISION = 1;
const MAX_FRAME = 64 * 1024;

const enum Kind {
  Hello = 0x01,
  Welcome = 0x02,
  Reject = 0x03,
  Ping = 0x04,
  Pong = 0x05,
  Refusal = 0x06,
  ActionRun = 0x31,
  ActionProgress = 0x33,
  ActionResult = 0x34,
  ActionCancel = 0x35,
  TopicDeclare = 0x40,
  TopicOpened = 0x42,
  TopicSubscribe = 0x43,
  TopicUnsubscribe = 0x44,
  TopicSample = 0x45,
  TopicDemand = 0x47,
  TopicState = 0x4a,
}

/** The roles a page asks for: a client, and a provider so that it may publish. */
const ROLES = (1 << 0) | (1 << 1);

/** The type of a topic's field. */
export enum FieldType {
  Bool = 1,
  Int = 2,
  Float = 3,
  Text = 4,
  Bytes = 5,
  FloatArray = 6,
  IntArray = 7,
}

/** One field of a topic. A sample belongs to one, by its place in the list. */
export interface Field {
  name: string;
  type: FieldType;
  unit?: string;
  typeName?: string;
}

/** A topic as its publisher declared it. */
export interface Declaration {
  name: string;
  fields: Field[];
  history: number;
  expiryMs: number;
  maxPayload: number;
}

/** One sample of one field. */
export interface Sample {
  /** Which field, by its place in the declaration. */
  field: number;
  /** The time it belongs to, in nanoseconds. */
  time: bigint;
  /** Whether later samples depend on this one: a key frame. */
  othersDepend: boolean;
  payload: Uint8Array;
}

/** Where a topic is. */
export enum Status {
  Live = 1,
  PublisherGone = 2,
  Waiting = 3,
}

/** How an action ended. */
export enum Outcome {
  Succeeded = 1,
  Failed = 2,
  Rejected = 3,
  TimedOut = 4,
  Cancelled = 5,
}

/** An action to run: against a node or a resource, or this board if neither is said. */
export interface Action {
  name: string;
  node?: string;
  resource?: string;
  args?: Uint8Array;
  /** When to give up, in milliseconds from now. */
  deadlineMs?: number;
}

/** How an action is doing. */
export interface Progress {
  percent: number;
  message: string;
  data: Uint8Array;
}

/** How an action ended. */
export interface Result {
  outcome: Outcome;
  output: Uint8Array;
  detail: string;
}

/** A request the board refused, with its stable code. */
export class Refused extends Error {
  constructor(
    readonly code: number,
    detail: string,
  ) {
    super(detail || `refused (${code})`);
    this.name = "Refused";
  }
}

// --- The wire: postcard bodies in checksummed frames ---

const CRC_TABLE = (() => {
  const table = new Uint32Array(256);
  for (let index = 0; index < 256; index++) {
    let value = index;
    for (let bit = 0; bit < 8; bit++) {
      value = value & 1 ? (value >>> 1) ^ 0x82f63b78 : value >>> 1;
    }
    table[index] = value >>> 0;
  }
  return table;
})();

/** CRC-32C, as every frame ends with. */
export function crc32c(bytes: Uint8Array): number {
  let crc = 0xffffffff;
  for (const byte of bytes) {
    crc = CRC_TABLE[(crc ^ byte) & 0xff]! ^ (crc >>> 8);
  }
  return (crc ^ 0xffffffff) >>> 0;
}

const TEXT = new TextEncoder();
const UTF8 = new TextDecoder("utf-8", { fatal: true });

class Writer {
  private bytes: number[] = [];

  u8(value: number): this {
    this.bytes.push(value & 0xff);
    return this;
  }

  bool(value: boolean): this {
    return this.u8(value ? 1 : 0);
  }

  /** An unsigned integer of any width, as postcard writes them: seven bits a byte. */
  uint(value: number | bigint): this {
    let rest = BigInt(value);
    do {
      const low = Number(rest & 0x7fn);
      rest >>= 7n;
      this.bytes.push(rest === 0n ? low : low | 0x80);
    } while (rest !== 0n);
    return this;
  }

  raw(bytes: Uint8Array): this {
    for (const byte of bytes) this.bytes.push(byte);
    return this;
  }

  blob(bytes: Uint8Array): this {
    return this.uint(bytes.length).raw(bytes);
  }

  str(text: string): this {
    return this.blob(TEXT.encode(text));
  }

  /** A list: its length in bytes, then each element with its own length in front. */
  list<T>(items: T[], write: (writer: Writer, item: T) => void): this {
    const inner = new Writer();
    for (const item of items) {
      const element = new Writer();
      write(element, item);
      inner.blob(element.done());
    }
    return this.blob(inner.done());
  }

  done(): Uint8Array {
    return Uint8Array.from(this.bytes);
  }
}

class Reader {
  private at = 0;

  constructor(private readonly bytes: Uint8Array) {}

  get rest(): number {
    return this.bytes.length - this.at;
  }

  u8(): number {
    if (this.at >= this.bytes.length) throw new Error("a body that ends early");
    return this.bytes[this.at++]!;
  }

  bool(): boolean {
    return this.u8() !== 0;
  }

  big(): bigint {
    let value = 0n;
    for (let shift = 0n; ; shift += 7n) {
      const byte = this.u8();
      value |= BigInt(byte & 0x7f) << shift;
      if ((byte & 0x80) === 0) return value;
      if (shift > 63n) throw new Error("a number that is too long");
    }
  }

  uint(): number {
    return Number(this.big());
  }

  blob(): Uint8Array {
    const length = this.uint();
    if (length > this.rest) throw new Error("a body that ends early");
    const out = this.bytes.subarray(this.at, this.at + length);
    this.at += length;
    return out;
  }

  str(): string {
    return UTF8.decode(this.blob());
  }

  list<T>(read: (reader: Reader) => T): T[] {
    const inner = new Reader(this.blob());
    const items: T[] = [];
    while (inner.rest > 0) items.push(read(new Reader(inner.blob())));
    return items;
  }
}

function frame(kind: number, seq: number, payload: Uint8Array): Uint8Array<ArrayBuffer> {
  const out = new Uint8Array(new ArrayBuffer(payload.length + 8));
  out[0] = PROTOCOL_VERSION;
  out[1] = kind;
  out[2] = seq & 0xff;
  out[3] = (seq >> 8) & 0xff;
  out.set(payload, 4);
  const crc = crc32c(out.subarray(0, payload.length + 4));
  new DataView(out.buffer).setUint32(payload.length + 4, crc, true);
  return out;
}

function unframe(bytes: Uint8Array): { kind: number; seq: number; payload: Uint8Array } {
  if (bytes.length < 8 || bytes[0] !== PROTOCOL_VERSION) throw new Error("not a frame");
  const body = bytes.length - 4;
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  if (view.getUint32(body, true) !== crc32c(bytes.subarray(0, body))) {
    throw new Error("a frame that fails its check");
  }
  return { kind: bytes[1]!, seq: view.getUint16(2, true), payload: bytes.subarray(4, body) };
}

function readDeclaration(payload: Uint8Array): Declaration {
  const reader = new Reader(payload);
  const name = reader.str();
  const fields = reader.list((field) => ({
    name: field.str(),
    type: field.u8() as FieldType,
    unit: field.str(),
    typeName: field.str(),
  }));
  return { name, fields, history: reader.uint(), expiryMs: reader.uint(), maxPayload: reader.uint() };
}

// --- What a page holds ---

/** A subscription to a topic. */
export class Subscription {
  /** The topic as it was declared, once it has been. */
  declaration: Declaration | null = null;
  status: Status = Status.Waiting;
  /** Called when the topic is declared, goes live, or its publisher goes. */
  onstatus: ((status: Status) => void) | null = null;
  /** @internal */
  handle: number | null = null;

  /** @internal */
  constructor(
    private readonly orion: Orion,
    readonly topic: string,
    /** @internal */
    readonly onsample: (sample: Sample) => void,
  ) {}

  /** Ends the subscription. */
  close(): void {
    this.orion.unsubscribe(this);
  }
}

/** A topic this page publishes. */
export class Publisher {
  /** How many subscribers the topic has. */
  subscribers = 0;
  /** What the newest subscriber asked of the publisher, unread by Orion. */
  request: Uint8Array = new Uint8Array();
  /** Called when the subscribers change. */
  ondemand: ((subscribers: number) => void) | null = null;

  /** @internal */
  constructor(
    private readonly orion: Orion,
    /** @internal */
    readonly handle: number,
  ) {}

  /** Publishes one sample of `field`, belonging to `time` in nanoseconds. */
  publish(field: number, time: bigint, payload: Uint8Array, othersDepend = false): void {
    this.orion.sample(this.handle, field, time, payload, othersDepend);
  }
}

interface Waiting {
  resolve: (value: Reader) => void;
  reject: (error: Error) => void;
}

interface Running {
  seq: number;
  onprogress: ((progress: Progress) => void) | undefined;
  resolve: (result: Result) => void;
  reject: (error: Error) => void;
}

/** A page's connection to a board. */
export class Orion {
  /** The board's name. */
  node = "";
  /** Called once when the connection ends, for whatever reason. */
  onclose: (() => void) | null = null;

  private seq = 0;
  private nextCall = 0n;
  private heartbeat: ReturnType<typeof setInterval> | undefined;
  /** Requests waiting for the frame that answers them, by their sequence number. */
  private readonly waiting = new Map<number, Waiting>();
  private readonly subscriptions: Subscription[] = [];
  /** Subscriptions not yet given a handle, by the sequence number of their request. */
  private readonly asked = new Map<number, Subscription>();
  private readonly publishers = new Map<number, Publisher>();
  private readonly runs = new Map<bigint, Running>();
  /** The declaration that came just before the `Opened` it belongs to. */
  private declared: Declaration | null = null;

  private constructor(private readonly socket: WebSocket) {}

  /** Connects to the comm core at `url` as the program `name`, and says hello. */
  static connect(url: string, name: string): Promise<Orion> {
    return new Promise((resolve, reject) => {
      const socket = new WebSocket(url, PROTOCOL);
      socket.binaryType = "arraybuffer";
      const orion = new Orion(socket);
      let welcomed = false;
      socket.onopen = () => {
        const hello = new Writer().str(name).uint(ROLES).uint(MAX_FRAME).uint(INTERFACE_REVISION);
        orion.send(Kind.Hello, hello.done());
      };
      socket.onerror = () => {
        if (!welcomed) reject(new Error(`no comm core answered at ${url}`));
      };
      socket.onclose = () => {
        if (!welcomed) reject(new Error(`the comm core at ${url} closed the connection`));
        orion.closed();
      };
      socket.onmessage = (event) => {
        if (!(event.data instanceof ArrayBuffer)) return;
        let message;
        try {
          message = unframe(new Uint8Array(event.data));
        } catch {
          // A frame that is damaged is not acted on.
          return;
        }
        if (welcomed) {
          orion.receive(message.kind, message.payload);
        } else if (message.kind === Kind.Welcome) {
          const welcome = new Reader(message.payload);
          orion.node = welcome.str();
          welcome.uint();
          const heartbeatMs = welcome.uint();
          // Heard from at least twice in every heartbeat, so the board never gives up on a
          // page that is merely quiet.
          orion.heartbeat = setInterval(
            () => orion.send(Kind.Ping, new Writer().uint(Date.now()).done()),
            Math.max(100, heartbeatMs / 2),
          );
          welcomed = true;
          resolve(orion);
        } else if (message.kind === Kind.Reject) {
          reject(new Error(`the comm core turned the session away (${message.payload[0]})`));
        }
      };
    });
  }

  /** Ends the connection. */
  close(): void {
    this.socket.close();
  }

  /**
   * Subscribes to `topic`, or to `node/topic` for another board's. `onsample` is given each
   * sample as it arrives. Resolves when the board has taken the subscription; the topic
   * may not have been declared yet, and `status` and `onstatus` say when it is.
   */
  subscribe(topic: string, onsample: (sample: Sample) => void): Promise<Subscription> {
    const [node, name] = topic.includes("/") ? topic.split("/", 2) : [undefined, topic];
    const body = new Writer();
    if (node === undefined) body.u8(0);
    else body.u8(1).str(node);
    body.str(name!).blob(new Uint8Array()).uint(0).uint(0);
    const subscription = new Subscription(this, topic, onsample);
    const seq = this.send(Kind.TopicSubscribe, body.done());
    this.asked.set(seq, subscription);
    this.subscriptions.push(subscription);
    return new Promise((resolve, reject) => {
      // Answered by `Opened` or by `TopicState`; either says it was taken.
      this.waiting.set(seq, { resolve: () => resolve(subscription), reject });
    });
  }

  /** @internal */
  unsubscribe(subscription: Subscription): void {
    const at = this.subscriptions.indexOf(subscription);
    if (at < 0) return;
    this.subscriptions.splice(at, 1);
    if (subscription.handle !== null) {
      this.send(Kind.TopicUnsubscribe, new Writer().uint(subscription.handle).done());
    }
  }

  /** Declares a topic this page publishes. */
  async declare(declaration: {
    name: string;
    fields: Field[];
    history?: number;
    expiryMs?: number;
    maxPayload: number;
  }): Promise<Publisher> {
    const body = new Writer()
      .str(declaration.name)
      .list(declaration.fields, (writer, field) => {
        writer.str(field.name).u8(field.type).str(field.unit ?? "").str(field.typeName ?? "");
      })
      .uint(declaration.history ?? 2)
      .uint(declaration.expiryMs ?? 0)
      .uint(declaration.maxPayload);
    const opened = await this.request(Kind.TopicDeclare, body.done());
    opened.uint();
    const publisher = new Publisher(this, opened.uint());
    this.publishers.set(publisher.handle, publisher);
    return publisher;
  }

  /** @internal */
  sample(handle: number, field: number, time: bigint, payload: Uint8Array, othersDepend: boolean): void {
    const body = new Writer().uint(handle).uint(field).uint(time).u8(othersDepend ? 1 : 0).blob(payload);
    this.send(Kind.TopicSample, body.done());
  }

  /**
   * Runs an action and resolves with how it ended. `onprogress` is told how it is doing
   * meanwhile. Rejects with `Refused` if the board would not run it.
   */
  run(action: Action, onprogress?: (progress: Progress) => void): Promise<Result> {
    const call = ++this.nextCall;
    const body = new Writer().uint(call).str(action.name);
    if (action.resource !== undefined) body.uint(1).str(action.resource);
    else body.uint(0).str(action.node ?? this.node);
    body.blob(action.args ?? new Uint8Array()).uint(action.deadlineMs ?? 0).bool(false);
    const seq = this.send(Kind.ActionRun, body.done());
    return new Promise((resolve, reject) => {
      this.runs.set(call, { seq, onprogress, resolve, reject });
    });
  }

  private send(kind: number, payload: Uint8Array): number {
    this.seq = (this.seq + 1) & 0xffff;
    if (this.socket.readyState === WebSocket.OPEN) {
      this.socket.send(frame(kind, this.seq, payload));
    }
    return this.seq;
  }

  private request(kind: number, payload: Uint8Array): Promise<Reader> {
    const seq = this.send(kind, payload);
    return new Promise((resolve, reject) => this.waiting.set(seq, { resolve, reject }));
  }

  private answered(seq: number, reader: Reader): void {
    const waiting = this.waiting.get(seq);
    if (waiting === undefined) return;
    this.waiting.delete(seq);
    waiting.resolve(reader);
  }

  private receive(kind: number, payload: Uint8Array): void {
    const reader = new Reader(payload);
    switch (kind) {
      case Kind.Ping:
        this.send(Kind.Pong, payload);
        break;
      case Kind.TopicSample: {
        const handle = reader.uint();
        const sample: Sample = {
          field: reader.uint(),
          time: reader.big(),
          othersDepend: (reader.u8() & 1) !== 0,
          payload: reader.blob(),
        };
        for (const subscription of this.subscriptions) {
          if (subscription.handle === handle) subscription.onsample(sample);
        }
        break;
      }
      case Kind.TopicDeclare:
        this.declared = readDeclaration(payload);
        break;
      case Kind.TopicOpened: {
        const seq = reader.uint();
        const handle = reader.uint();
        const subscription = this.asked.get(seq) ?? this.subscriptions.find((known) => known.handle === handle);
        if (subscription !== undefined) {
          this.asked.delete(seq);
          subscription.handle = handle;
          subscription.declaration = this.declared;
          this.declared = null;
          this.status(subscription, Status.Live);
        }
        this.answered(seq, new Reader(payload));
        break;
      }
      case Kind.TopicState: {
        const seq = reader.uint();
        const handle = reader.uint();
        const status = reader.u8() as Status;
        const subscription = this.asked.get(seq) ?? this.subscriptions.find((known) => known.handle === handle);
        if (subscription !== undefined) {
          // A topic that waits has a handle already: the declaration follows under it.
          subscription.handle = handle;
          this.status(subscription, status);
        }
        this.answered(seq, new Reader(payload));
        break;
      }
      case Kind.TopicDemand: {
        const publisher = this.publishers.get(reader.uint());
        if (publisher !== undefined) {
          publisher.subscribers = reader.uint();
          const request = reader.blob();
          if (request.length > 0) publisher.request = request.slice();
          publisher.ondemand?.(publisher.subscribers);
        }
        break;
      }
      case Kind.ActionProgress: {
        const run = this.runs.get(reader.big());
        run?.onprogress?.({ percent: reader.u8(), message: reader.str(), data: reader.blob().slice() });
        break;
      }
      case Kind.ActionResult: {
        const call = reader.big();
        const run = this.runs.get(call);
        if (run !== undefined) {
          this.runs.delete(call);
          run.resolve({ outcome: reader.u8() as Outcome, output: reader.blob().slice(), detail: reader.str() });
        }
        break;
      }
      case Kind.Refusal: {
        const seq = reader.uint();
        const refused = new Refused(reader.uint(), reader.str());
        const waiting = this.waiting.get(seq);
        if (waiting !== undefined) {
          this.waiting.delete(seq);
          waiting.reject(refused);
        }
        for (const [call, run] of this.runs) {
          if (run.seq === seq) {
            this.runs.delete(call);
            run.reject(refused);
          }
        }
        break;
      }
      default:
        // A kind this build does not know is ignored: that is the wire's rule.
        break;
    }
  }

  private status(subscription: Subscription, status: Status): void {
    subscription.status = status;
    subscription.onstatus?.(status);
  }

  private closed(): void {
    if (this.heartbeat !== undefined) clearInterval(this.heartbeat);
    this.heartbeat = undefined;
    const gone = new Error("the connection to the board ended");
    for (const waiting of this.waiting.values()) waiting.reject(gone);
    for (const run of this.runs.values()) run.reject(gone);
    this.waiting.clear();
    this.runs.clear();
    for (const subscription of this.subscriptions) this.status(subscription, Status.PublisherGone);
    this.onclose?.();
    this.onclose = null;
  }
}
