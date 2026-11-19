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
/** The `Reject` reason for a `Hello` of another wire revision. */
const REVISION_MISMATCH = 5;
/** Turned away for want of room, or with no session to take a frame: tried again. */
const BUSY = 3;
const NO_SESSION = 6;
const MAX_FRAME = 64 * 1024;
/** The roles a page asks for: a client, and a provider so that it may publish. */
const ROLES = (1 << 0) | (1 << 1);
/** The type of a topic's field. */
export var FieldType;
(function (FieldType) {
    FieldType[FieldType["Bool"] = 1] = "Bool";
    FieldType[FieldType["Int"] = 2] = "Int";
    FieldType[FieldType["Float"] = 3] = "Float";
    FieldType[FieldType["Text"] = 4] = "Text";
    FieldType[FieldType["Bytes"] = 5] = "Bytes";
    FieldType[FieldType["FloatArray"] = 6] = "FloatArray";
    FieldType[FieldType["IntArray"] = 7] = "IntArray";
})(FieldType || (FieldType = {}));
/** Where a topic is. */
export var Status;
(function (Status) {
    Status[Status["Live"] = 1] = "Live";
    Status[Status["PublisherGone"] = 2] = "PublisherGone";
    Status[Status["Waiting"] = 3] = "Waiting";
})(Status || (Status = {}));
/** How an action ended. */
export var Outcome;
(function (Outcome) {
    Outcome[Outcome["Succeeded"] = 1] = "Succeeded";
    Outcome[Outcome["Failed"] = 2] = "Failed";
    Outcome[Outcome["Rejected"] = 3] = "Rejected";
    Outcome[Outcome["TimedOut"] = 4] = "TimedOut";
    Outcome[Outcome["Cancelled"] = 5] = "Cancelled";
    /** Told to stop, and its program did not say how it ended in time: it may have been done. */
    Outcome[Outcome["Indeterminate"] = 6] = "Indeterminate";
})(Outcome || (Outcome = {}));
/** A request the board refused, with its stable code. */
export class Refused extends Error {
    code;
    constructor(code, detail) {
        super(detail || `refused (${code})`);
        this.code = code;
        this.name = "Refused";
    }
}
/**
 * A wait that ran past the ceiling `setReply` gave it. The connection is fine: the answer was
 * slow.
 */
/**
 * The comm core answered the hello with no: the page's roles or its wire revision. An answer,
 * so connecting again does not change it, and `Orion.connect` stops trying.
 */
export class TurnedAway extends Error {
    constructor(message) {
        super(message);
        this.name = "TurnedAway";
    }
}
/** How long `Orion.connect` waits between tries: a way in is tried again soon, and never backed off. */
export const CONNECT_AGAIN_MS = 250;
export class TimedOut extends Error {
    ceilingMs;
    constructor(ceilingMs) {
        super(`no answer within ${ceilingMs} ms`);
        this.ceilingMs = ceilingMs;
        this.name = "TimedOut";
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
export function crc32c(bytes) {
    let crc = 0xffffffff;
    for (const byte of bytes) {
        crc = CRC_TABLE[(crc ^ byte) & 0xff] ^ (crc >>> 8);
    }
    return (crc ^ 0xffffffff) >>> 0;
}
const TEXT = new TextEncoder();
const UTF8 = new TextDecoder("utf-8", { fatal: true });
class Writer {
    bytes = [];
    u8(value) {
        this.bytes.push(value & 0xff);
        return this;
    }
    bool(value) {
        return this.u8(value ? 1 : 0);
    }
    /** An unsigned integer of any width, as postcard writes them: seven bits a byte. */
    uint(value) {
        let rest = BigInt(value);
        do {
            const low = Number(rest & 0x7fn);
            rest >>= 7n;
            this.bytes.push(rest === 0n ? low : low | 0x80);
        } while (rest !== 0n);
        return this;
    }
    raw(bytes) {
        for (const byte of bytes)
            this.bytes.push(byte);
        return this;
    }
    blob(bytes) {
        return this.uint(bytes.length).raw(bytes);
    }
    str(text) {
        return this.blob(TEXT.encode(text));
    }
    /** A list: its length in bytes, then each element with its own length in front. */
    list(items, write) {
        const inner = new Writer();
        for (const item of items) {
            const element = new Writer();
            write(element, item);
            inner.blob(element.done());
        }
        return this.blob(inner.done());
    }
    done() {
        return Uint8Array.from(this.bytes);
    }
}
class Reader {
    bytes;
    at = 0;
    constructor(bytes) {
        this.bytes = bytes;
    }
    get rest() {
        return this.bytes.length - this.at;
    }
    u8() {
        if (this.at >= this.bytes.length)
            throw new Error("a body that ends early");
        return this.bytes[this.at++];
    }
    bool() {
        return this.u8() !== 0;
    }
    big() {
        let value = 0n;
        for (let shift = 0n;; shift += 7n) {
            const byte = this.u8();
            value |= BigInt(byte & 0x7f) << shift;
            if ((byte & 0x80) === 0)
                return value;
            if (shift > 63n)
                throw new Error("a number that is too long");
        }
    }
    uint() {
        return Number(this.big());
    }
    blob() {
        const length = this.uint();
        if (length > this.rest)
            throw new Error("a body that ends early");
        const out = this.bytes.subarray(this.at, this.at + length);
        this.at += length;
        return out;
    }
    str() {
        return UTF8.decode(this.blob());
    }
    list(read) {
        const inner = new Reader(this.blob());
        const items = [];
        while (inner.rest > 0)
            items.push(read(new Reader(inner.blob())));
        return items;
    }
}
function frame(kind, seq, payload) {
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
function unframe(bytes) {
    if (bytes.length < 8 || bytes[0] !== PROTOCOL_VERSION)
        throw new Error("not a frame");
    const body = bytes.length - 4;
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    if (view.getUint32(body, true) !== crc32c(bytes.subarray(0, body))) {
        throw new Error("a frame that fails its check");
    }
    return { kind: bytes[1], seq: view.getUint16(2, true), payload: bytes.subarray(4, body) };
}
function readDeclaration(payload) {
    const reader = new Reader(payload);
    const name = reader.str();
    const fields = reader.list((field) => ({
        name: field.str(),
        type: field.u8(),
        unit: field.str(),
        typeName: field.str(),
    }));
    return { name, fields, history: reader.uint(), expiryMs: reader.uint(), maxPayload: reader.uint() };
}
// --- What a page holds ---
/** A subscription to a topic. */
export class Subscription {
    orion;
    topic;
    onsample;
    /** The topic as it was declared, once it has been. */
    declaration = null;
    status = Status.Waiting;
    /** Called when the topic is declared, goes live, or its publisher goes. */
    onstatus = null;
    /** @internal */
    handle = null;
    /** @internal */
    constructor(orion, topic, 
    /** @internal */
    onsample) {
        this.orion = orion;
        this.topic = topic;
        this.onsample = onsample;
    }
    /** Ends the subscription. */
    close() {
        this.orion.unsubscribe(this);
    }
}
/** A topic this page publishes. */
export class Publisher {
    orion;
    handle;
    /** How many subscribers the topic has. */
    subscribers = 0;
    /** What the newest subscriber asked of the publisher, unread by Orion. */
    request = new Uint8Array();
    /** Called when the subscribers change. */
    ondemand = null;
    /** @internal */
    constructor(orion, 
    /** @internal */
    handle) {
        this.orion = orion;
        this.handle = handle;
    }
    /** Publishes one sample of `field`, belonging to `time` in nanoseconds. */
    publish(field, time, payload, othersDepend = false) {
        this.orion.sample(this.handle, field, time, payload, othersDepend);
    }
}
/** A page's connection to a board. */
export class Orion {
    socket;
    /** The board's name. */
    node = "";
    /** Called once when the connection ends, for whatever reason. */
    onclose = null;
    seq = 0;
    nextCall = 0n;
    heartbeat;
    reply = { alive: false };
    lastHeard = Date.now();
    /** The board's idle time, from its welcome. */
    idleMs = 0;
    /** Ended for being silent too long. */
    silent = false;
    /** Requests waiting for the frame that answers them, by their sequence number. */
    waiting = new Map();
    subscriptions = [];
    /** Subscriptions not yet given a handle, by the sequence number of their request. */
    asked = new Map();
    publishers = new Map();
    runs = new Map();
    /** The declaration that came just before the `Opened` it belongs to. */
    declared = null;
    constructor(socket) {
        this.socket = socket;
    }
    /**
     * Connects to the comm core at `url` as the program `name`, and says hello. Nobody answering
     * is not an answer: this keeps trying, every `CONNECT_AGAIN_MS`, until the comm core welcomes
     * the page or turns it away (`TurnedAway`, rejected at once), so a page may open before the
     * board is up. `keepTrying`, if given, is told how many tries have failed before each wait,
     * and stops the trying by returning false: the last failure is then the rejection.
     */
    static async connect(url, name, keepTrying) {
        for (let failures = 1;; failures++) {
            try {
                return await Orion.connectOnce(url, name);
            }
            catch (error) {
                if (error instanceof TurnedAway)
                    throw error;
                if (keepTrying !== undefined && !keepTrying(failures))
                    throw error;
            }
            await new Promise((again) => setTimeout(again, CONNECT_AGAIN_MS));
        }
    }
    /**
     * One try at connecting: nothing answering is the rejection. For a page that says so to a
     * person, not for one that needs the board.
     */
    static connectOnce(url, name) {
        return new Promise((resolve, reject) => {
            const socket = new WebSocket(url, PROTOCOL);
            socket.binaryType = "arraybuffer";
            const orion = new Orion(socket);
            let welcomed = false;
            socket.onopen = () => {
                const hello = new Writer().str(name).uint(ROLES).uint(MAX_FRAME).uint(INTERFACE_REVISION);
                orion.send(1 /* Kind.Hello */, hello.done());
            };
            socket.onerror = () => {
                if (!welcomed)
                    reject(new Error(`no comm core answered at ${url}`));
            };
            socket.onclose = () => {
                if (!welcomed)
                    reject(new Error(`the comm core at ${url} closed the connection`));
                orion.closed();
            };
            socket.onmessage = (event) => {
                if (!(event.data instanceof ArrayBuffer))
                    return;
                orion.lastHeard = Date.now();
                let message;
                try {
                    message = unframe(new Uint8Array(event.data));
                }
                catch {
                    // A frame that is damaged is not acted on.
                    return;
                }
                if (welcomed) {
                    orion.receive(message.kind, message.payload);
                }
                else if (message.kind === 2 /* Kind.Welcome */) {
                    const welcome = new Reader(message.payload);
                    orion.node = welcome.str();
                    welcome.uint();
                    const heartbeatMs = welcome.uint();
                    welcome.uint();
                    const revision = welcome.uint();
                    if (revision !== INTERFACE_REVISION) {
                        orion.close();
                        reject(new TurnedAway(`the board speaks wire revision ${revision} and this page ${INTERFACE_REVISION}`));
                        return;
                    }
                    // Appended: a board too old to say has three heartbeats.
                    const idleMs = welcome.rest > 0 ? welcome.uint() : 0;
                    orion.idleMs = idleMs > 0 ? idleMs : heartbeatMs * 3;
                    // Heard from at least twice in every heartbeat, so the board never gives up on a
                    // page that is merely quiet; and, when the page asked, a board silent for its idle
                    // time is taken for gone.
                    orion.heartbeat = setInterval(() => {
                        if (orion.reply.alive && orion.silentFor() >= orion.idleMs) {
                            orion.silent = true;
                            orion.close();
                            return;
                        }
                        orion.send(4 /* Kind.Ping */, new Writer().uint(Date.now()).done());
                    }, Math.max(100, heartbeatMs / 2));
                    welcomed = true;
                    resolve(orion);
                }
                else if (message.kind === 3 /* Kind.Reject */) {
                    const turned = new Reader(message.payload);
                    const reason = turned.u8();
                    if (reason === REVISION_MISMATCH) {
                        const board = turned.uint();
                        reject(new TurnedAway(`the board speaks wire revision ${board} and this page ${INTERFACE_REVISION}`));
                    }
                    else if (reason === BUSY || reason === NO_SESSION) {
                        // No room now, or no session for this one: not an answer, so tried again.
                        reject(new Error(`the comm core could not take the session now (${reason})`));
                    }
                    else {
                        reject(new TurnedAway(`the comm core turned the session away (${reason})`));
                    }
                }
            };
        });
    }
    /** Ends the connection. */
    close() {
        this.socket.close();
    }
    /** Milliseconds since anything at all, a ping's answer included, came from the board. */
    silentFor() {
        return Date.now() - this.lastHeard;
    }
    /**
     * How every wait for an answer begun from now on is bounded, until this is called again.
     * Set it back after a request that needed a longer ceiling.
     */
    setReply(reply) {
        this.reply = reply;
    }
    /**
     * Subscribes to `topic`, or to `node/topic` for another board's. `onsample` is given each
     * sample as it arrives. Resolves when the board has taken the subscription; the topic
     * may not have been declared yet, and `status` and `onstatus` say when it is.
     */
    subscribe(topic, onsample) {
        const [node, name] = topic.includes("/") ? topic.split("/", 2) : [undefined, topic];
        const body = new Writer();
        if (node === undefined)
            body.u8(0);
        else
            body.u8(1).str(node);
        body.str(name).blob(new Uint8Array()).uint(0).uint(0);
        const subscription = new Subscription(this, topic, onsample);
        const seq = this.send(67 /* Kind.TopicSubscribe */, body.done());
        this.asked.set(seq, subscription);
        this.subscriptions.push(subscription);
        return new Promise((resolve, reject) => {
            // Answered by `Opened` or by `TopicState`; either says it was taken.
            this.waiting.set(seq, { resolve: () => resolve(subscription), reject });
            this.ceiling(reject, () => this.waiting.delete(seq));
        });
    }
    /** @internal */
    unsubscribe(subscription) {
        const at = this.subscriptions.indexOf(subscription);
        if (at < 0)
            return;
        this.subscriptions.splice(at, 1);
        if (subscription.handle !== null) {
            this.send(68 /* Kind.TopicUnsubscribe */, new Writer().uint(subscription.handle).done());
        }
    }
    /** Declares a topic this page publishes. */
    async declare(declaration) {
        const body = new Writer()
            .str(declaration.name)
            .list(declaration.fields, (writer, field) => {
            writer.str(field.name).u8(field.type).str(field.unit ?? "").str(field.typeName ?? "");
        })
            .uint(declaration.history ?? 2)
            .uint(declaration.expiryMs ?? 0)
            .uint(declaration.maxPayload);
        const opened = await this.request(64 /* Kind.TopicDeclare */, body.done());
        opened.uint();
        const publisher = new Publisher(this, opened.uint());
        this.publishers.set(publisher.handle, publisher);
        return publisher;
    }
    /** @internal */
    sample(handle, field, time, payload, othersDepend) {
        const body = new Writer().uint(handle).uint(field).uint(time).u8(othersDepend ? 1 : 0).blob(payload);
        this.send(69 /* Kind.TopicSample */, body.done());
    }
    /**
     * Runs an action and resolves with how it ended. `onprogress` is told how it is doing
     * meanwhile. Rejects with `Refused` if the board would not run it.
     */
    run(action, onprogress) {
        const call = ++this.nextCall;
        const body = new Writer().uint(call).str(action.name);
        if (action.resource !== undefined)
            body.uint(1).str(action.resource);
        else
            body.uint(0).str(action.node ?? this.node);
        body.blob(action.args ?? new Uint8Array()).uint(action.deadlineMs ?? 0).bool(false);
        const seq = this.send(49 /* Kind.ActionRun */, body.done());
        return new Promise((resolve, reject) => {
            this.runs.set(call, { seq, instance: 0n, run: 0n, onprogress, resolve, reject });
            this.ceiling(reject, () => this.runs.delete(call));
        });
    }
    /**
     * The run a frame is about: by the handle it names, else by its number. A run whose handle
     * is known is never matched by number.
     */
    about(call, instance, id) {
        if (instance !== 0n && id !== 0n) {
            for (const entry of this.runs) {
                if (entry[1].instance === instance && entry[1].run === id)
                    return entry;
            }
        }
        const run = this.runs.get(call);
        return run !== undefined && run.run === 0n ? [call, run] : undefined;
    }
    send(kind, payload) {
        this.seq = (this.seq + 1) & 0xffff;
        if (this.socket.readyState === WebSocket.OPEN) {
            this.socket.send(frame(kind, this.seq, payload));
        }
        return this.seq;
    }
    request(kind, payload) {
        const seq = this.send(kind, payload);
        return new Promise((resolve, reject) => {
            this.waiting.set(seq, { resolve, reject });
            this.ceiling(reject, () => this.waiting.delete(seq));
        });
    }
    /** Rejects a wait with `TimedOut` at the ceiling, if there is one and it still waits. */
    ceiling(reject, stillWaiting) {
        const reply = this.reply;
        if (!reply.alive || reply.ceilingMs === undefined)
            return;
        const ceilingMs = reply.ceilingMs;
        setTimeout(() => {
            if (stillWaiting())
                reject(new TimedOut(ceilingMs));
        }, ceilingMs);
    }
    answered(seq, reader) {
        const waiting = this.waiting.get(seq);
        if (waiting === undefined)
            return;
        this.waiting.delete(seq);
        waiting.resolve(reader);
    }
    receive(kind, payload) {
        const reader = new Reader(payload);
        switch (kind) {
            case 4 /* Kind.Ping */:
                this.send(5 /* Kind.Pong */, payload);
                break;
            case 69 /* Kind.TopicSample */: {
                const handle = reader.uint();
                const sample = {
                    field: reader.uint(),
                    time: reader.big(),
                    othersDepend: (reader.u8() & 1) !== 0,
                    payload: reader.blob(),
                };
                for (const subscription of this.subscriptions) {
                    if (subscription.handle === handle)
                        subscription.onsample(sample);
                }
                break;
            }
            case 64 /* Kind.TopicDeclare */:
                this.declared = readDeclaration(payload);
                break;
            case 66 /* Kind.TopicOpened */: {
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
            case 74 /* Kind.TopicState */: {
                const seq = reader.uint();
                const handle = reader.uint();
                const status = reader.u8();
                const subscription = this.asked.get(seq) ?? this.subscriptions.find((known) => known.handle === handle);
                if (subscription !== undefined) {
                    // A topic that waits has a handle already: the declaration follows under it.
                    subscription.handle = handle;
                    this.status(subscription, status);
                }
                this.answered(seq, new Reader(payload));
                break;
            }
            case 71 /* Kind.TopicDemand */: {
                const publisher = this.publishers.get(reader.uint());
                if (publisher !== undefined) {
                    publisher.subscribers = reader.uint();
                    const request = reader.blob();
                    if (request.length > 0)
                        publisher.request = request.slice();
                    publisher.ondemand?.(publisher.subscribers);
                }
                break;
            }
            case 57 /* Kind.ActionStarted */: {
                const run = this.runs.get(reader.big());
                if (run !== undefined && run.run === 0n) {
                    run.instance = reader.big();
                    run.run = reader.big();
                }
                break;
            }
            case 51 /* Kind.ActionProgress */: {
                const call = reader.big();
                const progress = { percent: reader.u8(), message: reader.str(), data: reader.blob().slice() };
                // Appended: the run's handle. A board too old to send it said nothing.
                const [instance, id] = reader.rest > 0 ? [reader.big(), reader.big()] : [0n, 0n];
                this.about(call, instance, id)?.[1].onprogress?.(progress);
                break;
            }
            case 52 /* Kind.ActionResult */: {
                const call = reader.big();
                const outcome = reader.u8();
                const output = reader.blob().slice();
                const detail = reader.str();
                // Appended: a board too old to send them said nothing, which is 0.
                const code = reader.rest > 0 ? reader.uint() : 0;
                const [instance, id] = reader.rest > 0 ? [reader.big(), reader.big()] : [0n, 0n];
                const found = this.about(call, instance, id);
                if (found !== undefined) {
                    this.runs.delete(found[0]);
                    found[1].resolve({ outcome, code, output, detail });
                }
                break;
            }
            case 6 /* Kind.Refusal */: {
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
    status(subscription, status) {
        subscription.status = status;
        subscription.onstatus?.(status);
    }
    closed() {
        if (this.heartbeat !== undefined)
            clearInterval(this.heartbeat);
        this.heartbeat = undefined;
        const gone = new Error(this.silent
            ? `the board was silent for its idle time (${this.idleMs} ms)`
            : "the connection to the board ended");
        for (const waiting of this.waiting.values())
            waiting.reject(gone);
        for (const run of this.runs.values())
            run.reject(gone);
        this.waiting.clear();
        this.runs.clear();
        for (const subscription of this.subscriptions)
            this.status(subscription, Status.PublisherGone);
        this.onclose?.();
        this.onclose = null;
    }
}
