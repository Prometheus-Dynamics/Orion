/** The WebSocket subprotocol that carries Orion's frames. */
export declare const PROTOCOL = "orion.v1";
/** The type of a topic's field. */
export declare enum FieldType {
    Bool = 1,
    Int = 2,
    Float = 3,
    Text = 4,
    Bytes = 5,
    FloatArray = 6,
    IntArray = 7
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
export declare enum Status {
    Live = 1,
    PublisherGone = 2,
    Waiting = 3
}
/** How an action ended. */
export declare enum Outcome {
    Succeeded = 1,
    Failed = 2,
    Rejected = 3,
    TimedOut = 4,
    Cancelled = 5
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
export declare class Refused extends Error {
    readonly code: number;
    constructor(code: number, detail: string);
}
/** CRC-32C, as every frame ends with. */
export declare function crc32c(bytes: Uint8Array): number;
/** A subscription to a topic. */
export declare class Subscription {
    private readonly orion;
    readonly topic: string;
    /** @internal */
    readonly onsample: (sample: Sample) => void;
    /** The topic as it was declared, once it has been. */
    declaration: Declaration | null;
    status: Status;
    /** Called when the topic is declared, goes live, or its publisher goes. */
    onstatus: ((status: Status) => void) | null;
    /** @internal */
    handle: number | null;
    /** @internal */
    constructor(orion: Orion, topic: string, 
    /** @internal */
    onsample: (sample: Sample) => void);
    /** Ends the subscription. */
    close(): void;
}
/** A topic this page publishes. */
export declare class Publisher {
    private readonly orion;
    /** @internal */
    readonly handle: number;
    /** How many subscribers the topic has. */
    subscribers: number;
    /** What the newest subscriber asked of the publisher, unread by Orion. */
    request: Uint8Array;
    /** Called when the subscribers change. */
    ondemand: ((subscribers: number) => void) | null;
    /** @internal */
    constructor(orion: Orion, 
    /** @internal */
    handle: number);
    /** Publishes one sample of `field`, belonging to `time` in nanoseconds. */
    publish(field: number, time: bigint, payload: Uint8Array, othersDepend?: boolean): void;
}
/** A page's connection to a board. */
export declare class Orion {
    private readonly socket;
    /** The board's name. */
    node: string;
    /** Called once when the connection ends, for whatever reason. */
    onclose: (() => void) | null;
    private seq;
    private nextCall;
    private heartbeat;
    /** Requests waiting for the frame that answers them, by their sequence number. */
    private readonly waiting;
    private readonly subscriptions;
    /** Subscriptions not yet given a handle, by the sequence number of their request. */
    private readonly asked;
    private readonly publishers;
    private readonly runs;
    /** The declaration that came just before the `Opened` it belongs to. */
    private declared;
    private constructor();
    /** Connects to the comm core at `url` as the program `name`, and says hello. */
    static connect(url: string, name: string): Promise<Orion>;
    /** Ends the connection. */
    close(): void;
    /**
     * Subscribes to `topic`, or to `node/topic` for another board's. `onsample` is given each
     * sample as it arrives. Resolves when the board has taken the subscription; the topic
     * may not have been declared yet, and `status` and `onstatus` say when it is.
     */
    subscribe(topic: string, onsample: (sample: Sample) => void): Promise<Subscription>;
    /** @internal */
    unsubscribe(subscription: Subscription): void;
    /** Declares a topic this page publishes. */
    declare(declaration: {
        name: string;
        fields: Field[];
        history?: number;
        expiryMs?: number;
        maxPayload: number;
    }): Promise<Publisher>;
    /** @internal */
    sample(handle: number, field: number, time: bigint, payload: Uint8Array, othersDepend: boolean): void;
    /**
     * Runs an action and resolves with how it ended. `onprogress` is told how it is doing
     * meanwhile. Rejects with `Refused` if the board would not run it.
     */
    run(action: Action, onprogress?: (progress: Progress) => void): Promise<Result>;
    private send;
    private request;
    private answered;
    private receive;
    private status;
    private closed;
}
