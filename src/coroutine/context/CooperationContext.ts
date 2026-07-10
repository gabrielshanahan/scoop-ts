/**
 * A map-like container for sharing contextual data across saga executions and cooperation
 * lineages. Heavily inspired by Kotlin's CoroutineContext; direct port of the Kotlin original.
 *
 * The entire implementation is built around lazy deserialization — all context data is stored as
 * JSON strings and only deserialized when first accessed via `get()`. The distinction between
 * [MappedKey] and [UnmappedKey] addresses the fact that the context can contain data that is
 * relevant only to some services and completely unknown to others:
 * - MappedKey: context elements known to this service (deserialized to typed objects)
 * - UnmappedKey: context elements from other services (kept as JSON)
 */
export interface CooperationContext {
    get<E extends Element>(key: Key<E>): E | null

    plus(context: CooperationContext): CooperationContext

    minus(key: Key<Element>): CooperationContext

    fold<R>(initial: R, operation: (acc: R, element: Element) => R): R
}

let mappedKeyCounter = 0

/**
 * A key for storing typed context elements that this service knows about.
 *
 * The Kotlin original derives the serialized key name from the class's simple name and the
 * element class from generics reflection; here the name and a reviver function are passed
 * explicitly (there is no runtime type information in TS — see DECISIONS.md). Keys are singletons
 * compared by identity, exactly like Kotlin `data object` keys.
 */
export class MappedKey<E extends MappedElement> {
    readonly kind = "mapped" as const
    /** Identity token reproducing Kotlin HashMap identity semantics for singleton keys. */
    readonly identity = `mapped:${mappedKeyCounter++}`

    constructor(
        readonly name: string,
        readonly reviveElement: (json: any) => E,
    ) {}
}

/** A key for storing context data from other services that this service doesn't know about. */
export class UnmappedKey {
    readonly kind = "unmapped" as const

    constructor(readonly key: string) {}

    get identity(): string {
        return `unmapped:${this.key}`
    }
}

export type Key<E extends Element> = MappedKey<E & MappedElement> | UnmappedKey

export function keySerializedValue(key: Key<Element>): string {
    return key.kind === "unmapped" ? key.key : key.name
}

export function keyEquals(a: Key<Element>, b: Key<Element>): boolean {
    return a.identity === b.identity
}

/**
 * A single piece of data stored in the cooperation context. Each element is itself a valid
 * [CooperationContext] containing only that element.
 */
export abstract class Element implements CooperationContext {
    abstract readonly key: Key<Element>

    get<E extends Element>(key: Key<E>): E | null {
        return keyEquals(this.key, key as Key<Element>) ? (this as unknown as E) : null
    }

    plus(context: CooperationContext): CooperationContext {
        if (has(context, this.key)) {
            return context
        }
        return singletonContextMap(this.key, this).plus(context)
    }

    minus(key: Key<Element>): CooperationContext {
        return keyEquals(this.key, key) ? emptyContext() : this
    }

    fold<R>(initial: R, operation: (acc: R, element: Element) => R): R {
        return operation(initial, this)
    }
}

/**
 * An element that wraps JSON data from other services as a string, unparsed, so it can be passed
 * through unchanged.
 */
export class OpaqueElement extends Element {
    constructor(
        override readonly key: UnmappedKey,
        readonly json: string,
    ) {
        super()
    }
}

/**
 * Base class for typed context elements defined by this service.
 *
 * The element's "bean" form is all own enumerable properties except `key` (the analog of Jackson
 * bean serialization with `@JsonIgnore` on the key). When an element appears NESTED inside
 * another element's data (e.g. a deadline inside a deadline's `trace`), it serializes in
 * context-wrapped form `{"KeyName": {…bean…}}` — exactly what the original's Jackson serializer
 * modifier produces for any CooperationContext instance it encounters. The context codec unwraps
 * the top level (see CooperationContextModule.ts).
 */
export abstract class MappedElement extends Element {
    constructor(override readonly key: MappedKey<any>) {
        super()
    }

    toJSON(): unknown {
        return { [keySerializedValue(this.key)]: elementBean(this) }
    }
}

/** The element's data properties (everything except the key). */
export function elementBean(element: Element): Record<string, unknown> {
    const { key: _key, ...rest } = element as unknown as Record<string, unknown>
    return rest
}

export function has(context: CooperationContext, key: Key<Element>): boolean {
    return context.get(key) !== null
}

export function contextSize(context: CooperationContext): number {
    return context.fold(0, acc => acc + 1)
}

export function isNotEmpty(context: CooperationContext): boolean {
    return contextSize(context) > 0
}

// The two functions below are implemented in CooperationContextMap.ts and injected here to break
// the module cycle (Element.plus/minus need a map implementation; the map needs Element).
type SingletonFactory = (key: Key<Element>, element: Element) => CooperationContext
type EmptyFactory = () => CooperationContext

let singletonFactory: SingletonFactory | null = null
let emptyFactory: EmptyFactory | null = null

export function _registerContextMapFactories(s: SingletonFactory, e: EmptyFactory): void {
    singletonFactory = s
    emptyFactory = e
}

function singletonContextMap(key: Key<Element>, element: Element): CooperationContext {
    return singletonFactory!(key, element)
}

export function emptyContext(): CooperationContext {
    return emptyFactory!()
}
