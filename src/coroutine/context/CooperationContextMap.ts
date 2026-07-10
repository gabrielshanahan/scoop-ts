import {
    _registerContextMapFactories,
    CooperationContext,
    Element,
    Key,
    keyEquals,
    keySerializedValue,
    MappedKey,
    OpaqueElement,
    UnmappedKey,
    has,
} from "./CooperationContext.js"

/**
 * Insertion-ordered map keyed by context [Key] with the Kotlin HashMap equality semantics:
 * mapped keys by identity (they are singletons), unmapped keys by name.
 */
export class KeyedElementMap {
    private readonly entries_ = new Map<string, { key: Key<Element>; element: Element }>()

    static of(...pairs: Array<[Key<Element>, Element]>): KeyedElementMap {
        const map = new KeyedElementMap()
        for (const [key, element] of pairs) {
            map.set(key, element)
        }
        return map
    }

    get(key: Key<Element>): Element | undefined {
        return this.entries_.get(key.identity)?.element
    }

    hasKey(key: Key<Element>): boolean {
        return this.entries_.has(key.identity)
    }

    set(key: Key<Element>, element: Element): void {
        this.entries_.set(key.identity, { key, element })
    }

    delete(key: Key<Element>): void {
        this.entries_.delete(key.identity)
    }

    keys(): Key<Element>[] {
        return [...this.entries_.values()].map(e => e.key)
    }

    values(): Element[] {
        return [...this.entries_.values()].map(e => e.element)
    }

    copy(): KeyedElementMap {
        const map = new KeyedElementMap()
        for (const { key, element } of this.entries_.values()) {
            map.set(key, element)
        }
        return map
    }
}

/**
 * The primary implementation of lazy deserialization for [CooperationContext]. Direct port of the
 * Kotlin `CooperationContextMap`, including its mutable-map sharing behavior (the serialized map
 * is shared by reference between copies where the original shares it, and `get` caches
 * deserialized elements by mutating the deserialized map in place).
 *
 * The Kotlin version carries a nullable ObjectMapper and skips serialized-map lookups when it is
 * null; in this port the deserializer lives on each [MappedKey], so the mapper parameter is
 * unnecessary — mapperless maps in the original always have an empty serialized map, making the
 * behavior identical (see DECISIONS.md).
 */
export class CooperationContextMap implements CooperationContext {
    constructor(
        private readonly serializedMap: Map<string, string>,
        private readonly deserializedMap: KeyedElementMap,
    ) {}

    get<E extends Element>(key: Key<E>): E | null {
        const cached = this.deserializedMap.get(key as Key<Element>)
        if (cached !== undefined) {
            return cached as E
        }

        const serializedElement = this.serializedMap.get(keySerializedValue(key as Key<Element>))
        if (serializedElement === undefined) {
            return null
        }
        const deserializedElement =
            key instanceof UnmappedKey
                ? new OpaqueElement(key, serializedElement)
                : (key as MappedKey<any>).reviveElement(JSON.parse(serializedElement))
        this.deserializedMap.set(key as Key<Element>, deserializedElement)
        return deserializedElement as E
    }

    plus(context: CooperationContext): CooperationContext {
        if (context instanceof Element) {
            if (has(this, context.key)) {
                return this.minus(context.key).plus(this.get(context.key)!.plus(context))
            }
            const newDeserialized = this.deserializedMap.copy()
            newDeserialized.set(context.key, context)
            return new CooperationContextMap(this.serializedMap, newDeserialized)
        }
        if (context instanceof CooperationContextMap) {
            // Union of deserialized keys from both maps (identity semantics, insertion order:
            // ours first, then the other map's novel keys — matching Kotlin's `keys + keys`).
            const deserializedKeys: Key<Element>[] = [...this.deserializedMap.keys()]
            for (const key of context.deserializedMap.keys()) {
                if (!deserializedKeys.some(k => keyEquals(k, key))) {
                    deserializedKeys.push(key)
                }
            }

            // Make sure all keys deserialized in one map are also deserialized in the other map,
            // so we can run instance-specific logic when calling Element.plus
            for (const key of deserializedKeys) {
                this.get(key)
                context.get(key)
            }

            const mergedSerialized = new Map(this.serializedMap)
            for (const [k, v] of context.serializedMap) {
                mergedSerialized.set(k, v)
            }

            const mergedDeserialized = new KeyedElementMap()
            for (const key of deserializedKeys) {
                const mine = this.deserializedMap.get(key)
                const theirs = context.deserializedMap.get(key)
                if (mine !== undefined && theirs !== undefined) {
                    mergedDeserialized.set(key, mine.plus(theirs).get(key)!)
                } else if (mine !== undefined) {
                    mergedDeserialized.set(key, mine)
                } else {
                    mergedDeserialized.set(key, theirs!)
                }
            }

            return new CooperationContextMap(mergedSerialized, mergedDeserialized)
        }
        return context.fold<CooperationContext>(this, (acc, element) => acc.plus(element))
    }

    minus(key: Key<Element>): CooperationContext {
        if (!has(this, key)) {
            return this
        }
        const newSerialized = new Map(this.serializedMap)
        newSerialized.delete(keySerializedValue(key))
        const newDeserialized = this.deserializedMap.copy()
        newDeserialized.delete(key)
        return new CooperationContextMap(newSerialized, newDeserialized)
    }

    /**
     * Folds over all context elements, presenting a unified view of both maps: serialized entries
     * as [OpaqueElement]s, deserialized entries as their typed objects. Reproduces the Kotlin
     * `buildSet { … }.toSortedSet(compareBy { serializedValue })` semantics exactly: duplicates by
     * serialized key name keep the FIRST occurrence (serialized entries are added first), and the
     * result iterates sorted by key name.
     */
    fold<R>(initial: R, operation: (acc: R, element: Element) => R): R {
        const byName = new Map<string, Element>()
        for (const [name, json] of this.serializedMap) {
            if (!byName.has(name)) {
                byName.set(name, new OpaqueElement(new UnmappedKey(name), json))
            }
        }
        for (const element of this.deserializedMap.values()) {
            const name = keySerializedValue(element.key)
            if (!byName.has(name)) {
                byName.set(name, element)
            }
        }
        const sorted = [...byName.entries()]
            .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
            .map(([, element]) => element)
        return sorted.reduce(operation, initial)
    }
}

_registerContextMapFactories(
    (key, element) => new CooperationContextMap(new Map(), KeyedElementMap.of([key, element])),
    () => new CooperationContextMap(new Map(), new KeyedElementMap()),
)
