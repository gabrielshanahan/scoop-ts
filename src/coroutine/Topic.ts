/**
 * A type-safe identifier for a message topic/channel.
 *
 * The type parameter P represents the payload type that messages on this topic carry (for
 * documentation purposes only, as in the original). The Kotlin original derives the serialized
 * value from the singleton's class simple name; here the name is passed explicitly.
 *
 * Example:
 * ```ts
 * const Orders = new Topic<OrderRequest>("Orders")
 * ```
 */
export class Topic<P> {
    declare readonly _payload?: P

    constructor(readonly serializedValue: string) {}
}
