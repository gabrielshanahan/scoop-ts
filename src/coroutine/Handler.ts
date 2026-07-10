import type { DistributedCoroutine } from "./DistributedCoroutine.js"
import type { Topic } from "./Topic.js"

/**
 * A type-safe identifier that binds together a handler name, its topic, and its implementation.
 *
 * The Kotlin original derives the handler name from the singleton's class simple name; here it is
 * passed explicitly. Subclass (or instantiate a subclass of) this and implement [implementation].
 *
 * Example:
 * ```ts
 * const OrderProcessor = new (class extends Handler<OrderRequest> {
 *     implementation() {
 *         return saga(this.handlerName, eventLoopStrategy, b => { ... })
 *     }
 * })("OrderProcessor", Orders)
 * ```
 */
export abstract class Handler<P> {
    constructor(
        readonly handlerName: string,
        readonly topic: Topic<P>,
    ) {}

    abstract implementation(): DistributedCoroutine
}
