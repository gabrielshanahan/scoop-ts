import { nowIso } from "../util/Clock.js"
import { StandardEventLoopStrategy } from "../coroutine/eventloop/strategy/StandardEventLoopStrategy.js"

/**
 * Solves (as a placeholder) the "who is listening" problem: structured cooperation requires
 * knowing which handlers exist for each topic to determine when all message emissions have
 * corresponding handler starts.
 */
export interface HandlerRegistry {
    /** Mapping of topics to handler names that listen to those topics. */
    listenersByTopic(): Map<string, string[]>
}

/** Creates a [StandardEventLoopStrategy] using this registry's listener topology. */
export function eventLoopStrategy(registry: HandlerRegistry): StandardEventLoopStrategy {
    return new StandardEventLoopStrategy(nowIso(), () => registry.listenersByTopic())
}
