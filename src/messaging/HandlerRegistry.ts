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

/**
 * Creates a [StandardEventLoopStrategy] using this registry's listener topology.
 *
 * [ignoreOlderThan] anchors the "ignore hierarchies older than" cutoff. It defaults to the
 * client clock, which is subject to client-vs-database clock skew: a cutoff even a few
 * milliseconds ahead of the database clock permanently ignores a message emitted right after
 * the saga is built. Callers that can obtain a database-clock timestamp (tests do) should pass
 * it explicitly (see DECISIONS.md).
 */
export function eventLoopStrategy(
    registry: HandlerRegistry,
    ignoreOlderThan: string = nowIso(),
): StandardEventLoopStrategy {
    return new StandardEventLoopStrategy(ignoreOlderThan, () => registry.listenersByTopic())
}
