import type { ActionInput } from "./ActionInput.js"
import { Topic } from "./Topic.js"

/**
 * A topic for actions (handlers that return values). Wraps the payload type in [ActionInput],
 * ensuring messages always include a return value variable name alongside the payload.
 */
export class ActionTopic<P> extends Topic<ActionInput<P>> {}
