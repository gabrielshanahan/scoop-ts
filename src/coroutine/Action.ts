import type { JsonbHelper } from "../JsonbHelper.js"
import type { ActionInput } from "./ActionInput.js"
import type { CooperationScope } from "./CooperationScope.js"
import { Handler } from "./Handler.js"

/**
 * A handler that returns a value to its caller. Actions use [ActionTopic]s, whose payloads are
 * wrapped in [ActionInput] so callers always specify where the result should be stored.
 *
 * Type parameters: I = input payload type, O = output result type.
 */
export abstract class Action<I, O> extends Handler<ActionInput<I>> {
    /** JsonbHelper for serializing output to JSONB. Must be provided by subclasses. */
    protected abstract readonly jsonbHelper: JsonbHelper

    /**
     * Type-safe method to store action output. Actions must use this instead of raw
     * `scope.storeReturnValue` so the output type matches the declared O parameter.
     */
    protected storeActionResult(
        scope: CooperationScope,
        input: ActionInput<I>,
        output: O,
    ): Promise<void> {
        return scope.storeReturnValue(input.returnValueVariableName, output)
    }
}
