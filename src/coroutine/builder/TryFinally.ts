import type { Message } from "../../messaging/Message.js"
import type { CooperationScope } from "../CooperationScope.js"
import { type CooperationContext, MappedElement, MappedKey } from "../context/CooperationContext.js"
import type { SagaBuilder } from "./SagaBuilder.js"

/**
 * Try-finally semantics for distributed sagas using CooperationContext tracking: "finally" blocks
 * run exactly once, whether the saga completes successfully or rolls back. [tryFinallyStep]
 * creates two actual saga steps — the try step (with exception handling and a rollback that runs
 * the finally if needed) and the finally step.
 */

/**
 * Context element tracking which finally blocks have been executed; combining merges the lists.
 */
export class TryFinallyElement extends MappedElement {
    constructor(readonly finallyRunForSteps: string[]) {
        super(TryFinallyKey)
    }

    override plus(context: CooperationContext): CooperationContext {
        if (context instanceof TryFinallyElement) {
            return new TryFinallyElement([
                ...this.finallyRunForSteps,
                ...context.finallyRunForSteps,
            ])
        }
        return super.plus(context)
    }
}

/** Context key for tracking executed finally blocks. */
export const TryFinallyKey = new MappedKey<TryFinallyElement>(
    "TryFinallyKey",
    json => new TryFinallyElement(json.finallyRunForSteps),
)

/** Whether the finally block for the given step has already been executed. */
export function finallyRun(scope: CooperationScope, name: string): boolean {
    return scope.context.get(TryFinallyKey)?.finallyRunForSteps.includes(name) ?? false
}

/** Marks the finally block for the given step as executed. */
export function markFinallyRun(scope: CooperationScope, name: string): void {
    scope.context = scope.context.plus(new TryFinallyElement([name]))
}

/**
 * Creates a try-finally step pattern in a saga:
 * - normal execution: finally runs in the second step after invoke completes
 * - exception in invoke: finally runs immediately, then the exception is re-thrown
 * - rollback: the try step's rollback runs finally if it hasn't run yet
 */
export function tryFinallyStep(
    builder: SagaBuilder,
    invoke: (scope: CooperationScope, message: Message) => Promise<void> | void,
    finallyBlock: (scope: CooperationScope, message: Message) => Promise<void> | void,
): void {
    const name = String(builder.steps.length)
    builder.step({
        invoke: async (scope, message) => {
            try {
                await invoke(scope, message)
            } catch (e) {
                // In case the handler itself throws (not a child scope), we still want to run the
                // `finally` block, but there's no point in checks or markings, since we never
                // leave the step
                await finallyBlock(scope, message)
                throw e
            }
        },
        rollback: async (scope, message, _throwable) => {
            if (!finallyRun(scope, name)) {
                markFinallyRun(scope, name)
                await finallyBlock(scope, message)
            }
        },
    })

    builder.step({
        invoke: async (scope, message) => {
            if (!finallyRun(scope, name)) {
                markFinallyRun(scope, name)
                await finallyBlock(scope, message)
            }
        },
    })
}
