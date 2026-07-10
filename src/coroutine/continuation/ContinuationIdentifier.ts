import type { DistributedCoroutineIdentifier } from "../DistributedCoroutineIdentifier.js"
import type { ChildFailureHandlerIteration } from "../eventloop/SuspensionState.js"

/**
 * ContinuationId = CoroutineId + StepId + stepIteration + childFailureHandlerIteration —
 * uniquely identifies a specific continuation within the distributed coroutine execution.
 */
export interface ContinuationIdentifier {
    readonly stepName: string
    readonly stepIteration: number
    readonly childFailureHandlerIteration: ChildFailureHandlerIteration
    readonly distributedCoroutineIdentifier: DistributedCoroutineIdentifier
}
