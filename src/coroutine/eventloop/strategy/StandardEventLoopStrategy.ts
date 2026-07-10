import { BaseEventLoopStrategy } from "./EventLoopStrategy.js"
import { allEmissionsHaveCorrespondingContinuationStarts } from "./strategyBuilders.js"

/**
 * Default implementation of structured cooperation execution policy: sagas suspend after emitting
 * messages and don't continue until all handlers of those messages have finished executing.
 *
 * @param ignoreOlderThan ISO timestamp; messages older than this are ignored
 * @param getTopicsToHandlerNames provides handler topology (topics to handler names)
 */
export class StandardEventLoopStrategy extends BaseEventLoopStrategy {
    constructor(
        ignoreOlderThan: string,
        readonly getTopicsToHandlerNames: () => Map<string, string[]>,
    ) {
        super(ignoreOlderThan)
    }

    resumeHappyPath(
        candidateSeen: string,
        emittedInLatestStep: string,
        childSeens: string,
    ): string {
        return allEmissionsHaveCorrespondingContinuationStarts(
            this.getTopicsToHandlerNames(),
            candidateSeen,
            emittedInLatestStep,
            childSeens,
        )
    }

    resumeRollbackPath(
        candidateSeen: string,
        rollbacksEmittedInLatestStep: string,
        childRollingBacks: string,
    ): string {
        return allEmissionsHaveCorrespondingContinuationStarts(
            this.getTopicsToHandlerNames(),
            candidateSeen,
            rollbacksEmittedInLatestStep,
            childRollingBacks,
        )
    }
}
