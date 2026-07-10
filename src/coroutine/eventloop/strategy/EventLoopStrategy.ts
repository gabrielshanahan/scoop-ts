import {
    absoluteDeadlineMissed,
    cancellationRequested,
    happyPathDeadlineMissed,
    ignoreHierarchiesOlderThan,
    rollbackDeadlineMissed,
} from "./strategyBuilders.js"

/**
 * Defines when and how sagas should be started, resumed or given up on, based on their current
 * state. Each method returns SQL fragments that are spliced into the larger queries that
 * determine which sagas are ready to execute (see PendingCoroutineRunSql / finalSelect).
 *
 * This is also where the "who is listening" problem is solved — resumeHappyPath /
 * resumeRollbackPath must check that all expected handlers have reacted to all emissions in the
 * previous step. Scoop provides a dummy HandlerRegistry-based solution.
 */
export interface EventLoopStrategy {
    /** Determines which emitted messages should have SEEN events created. */
    start(emitted: string): string

    /** Determines when a saga in normal execution can resume to its next step. */
    resumeHappyPath(candidateSeen: string, emittedInLatestStep: string, childSeens: string): string

    /**
     * Determines when a saga in normal execution should be cancelled/give up. Returns SQL
     * selecting JSONBs of CooperationFailures under the column `exception`.
     */
    giveUpOnHappyPath(seen: string): string

    /** Determines when a saga in rollback mode can resume to its next rollback step. */
    resumeRollbackPath(
        candidateSeen: string,
        rollbacksEmittedInLatestStep: string,
        childRollingBacks: string,
    ): string

    /** Determines when a saga in rollback mode should be cancelled/give up. */
    giveUpOnRollbackPath(seen: string): string
}

/**
 * Base implementation providing the standard cancellation and deadline give-up logic, plus a
 * pedagogical implementation of `start`. Subclasses only implement the core resumption logic.
 *
 * @param ignoreOlderThan ISO timestamp; messages from hierarchies older than this are ignored
 */
export abstract class BaseEventLoopStrategy implements EventLoopStrategy {
    constructor(readonly ignoreOlderThan: string) {}

    start(_emitted: string): string {
        return ignoreHierarchiesOlderThan(this.ignoreOlderThan)
    }

    abstract resumeHappyPath(
        candidateSeen: string,
        emittedInLatestStep: string,
        childSeens: string,
    ): string

    abstract resumeRollbackPath(
        candidateSeen: string,
        rollbacksEmittedInLatestStep: string,
        childRollingBacks: string,
    ): string

    giveUpOnHappyPath(seen: string): string {
        return `${cancellationRequested(seen)}

UNION ALL

${happyPathDeadlineMissed(seen)}

UNION ALL

${absoluteDeadlineMissed(seen)}`
    }

    giveUpOnRollbackPath(seen: string): string {
        return `${rollbackDeadlineMissed(seen)}

UNION ALL

${absoluteDeadlineMissed(seen)}`
    }
}
