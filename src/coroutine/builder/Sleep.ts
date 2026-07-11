import type { Message } from "../../messaging/Message.js"
import { isoFromNowMillis } from "../../util/Clock.js"
import type { CooperationScope } from "../CooperationScope.js"
import { has, MappedElement, MappedKey } from "../context/CooperationContext.js"
import { BaseEventLoopStrategy } from "../eventloop/strategy/EventLoopStrategy.js"
import type { SagaBuilder } from "./SagaBuilder.js"

/**
 * Scheduling and delay functionality: sagas emit messages to a special sleep topic whose handler
 * uses [SleepEventLoopStrategy] to resume only when the specified time has elapsed. Enables
 * sleeping steps, scheduled steps, and periodic (self-restarting) sagas.
 */

/**
 * Special topic for sleep/scheduling messages, automatically subscribed by PostgresMessageQueue
 * with a dedicated sleep handler. The UUID suffix prevents collisions with user topics.
 */
export const SLEEP_TOPIC = "sleep-9d24148d-d851-4107-8beb-e5c57f5cca88"

/** Context element specifying when a saga should wake up from sleep. */
export class SleepUntil extends MappedElement {
    constructor(readonly wakeAfter: string) {
        super(SleepUntilKey)
    }
}

/** Context key for sleep/scheduling information. */
export const SleepUntilKey = new MappedKey<SleepUntil>(
    "SleepUntilKey",
    json => new SleepUntil(json.wakeAfter),
)

/** Creates a [SleepUntil] waking up [durationMillis] from now. */
export function sleepFor(durationMillis: number): SleepUntil {
    return new SleepUntil(isoFromNowMillis(durationMillis))
}

/** Creates a [SleepUntil] waking up at the given absolute ISO timestamp. */
export function sleepUntil(wakeAfter: string): SleepUntil {
    return new SleepUntil(wakeAfter)
}

/**
 * Event loop strategy for the sleep handler: resumes on the happy path only once the wake time
 * has passed (checked against the database clock); rollbacks resume immediately (sleep handlers
 * never emit anything).
 */
export class SleepEventLoopStrategy extends BaseEventLoopStrategy {
    resumeHappyPath(candidateSeen: string): string {
        return `EXISTS (
    SELECT 1
        FROM ${candidateSeen}
        WHERE jsonb_exists_any_indexed(${candidateSeen}.context, 'SleepUntilKey')
            AND (${candidateSeen}.context->'SleepUntilKey'->>'wakeAfter')::timestamptz < CLOCK_TIMESTAMP()
)`
    }

    resumeRollbackPath(): string {
        return "TRUE"
    }
}

/** Creates a saga step that pauses execution for [durationMillis]. */
export function sleepForStep(
    builder: SagaBuilder,
    durationMillis: number,
    name: string = String(builder.steps.length),
): void {
    builder.step({
        name,
        invoke: async (scope, _message) => {
            await scope.launch(SLEEP_TOPIC, {}, sleepFor(durationMillis))
        },
    })
}

/**
 * Creates a scheduled step executing at a specific absolute time: a sleep step waiting until
 * [wakeAfter] (ISO timestamp), then the logic step.
 */
export function scheduledStep(
    builder: SagaBuilder,
    name: string,
    wakeAfter: string,
    invoke: (scope: CooperationScope, message: Message) => Promise<void> | void,
    rollback?: (
        scope: CooperationScope,
        message: Message,
        throwable: Error,
    ) => Promise<void> | void,
    handleChildFailures?: (
        scope: CooperationScope,
        message: Message,
        throwable: Error,
    ) => Promise<void> | void,
): void {
    builder.step({
        name: `${name} (waiting for scheduled time)`,
        invoke: async (scope, _message) => {
            await scope.launch(SLEEP_TOPIC, {}, sleepUntil(wakeAfter))
        },
    })
    builder.step({ name, invoke, rollback, handleChildFailures })
}

/** Context element tracking the current execution count for periodic tasks. */
export class RunCount extends MappedElement {
    constructor(readonly value: number = 0) {
        super(RunCountKey)
    }
}

/** Context key for tracking execution count in periodic tasks. */
export const RunCountKey = new MappedKey<RunCount>("RunCountKey", json => new RunCount(json.value))

/**
 * Turns the current saga into a periodic task: sleep for [runEveryMillis], then (if the run
 * counter hasn't reached [runCount]) launch the same message again on the global scope as a new,
 * independent run. "Schedule first" semantics: the next run is guaranteed even if the current run
 * fails, at the cost of possible overlap when execution time exceeds the period.
 */
export function periodic(
    builder: SagaBuilder,
    runEveryMillis: number,
    runCount: number,
    name: string = String(builder.steps.length),
): void {
    sleepForStep(builder, runEveryMillis, `${name} (sleep)`)
    builder.step({
        name: `${name} (launch next)`,
        invoke: async (scope, message) => {
            if (!has(scope.context, RunCountKey)) {
                scope.context = scope.context.plus(new RunCount(0))
            }

            scope.context = scope.context.plus(
                new RunCount(scope.context.get(RunCountKey)!.value + 1),
            )

            if (scope.context.get(RunCountKey)!.value < runCount) {
                await scope.launchOnGlobalScope(message.topic, message.payload, scope.context)
            }
        },
    })
}
