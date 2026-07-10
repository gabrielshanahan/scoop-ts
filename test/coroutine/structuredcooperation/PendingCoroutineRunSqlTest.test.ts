import assert from "node:assert/strict"
import { createHash, randomUUID } from "node:crypto"
import { describe, test } from "node:test"
import { ContinuationIdentifier } from "../../../src/coroutine/continuation/ContinuationIdentifier.js"
import {
    ROLLING_BACK_CHILD_SCOPES_STEP_SUFFIX,
    ROLLING_BACK_PREFIX,
} from "../../../src/coroutine/continuation/RollbackPathContinuation.js"
import { DistributedCoroutineIdentifier } from "../../../src/coroutine/DistributedCoroutineIdentifier.js"
import { NO_CHILD_FAILURE } from "../../../src/coroutine/eventloop/SuspensionState.js"
import type { EventLoopStrategy } from "../../../src/coroutine/eventloop/strategy/EventLoopStrategy.js"
import { StandardEventLoopStrategy } from "../../../src/coroutine/eventloop/strategy/StandardEventLoopStrategy.js"
import type { CooperationFailure } from "../../../src/coroutine/structuredcooperation/CooperationFailure.js"
import {
    buildSql,
    candidateSeens,
    candidateSeensWaitingToBeProcessed,
    childEmissionsInLatestStep,
    childRollbackEmissionsInLatestStep,
    childRollingBacks,
    childSeens,
    finalSelect,
    latestSuspended,
    seenForProcessing,
    SQL,
    terminatedChildRollingBacks,
    terminatedChildSeens,
} from "../../../src/coroutine/structuredcooperation/PendingCoroutineRunSql.js"
import { transactional } from "../../../src/coroutine/TransactionRunner.js"
import { queryNamed } from "../../../src/sql.js"
import { nowIso } from "../../../src/util/Clock.js"
import type { Message } from "../../../src/messaging/Message.js"
import { setupScoopTest } from "../../support/harness.js"
import { SqlTestUtils } from "../../support/SqlTestUtils.js"

const h = setupScoopTest()

type Row = Record<string, unknown>

const rootHandler = "root-handler"
const rootTopic = "root-topic"

const childTopic1 = "child-topic-1"
const childTopic2 = "child-topic-1"
const childHandler1 = "child-handler-1"
const childHandler2 = "child-handler-2"
const childHandler3 = "child-handler-3"
const childHandler4 = "child-handler-4"

function step(
    identifier: DistributedCoroutineIdentifier,
    stepName: string,
): ContinuationIdentifier {
    return {
        stepName,
        stepIteration: 0,
        childFailureHandlerIteration: NO_CHILD_FAILURE,
        distributedCoroutineIdentifier: identifier,
    }
}

function assertEqualInAnyOrder<T>(actual: T[], expected: T[], message: string): void {
    const containsAll = (a: T[], b: T[]) => b.every(item => a.includes(item))
    assert.ok(containsAll(actual, expected) && containsAll(expected, actual), message)
}

/** The Java `UUID.nameUUIDFromBytes` (md5-based v3 UUID) — deterministic scope generation. */
function nameUUIDFromBytes(input: string): string {
    const digest = createHash("md5").update(input, "utf-8").digest()
    digest[6] = (digest[6]! & 0x0f) | 0x30 // version 3
    digest[8] = (digest[8]! & 0x3f) | 0x80 // RFC4122 variant
    const hex = digest.toString("hex")
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`
}

function utils(): SqlTestUtils {
    return new SqlTestUtils(h.sql, h.jsonbHelper)
}

class CoroutineProgressBuilder {
    readonly scope: string[]

    constructor(
        readonly message: Message,
        readonly distributedCoroutineIdentifier: DistributedCoroutineIdentifier,
        parentScope: string[] = [],
    ) {
        // Generate the same scope for the same (message, coroutine) pair
        this.scope = [
            ...parentScope,
            nameUUIDFromBytes(message.id + distributedCoroutineIdentifier.name),
        ]
    }

    childScopeRollback(stepName: string | number): string {
        return ROLLING_BACK_PREFIX + String(stepName) + ROLLING_BACK_CHILD_SCOPES_STEP_SUFFIX
    }

    rollback(stepName: string | number): string {
        return ROLLING_BACK_PREFIX + String(stepName)
    }

    async emitted(topic: string, stepName: string | number, key = "key", value = "value"): Promise<Message> {
        const message = await utils().createSimpleMessage(topic, key, value)
        await utils().emitted(
            message.id,
            step(this.distributedCoroutineIdentifier, String(stepName)),
            this.scope,
        )
        return message
    }

    async childCoroutineProgress(
        childMessage: Message,
        name: string,
        block: (builder: CoroutineProgressBuilder) => Promise<void>,
    ): Promise<Message> {
        const builder = new CoroutineProgressBuilder(
            childMessage,
            new DistributedCoroutineIdentifier(name, randomUUID()),
            this.scope,
        )
        await block(builder)
        return childMessage
    }

    seen(): Promise<string> {
        return utils().seen(this.message.id, this.distributedCoroutineIdentifier, this.scope)
    }

    suspended(stepName: string | number): Promise<string> {
        return utils().suspended(
            this.message.id,
            step(this.distributedCoroutineIdentifier, String(stepName)),
            this.scope,
        )
    }

    committed(stepName: string | number): Promise<string> {
        return utils().committed(
            this.message.id,
            step(this.distributedCoroutineIdentifier, String(stepName)),
            this.scope,
        )
    }

    rollingBack(throwable: Error, stepName: string | number | null = null): Promise<string> {
        if (stepName === null) {
            return utils().rollingBackCoroutine(
                this.message.id,
                this.distributedCoroutineIdentifier,
                this.scope,
                throwable,
            )
        }
        return utils().rollingBack(
            this.message.id,
            step(this.distributedCoroutineIdentifier, String(stepName)),
            this.scope,
            throwable,
        )
    }

    rollbackEmitted(
        childMessage: Message,
        stepName: string | number,
        throwable: Error,
    ): Promise<string> {
        return utils().rollbackEmitted(
            childMessage.id,
            step(this.distributedCoroutineIdentifier, String(stepName)),
            this.scope,
            throwable,
        )
    }

    rolledBack(stepName: string | number): Promise<string> {
        return utils().rolledBack(
            this.message.id,
            step(this.distributedCoroutineIdentifier, String(stepName)),
            this.scope,
        )
    }

    rollbackFailed(stepName: string | number, throwable: Error): Promise<string> {
        return utils().rollbackFailed(
            this.message.id,
            step(this.distributedCoroutineIdentifier, String(stepName)),
            this.scope,
            throwable,
        )
    }

    async verify(sql: SQL, block: (result: Row[]) => void): Promise<void> {
        const result = await queryNamed(h.sql, buildSql(sql), {
            coroutine_name: this.distributedCoroutineIdentifier.name,
        })
        block(result)
    }
}

async function emitRootMessage(topic: string, key = "key", value = "value"): Promise<Message> {
    const message = await utils().createSimpleMessage(topic, key, value)
    await utils().emitted(message.id)
    return message
}

async function coroutineProgress(
    message: Message,
    name: string,
    block: (builder: CoroutineProgressBuilder) => Promise<void>,
): Promise<Message> {
    const builder = new CoroutineProgressBuilder(
        message,
        new DistributedCoroutineIdentifier(name, randomUUID()),
    )
    await block(builder)
    return message
}

function assertNoSeen(result: Row[]): void {
    assert.ok(result.length === 0, `no SEEN should be picked up, but got ${JSON.stringify(result)}`)
}

function assertSeen(result: Row[], ...seenIds: string[]): void {
    const actual = result.map(row => row.id as string)
    assertEqualInAnyOrder(actual, seenIds, `SEEN id should be ${seenIds}, but was ${actual}`)
}

describe("PendingCoroutineRunSqlTest", () => {
    describe("CandidateSeenTest", () => {
        function assertRollbackEmittedPresent(result: Row[]): void {
            assert.equal(result.length, 1)
            assert.ok(
                result[0]!.rollback_emitted_at !== null &&
                    result[0]!.rollback_emitted_at !== undefined,
                "rollback_emitted should be present",
            )
        }

        test("picks up SEEN", async () => {
            await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                const seenId = await c.seen()

                await c.verify(candidateSeens, result => assertSeen(result, seenId))
            })
        })

        test("picks up SUSPENDED", async () => {
            await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                const seenId = await c.seen()
                await c.suspended(0)

                await c.verify(candidateSeens, result => assertSeen(result, seenId))
            })
        })

        test("picks up ROLLING_BACK", async () => {
            const throwable = new Error("A thing happened")
            await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                const seenId = await c.seen()
                await c.rollingBack(throwable, 0)

                await c.verify(candidateSeens, result => assertSeen(result, seenId))
            })
        })

        test("picks up combinations", async () => {
            let seenId1!: string
            let seenId2!: string
            const throwable = new Error("A thing happened")

            await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                seenId1 = await c.seen()
            })

            await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                seenId2 = await c.seen()
                await c.suspended(0)
            })

            await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                const seenId3 = await c.seen()
                await c.rollingBack(throwable, 0)
                await c.verify(candidateSeens, result =>
                    assertSeen(result, seenId1, seenId2, seenId3),
                )
            })
        })

        test("picks up COMMITTED with parent ROLLBACK_EMITTED", async () => {
            let childSeenId!: string
            const throwable = new Error("A thing happened")

            await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                const seenId = await c.seen()
                const childMessage = await c.emitted(childTopic1, 0)
                await c.suspended(0)
                await c.childCoroutineProgress(childMessage, childHandler1, async child => {
                    childSeenId = await child.seen()
                    await child.suspended(0)
                    await child.committed(0)
                })
                await c.rollingBack(throwable, 1)
                await c.rollbackEmitted(childMessage, c.childScopeRollback(0), throwable)
                await c.suspended(c.childScopeRollback(0))

                await c.childCoroutineProgress(childMessage, childHandler1, async child => {
                    await child.rollingBack(throwable)
                    await child.verify(candidateSeens, result => {
                        assertSeen(result, childSeenId)
                        assertRollbackEmittedPresent(result)
                    })
                })

                await c.verify(candidateSeens, result => assertSeen(result, seenId))
            })
        })

        test("does not pick up COMMITED without parent ROLLING_BACK", async () => {
            await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                await c.seen()
                await c.suspended(0)
                await c.committed(0)

                await c.verify(candidateSeens, result => assertNoSeen(result))
            })
        })

        test("does not pick up rolled back", async () => {
            const throwable = new Error("A thing happened")
            await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                await c.seen()
                await c.suspended(0)
                await c.rollingBack(throwable, 0)
                await c.suspended(c.childScopeRollback(0))
                await c.suspended(c.rollback(0))
                await c.rolledBack(c.rollback(0))

                await c.verify(candidateSeens, result => assertNoSeen(result))
            })
        })

        test("does not pick up rollback failed", async () => {
            const throwable = new Error("A thing happened")
            await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                await c.seen()
                await c.suspended(0)
                await c.rollingBack(throwable, 0)
                await c.suspended(c.childScopeRollback(0))
                await c.rollbackFailed(c.childScopeRollback(0), throwable)

                await c.verify(candidateSeens, result => assertNoSeen(result))
            })
        })
    })

    describe("LatestSuspendedTest", () => {
        function assertSuspendedAbsent(result: Row[]): void {
            assert.ok(result.length === 0, "SUSPENDED row shouldn't be present")
        }

        function assertSuspendedStepIs(result: Row[], stepName: string | number): void {
            assert.equal(result.length, 1)
            const actual = result[0]!.step as string
            assert.ok(
                actual === String(stepName),
                `SUSPENDED step should be ${stepName}, but is ${actual}`,
            )
        }

        test("picks up latest suspended", async () => {
            await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                await c.seen()
                await c.suspended(0)

                await c.verify(latestSuspended, result => assertSuspendedStepIs(result, 0))
            })
        })

        test("picks up latest suspended after rollback", async () => {
            const throwable = new Error("A thing happened")
            await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                await c.seen()
                await c.suspended(0)
                await c.rollingBack(throwable, 0)

                await c.verify(latestSuspended, result => assertSuspendedStepIs(result, 0))
            })
        })

        test("works when no suspended is present", async () => {
            await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                await c.seen()
                await c.verify(latestSuspended, result => assertSuspendedAbsent(result))
            })
        })
    })

    describe("ChildEmissionsInLatestStep", () => {
        function assertNothingWasEmitted(result: Row[]): void {
            assert.ok(result.length === 0, "no EMITTED rows should be present")
        }

        function assertWasEmitted(result: Row[], ...messageIds: string[]): void {
            const actual = result.map(row => row.message_id as string)
            assertEqualInAnyOrder(
                actual,
                messageIds,
                `expected ${messageIds} to be emitted, but was ${actual}`,
            )
        }

        test("picks up child emissions in last step", async () => {
            await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                await c.seen()
                const childMessage1 = await c.emitted(childTopic1, 0)
                const childMessage2 = await c.emitted(childTopic1, 0)
                await c.suspended(0)

                await c.verify(childEmissionsInLatestStep, result =>
                    assertWasEmitted(result, childMessage1.id, childMessage2.id),
                )
            })
        })

        test("does nothing when no emissions", async () => {
            await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                await c.seen()
                await c.suspended(0)

                await c.verify(childEmissionsInLatestStep, result =>
                    assertNothingWasEmitted(result),
                )
            })
        })
    })

    describe("ChildSeens", () => {
        function assertParentLineagesIs(result: Row[], lineage: string[]): void {
            for (const row of result) {
                const parentLineage = row.parent_cooperation_lineage as string[]
                assert.ok(
                    JSON.stringify(parentLineage) === JSON.stringify(lineage),
                    `expected ${lineage} as parent lineages, but was ${parentLineage}`,
                )
            }
        }

        test("picks up child seens and their terminations", async () => {
            let childSeenId1!: string
            let childSeenId2!: string
            let childSeenId3!: string
            let childSeenId4!: string
            const throwable = new Error("A thing happened")
            await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                await c.seen()
                const childMessage1 = await c.emitted(childTopic1, 0)
                const childMessage2 = await c.emitted(childTopic2, 0)
                await c.suspended(0)

                await c.childCoroutineProgress(childMessage1, childHandler1, async child => {
                    childSeenId1 = await child.seen()
                    await child.suspended(0)
                    await child.committed(0)
                })

                await c.childCoroutineProgress(childMessage1, childHandler2, async child => {
                    childSeenId2 = await child.seen()
                    await child.suspended(0)
                })

                await c.childCoroutineProgress(childMessage2, childHandler3, async child => {
                    childSeenId3 = await child.seen()
                    await child.rollingBack(throwable, 0)
                    await child.rolledBack(0)
                })

                await c.childCoroutineProgress(childMessage2, childHandler4, async child => {
                    childSeenId4 = await child.seen()
                    await child.rollingBack(throwable, 0)
                    await child.rollbackFailed(0, throwable)
                })

                await c.verify(childSeens, result => {
                    assertSeen(result, childSeenId1, childSeenId2, childSeenId3, childSeenId4)
                    assertParentLineagesIs(result, c.scope)
                })

                await c.verify(terminatedChildSeens, result => {
                    assertSeen(result, childSeenId1, childSeenId3, childSeenId4)
                })
            })
        })
    })

    describe("ChildRollbackEmissionsInLatestStep", () => {
        function assertNothingWasEmitted(result: Row[]): void {
            assert.ok(result.length === 0, "no ROLLBACK_EMMITED rows should be present")
        }

        function assertWasEmitted(result: Row[], ...messageIds: string[]): void {
            const actual = result.map(row => row.message_id as string)
            assertEqualInAnyOrder(
                actual,
                messageIds,
                `expected ${messageIds} to be emitted, but was ${actual}`,
            )
        }

        test("picks up rollback emissions in last step", async () => {
            const throwable = new Error("A thing happened")
            await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                await c.seen()
                const childMessage = await c.emitted(childTopic1, 0)
                await c.suspended(0)
                await c.childCoroutineProgress(childMessage, childHandler1, async child => {
                    await child.seen()
                    await child.suspended(0)
                    await child.committed(0)
                })
                await c.rollingBack(throwable, 1)
                await c.rollbackEmitted(childMessage, c.childScopeRollback(0), throwable)
                await c.suspended(c.childScopeRollback(0))

                await c.verify(childRollbackEmissionsInLatestStep, result =>
                    assertWasEmitted(result, childMessage.id),
                )
            })
        })

        test("does nothing when no emissions", async () => {
            await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                await c.seen()
                await c.suspended(0)

                await c.verify(childRollbackEmissionsInLatestStep, result =>
                    assertNothingWasEmitted(result),
                )
            })
        })
    })

    describe("ChildRollingBacks", () => {
        function assertRollingBacksAre(result: Row[], ...rollingBackIds: string[]): void {
            const actual = result.map(row => row.id as string)
            assertEqualInAnyOrder(
                actual,
                rollingBackIds,
                `expected ${rollingBackIds} to be rolling back, but was ${actual}`,
            )
        }

        function assertParentLineagesIs(result: Row[], lineage: string[]): void {
            for (const row of result) {
                const parentLineage = row.parent_cooperation_lineage as string[]
                assert.ok(
                    JSON.stringify(parentLineage) === JSON.stringify(lineage),
                    `expected ${lineage} as parent lineages, but was ${parentLineage}`,
                )
            }
        }

        test("picks up child rolling backs and their terminations", async () => {
            let childRollingBackId1!: string
            let childRollingBackId2!: string
            let childRollingBackId3!: string
            let childRollingBackId4!: string
            await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                const throwable = new Error("A thing happened")
                await c.seen()
                const childMessage1 = await c.emitted(childTopic1, 0)
                const childMessage2 = await c.emitted(childTopic2, 0)
                await c.suspended(0)

                await c.childCoroutineProgress(childMessage1, childHandler1, async child => {
                    await child.seen()
                    await child.suspended(0)
                    await child.committed(0)
                })

                await c.childCoroutineProgress(childMessage1, childHandler2, async child => {
                    await child.seen()
                    await child.suspended(0)
                    await child.committed(0)
                })

                await c.childCoroutineProgress(childMessage2, childHandler3, async child => {
                    await child.seen()
                    childRollingBackId3 = await child.rollingBack(throwable, 0)
                    await child.rolledBack(0)
                })

                await c.childCoroutineProgress(childMessage2, childHandler4, async child => {
                    await child.seen()
                    await child.committed(0)
                })
                await c.rollingBack(throwable, 1)
                await c.rollbackEmitted(childMessage1, c.childScopeRollback(0), throwable)
                await c.rollbackEmitted(childMessage2, c.childScopeRollback(0), throwable)
                await c.suspended(c.childScopeRollback(0))

                await c.childCoroutineProgress(childMessage1, childHandler1, async child => {
                    childRollingBackId1 = await child.rollingBack(throwable)
                    await child.suspended(child.childScopeRollback(0))
                    await child.suspended(child.rollback(0))
                    await child.rolledBack(child.rollback(0))
                })

                await c.childCoroutineProgress(childMessage1, childHandler2, async child => {
                    childRollingBackId2 = await child.rollingBack(throwable)
                    await child.suspended(child.childScopeRollback(0))
                })

                await c.childCoroutineProgress(childMessage2, childHandler4, async child => {
                    childRollingBackId4 = await child.rollingBack(throwable)
                    await child.rollbackFailed(child.childScopeRollback(0), throwable)
                })

                await c.verify(childRollingBacks, result => {
                    assertRollingBacksAre(
                        result,
                        childRollingBackId1,
                        childRollingBackId2,
                        childRollingBackId3,
                        childRollingBackId4,
                    )
                    assertParentLineagesIs(result, c.scope)
                })

                await c.verify(terminatedChildRollingBacks, result => {
                    assertRollingBacksAre(
                        result,
                        childRollingBackId1,
                        childRollingBackId3,
                        childRollingBackId4,
                    )
                })
            })
        })
    })

    describe("CandidateSeensWaitingToBeProcessed", () => {
        describe("NoStrategy", () => {
            const noStrategy: EventLoopStrategy = {
                start: () => "TRUE",
                resumeHappyPath: () => "TRUE",
                giveUpOnHappyPath: () => "SELECT NULL WHERE FALSE",
                resumeRollbackPath: () => "TRUE",
                giveUpOnRollbackPath: () => "SELECT NULL WHERE FALSE",
            }

            const waiting = candidateSeensWaitingToBeProcessed(noStrategy)

            test("picks up SEEN", async () => {
                await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                    const seenId = await c.seen()

                    await c.verify(waiting, result => assertSeen(result, seenId))
                })
            })

            test("picks up SUSPENDED", async () => {
                await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                    const seenId = await c.seen()
                    await c.suspended(0)

                    await c.verify(waiting, result => assertSeen(result, seenId))
                })
            })

            test("picks up ROLLING_BACK", async () => {
                const throwable = new Error("A thing happened")
                await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                    const seenId = await c.seen()
                    await c.rollingBack(throwable, 0)

                    await c.verify(waiting, result => assertSeen(result, seenId))
                })
            })

            test("picks up combinations", async () => {
                let seenId1!: string
                let seenId2!: string
                const throwable = new Error("A thing happened")

                await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                    seenId1 = await c.seen()
                })

                await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                    seenId2 = await c.seen()
                    await c.suspended(0)
                })

                await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                    const seenId3 = await c.seen()
                    await c.rollingBack(throwable, 0)
                    await c.verify(waiting, result =>
                        assertSeen(result, seenId1, seenId2, seenId3),
                    )
                })
            })

            test("picks up when children committed", async () => {
                await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                    const seenId = await c.seen()
                    const childMessage = await c.emitted(childTopic1, 0)
                    await c.suspended(0)
                    await c.childCoroutineProgress(childMessage, childHandler1, async child => {
                        await child.seen()
                        await child.suspended(0)
                        await child.committed(0)
                        await child.verify(waiting, result => assertNoSeen(result))
                    })

                    await c.verify(waiting, result => assertSeen(result, seenId))
                })
            })

            test("picks up when children rolled back", async () => {
                const throwable = new Error("A thing happened")
                await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                    const seenId = await c.seen()
                    const childMessage = await c.emitted(childTopic1, 0)
                    await c.suspended(0)
                    await c.childCoroutineProgress(childMessage, childHandler1, async child => {
                        await child.seen()
                        await child.rollingBack(throwable, 0)
                        await child.rolledBack(child.rollback(0))
                        await child.verify(waiting, result => assertNoSeen(result))
                    })

                    await c.verify(waiting, result => assertSeen(result, seenId))
                })
            })

            test("picks up COMMITTED with parent ROLLBACK_EMITTED and unfinished child ROLLING_BACK", async () => {
                let childSeenId!: string
                const throwable = new Error("A thing happened")

                await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                    await c.seen()
                    const childMessage = await c.emitted(childTopic1, 0)
                    await c.suspended(0)
                    await c.childCoroutineProgress(childMessage, childHandler1, async child => {
                        childSeenId = await child.seen()
                        await child.suspended(0)
                        await child.committed(0)
                    })
                    await c.rollingBack(throwable, 1)
                    await c.rollbackEmitted(childMessage, c.childScopeRollback(0), throwable)
                    await c.suspended(c.childScopeRollback(0))

                    await c.childCoroutineProgress(childMessage, childHandler1, async child => {
                        // This rollingBack is important, since noStrategy says "no handler is
                        // missing." If this wasn't included, the toplevel seen would get picked
                        // up as well, even though we know its child hasn't finished rolling back
                        await child.rollingBack(throwable)
                        await child.verify(waiting, result => assertSeen(result, childSeenId))
                    })

                    await c.verify(waiting, result => {
                        // Child has a ROLLBACK_EMITTED, so it hasn't finished yet
                        assertNoSeen(result)
                    })
                })
            })

            test("doesn't pick up when there are unfinished children emissions", async () => {
                await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                    await c.seen()
                    const childMessage = await c.emitted(childTopic1, 0)
                    await c.suspended(0)
                    await c.childCoroutineProgress(childMessage, childHandler1, async child => {
                        const childSeenId = await child.seen()
                        await child.suspended(0)
                        await child.verify(waiting, result => assertSeen(result, childSeenId))
                    })

                    await c.verify(waiting, result => assertNoSeen(result))
                })
            })

            test("doesn't pick up when there are unfinished children emissions - 2 deep", async () => {
                await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                    await c.seen()
                    const childMessage = await c.emitted(childTopic1, 0)
                    await c.suspended(0)
                    await c.childCoroutineProgress(childMessage, childHandler1, async child => {
                        await child.seen()
                        const grandchildMessage = await child.emitted(childTopic2, 0)
                        await child.suspended(0)
                        await child.childCoroutineProgress(
                            grandchildMessage,
                            childHandler2,
                            async grandchild => {
                                const grandChildSeenId = await grandchild.seen()
                                await grandchild.suspended(0)
                                await grandchild.verify(waiting, result =>
                                    assertSeen(result, grandChildSeenId),
                                )
                            },
                        )
                        await child.verify(waiting, result => assertNoSeen(result))
                    })

                    await c.verify(waiting, result => assertNoSeen(result))
                })
            })
        })

        describe("RegistryStrategy", () => {
            const registryStrategy = new StandardEventLoopStrategy(
                nowIso(),
                () =>
                    new Map([
                        [rootTopic, [rootHandler]],
                        [childTopic1, [childHandler1, childHandler2]],
                    ]),
            )

            const waiting = candidateSeensWaitingToBeProcessed(registryStrategy)

            test("picks up SEEN", async () => {
                await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                    const seenId = await c.seen()

                    await c.verify(waiting, result => assertSeen(result, seenId))
                })
            })

            test("picks up SUSPENDED", async () => {
                await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                    const seenId = await c.seen()
                    await c.suspended(0)

                    await c.verify(waiting, result => assertSeen(result, seenId))
                })
            })

            test("picks up ROLLING_BACK", async () => {
                const throwable = new Error("A thing happened")
                await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                    const seenId = await c.seen()
                    await c.rollingBack(throwable, 0)

                    await c.verify(waiting, result => assertSeen(result, seenId))
                })
            })

            test("picks up combinations", async () => {
                let seenId1!: string
                let seenId2!: string
                const throwable = new Error("A thing happened")

                await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                    seenId1 = await c.seen()
                    const childMessage = await c.emitted(childTopic1, 0)
                    await c.suspended(0)
                    await c.childCoroutineProgress(childMessage, childHandler1, async child => {
                        await child.seen()
                        await child.suspended(0)
                        await child.committed(0)
                        await child.verify(waiting, result => assertNoSeen(result))
                    })

                    await c.childCoroutineProgress(childMessage, childHandler2, async child => {
                        await child.seen()
                        await child.rollingBack(throwable, 0)
                        await child.rolledBack(child.rollback(0))
                        await child.verify(waiting, result => assertNoSeen(result))
                    })
                })

                await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                    seenId2 = await c.seen()
                    await c.suspended(0)
                })

                await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                    const seenId3 = await c.seen()
                    await c.rollingBack(throwable, 0)
                    await c.verify(waiting, result =>
                        assertSeen(result, seenId1, seenId2, seenId3),
                    )
                })
            })

            test("picks up when all children finished", async () => {
                const throwable = new Error("A thing happened")
                await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                    const seenId = await c.seen()
                    const childMessage = await c.emitted(childTopic1, 0)
                    await c.suspended(0)
                    await c.childCoroutineProgress(childMessage, childHandler1, async child => {
                        await child.seen()
                        await child.suspended(0)
                        await child.committed(0)
                        await child.verify(waiting, result => assertNoSeen(result))
                    })

                    await c.childCoroutineProgress(childMessage, childHandler2, async child => {
                        await child.seen()
                        await child.rollingBack(throwable, 0)
                        await child.rolledBack(child.rollback(0))
                        await child.verify(waiting, result => assertNoSeen(result))
                    })

                    await c.verify(waiting, result => assertSeen(result, seenId))
                })
            })

            test("picks up COMMITTED with parent ROLLBACK_EMITTED and unfinished child ROLLING_BACK", async () => {
                let childSeenId!: string
                const throwable = new Error("A thing happened")

                await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                    await c.seen()
                    const childMessage = await c.emitted(childTopic1, 0)
                    await c.suspended(0)
                    await c.childCoroutineProgress(childMessage, childHandler1, async child => {
                        childSeenId = await child.seen()
                        await child.suspended(0)
                        await child.committed(0)
                    })
                    await c.rollingBack(throwable, 1)
                    await c.rollbackEmitted(childMessage, c.childScopeRollback(0), throwable)
                    await c.suspended(c.childScopeRollback(0))

                    await c.childCoroutineProgress(childMessage, childHandler1, async child => {
                        // This rollingBack is important, since noStrategy says "no handler is
                        // missing." If this wasn't included, the toplevel seen would get picked
                        // up as well, even though we know its child hasn't finished rolling back
                        await child.rollingBack(throwable, child.childScopeRollback(0))
                        await child.verify(waiting, result => assertSeen(result, childSeenId))
                    })

                    await c.verify(waiting, result => {
                        // Child has a ROLLBACK_EMITTED, so it hasn't finished yet
                        assertNoSeen(result)
                    })
                })
            })

            test("doesn't pick up when no child SEENs are present", async () => {
                await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                    await c.seen()
                    await c.emitted(childTopic1, 0)
                    await c.suspended(0)

                    await c.verify(waiting, result => assertNoSeen(result))
                })
            })

            test("doesn't pick up when a child SEEN is missing, even if the rest are finished", async () => {
                await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                    await c.seen()
                    const childMessage = await c.emitted(childTopic1, 0)
                    await c.suspended(0)
                    await c.childCoroutineProgress(childMessage, childHandler1, async child => {
                        await child.seen()
                        await child.suspended(0)
                        await child.committed(0)
                        await child.verify(waiting, result => assertNoSeen(result))
                    })

                    await c.verify(waiting, result => assertNoSeen(result))
                })
            })

            test("doesn't pick up when no child ROLLING_BACKs are present", async () => {
                const throwable = new Error("A thing happened")
                await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                    await c.seen()
                    const childMessage = await c.emitted(childTopic1, 0)
                    await c.suspended(0)
                    await c.childCoroutineProgress(childMessage, childHandler1, async child => {
                        await child.seen()
                        await child.suspended(0)
                        await child.committed(0)
                    })
                    await c.childCoroutineProgress(childMessage, childHandler2, async child => {
                        await child.seen()
                        await child.suspended(0)
                        await child.committed(0)
                    })
                    await c.rollingBack(throwable, 1)
                    await c.rollbackEmitted(childMessage, c.childScopeRollback(0), throwable)
                    await c.suspended(c.childScopeRollback(0))

                    await c.verify(waiting, result => {
                        // Child has a ROLLBACK_EMITTED, so it hasn't finished yet
                        assertNoSeen(result)
                    })
                })
            })

            test("doesn't pick up when a child ROLLING_BACK is missing, even if the rest are finished", async () => {
                const throwable = new Error("A thing happened")
                await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                    await c.seen()
                    const childMessage = await c.emitted(childTopic1, 0)
                    await c.suspended(0)
                    await c.childCoroutineProgress(childMessage, childHandler1, async child => {
                        await child.seen()
                        await child.suspended(0)
                        await child.committed(0)
                    })
                    await c.childCoroutineProgress(childMessage, childHandler2, async child => {
                        await child.seen()
                        await child.suspended(0)
                        await child.committed(0)
                    })
                    await c.rollingBack(throwable, 1)
                    await c.rollbackEmitted(childMessage, c.childScopeRollback(0), throwable)
                    await c.suspended(c.childScopeRollback(0))

                    await c.childCoroutineProgress(childMessage, childHandler1, async child => {
                        await child.rollingBack(throwable)
                        await child.suspended(child.childScopeRollback(0))
                        await child.suspended(child.rollback(0))
                        await child.rolledBack(child.rollback(0))
                    })

                    await c.verify(waiting, result => {
                        // Child has a ROLLBACK_EMITTED, so it hasn't finished yet
                        assertNoSeen(result)
                    })
                })
            })
        })
    })

    describe("SeenForProcessing", () => {
        const registryStrategy = new StandardEventLoopStrategy(
            nowIso(),
            () =>
                new Map([
                    [rootTopic, [rootHandler]],
                    [childTopic1, [childHandler1, childHandler2]],
                ]),
        )

        const seenForProcessingChain = seenForProcessing(registryStrategy)

        function assertSeenIsFor(result: Row[], messageId: string): void {
            assert.equal(result.length, 1)
            const actual = result[0]!.message_id as string
            assert.ok(
                actual === messageId,
                `SEEN message_id should be ${messageId}, but was ${actual}`,
            )
        }

        test("when present, the time of rollback emission time determines precedence", async () => {
            let messageId!: string
            const throwable = new Error("A thing happened")
            await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                await c.seen()
                const childMessage = await c.emitted(childTopic1, 0)
                await c.suspended(0)
                await c.childCoroutineProgress(childMessage, childHandler1, async child => {
                    await child.seen()
                    await child.suspended(0)
                    await child.committed(0)
                })
                await c.childCoroutineProgress(childMessage, childHandler2, async child => {
                    await child.seen()
                    await child.suspended(0)
                    await child.committed(0)
                })

                await coroutineProgress(
                    await emitRootMessage(rootTopic),
                    rootHandler,
                    async other => {
                        await other.seen()
                        const otherChildMessage = await other.emitted(childTopic1, 0)
                        messageId = otherChildMessage.id
                        await other.suspended(0)
                        await other.childCoroutineProgress(
                            otherChildMessage,
                            childHandler1,
                            async child => {
                                await child.seen()
                            },
                        )
                    },
                )

                await c.rollingBack(throwable, 1)
                await c.rollbackEmitted(childMessage, c.childScopeRollback(0), throwable)
                await c.suspended(c.childScopeRollback(0))

                await c.childCoroutineProgress(childMessage, childHandler1, async child => {
                    await child.rollingBack(throwable)
                    await child.verify(seenForProcessingChain, result =>
                        assertSeenIsFor(result, messageId),
                    )
                })
            })
        })

        test("otherwise, first emitted first processed", async () => {
            let messageId!: string
            await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                await c.seen()
                const childMessage = await c.emitted(childTopic1, 0)
                messageId = childMessage.id
                await c.suspended(0)
                await c.childCoroutineProgress(childMessage, childHandler1, async child => {
                    await child.seen()
                    await child.suspended(0)
                })
                await c.childCoroutineProgress(childMessage, childHandler2, async child => {
                    await child.seen()
                    await child.suspended(0)
                    await child.committed(0)
                })

                await coroutineProgress(
                    await emitRootMessage(rootTopic),
                    rootHandler,
                    async other => {
                        await other.seen()
                        const otherChildMessage = await other.emitted(childTopic1, 0)
                        await other.suspended(0)
                        await other.childCoroutineProgress(
                            otherChildMessage,
                            childHandler1,
                            async child => {
                                await child.seen()
                                await child.verify(seenForProcessingChain, result =>
                                    assertSeenIsFor(result, messageId),
                                )
                            },
                        )
                    },
                )
            })
        })

        test("only single transaction can pick up a SEEN", async () => {
            await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                await c.seen()
                await c.verify(seenForProcessingChain, result =>
                    assertSeenIsFor(result, c.message.id),
                )

                const result = await transactional(h.sql, async connection => {
                    const locked = await queryNamed(
                        connection,
                        buildSql(seenForProcessingChain),
                        { coroutine_name: c.distributedCoroutineIdentifier.name },
                    )
                    assertSeenIsFor(locked, c.message.id)
                    // The analog of the Kotlin `thread { verify(...) }.join()`: the same query on
                    // a DIFFERENT connection must skip the locked row.
                    await c.verify(seenForProcessingChain, concurrent => assertNoSeen(concurrent))
                    return locked
                })

                assertSeenIsFor(result, c.message.id)
                await c.verify(seenForProcessingChain, r => assertSeenIsFor(r, c.message.id))
            })
        })
    })

    describe("FinalSelect", () => {
        const registryStrategy = new StandardEventLoopStrategy(
            nowIso(),
            () =>
                new Map([
                    [rootTopic, [rootHandler]],
                    [childTopic1, [childHandler1, childHandler2]],
                ]),
        )

        const finalSelectChain = finalSelect(registryStrategy)

        interface FinalSelectResult {
            anyChildRolledBack: boolean
            anyChildRollbackFailed: boolean
            rollingBack: boolean
        }

        function asResult(result: Row[]): FinalSelectResult {
            assert.equal(result.length, 1)
            const row = result[0]!
            const childRolledBackExceptions = h.jsonbHelper.fromJsonb<CooperationFailure[]>(
                row.child_rolled_back_exceptions,
            )
            const childRollbackFailedExceptions = h.jsonbHelper.fromJsonb<CooperationFailure[]>(
                row.child_rollback_failed_exceptions,
            )
            const rollingBackException =
                row.rolling_back_exception !== null && row.rolling_back_exception !== undefined
                    ? h.jsonbHelper.fromJsonb<CooperationFailure>(row.rolling_back_exception)
                    : null
            return {
                anyChildRolledBack: childRolledBackExceptions.length > 0,
                anyChildRollbackFailed: childRollbackFailedExceptions.length > 0,
                rollingBack: rollingBackException !== null,
            }
        }

        test("happy path", async () => {
            await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                await c.seen()
                const childMessage = await c.emitted(childTopic1, 0)
                await c.suspended(0)

                await c.childCoroutineProgress(childMessage, childHandler1, async child => {
                    await child.seen()
                    await child.suspended(0)
                    await child.committed(0)
                })

                await c.childCoroutineProgress(childMessage, childHandler2, async child => {
                    await child.seen()
                    await child.suspended(0)
                    await child.committed(0)
                })

                await c.verify(finalSelectChain, result => {
                    const parsed = asResult(result)
                    assert.equal(parsed.anyChildRolledBack, false)
                    assert.equal(parsed.anyChildRollbackFailed, false)
                    assert.equal(parsed.rollingBack, false)
                })
            })
        })

        test("picks up children rolling back", async () => {
            const throwable = new Error("A thing happened")
            await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                await c.seen()
                const childMessage = await c.emitted(childTopic1, 0)
                await c.suspended(0)

                await c.childCoroutineProgress(childMessage, childHandler1, async child => {
                    await child.seen()
                    await child.suspended(0)
                    await child.committed(0)
                })

                await c.childCoroutineProgress(childMessage, childHandler2, async child => {
                    await child.seen()
                    await child.rollingBack(throwable, 0)
                    await child.rolledBack(child.rollback(0))
                })

                await c.verify(finalSelectChain, result => {
                    const parsed = asResult(result)
                    assert.equal(parsed.anyChildRolledBack, true)
                    assert.equal(parsed.anyChildRollbackFailed, false)
                    assert.equal(parsed.rollingBack, false)
                })
            })
        })

        test("picks up children rollback failures", async () => {
            const originalThrowable = new Error("A thing happened")
            const rollbackThrowable = new Error("Another thing happened")
            await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                await c.seen()
                const childMessage = await c.emitted(childTopic1, 0)
                await c.suspended(0)

                await c.childCoroutineProgress(childMessage, childHandler1, async child => {
                    await child.seen()
                    await child.suspended(0)
                    await child.committed(0)
                })

                await c.childCoroutineProgress(childMessage, childHandler2, async child => {
                    await child.seen()
                    await child.rollingBack(originalThrowable, 0)
                    await child.rollbackFailed(child.rollback(0), rollbackThrowable)
                })

                await c.verify(finalSelectChain, result => {
                    const parsed = asResult(result)
                    assert.equal(parsed.anyChildRolledBack, true)
                    assert.equal(parsed.anyChildRollbackFailed, true)
                    assert.equal(parsed.rollingBack, false)
                })
            })
        })

        test("picks up rolling backs just starting", async () => {
            const throwable = new Error("A thing happened")
            await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                await c.seen()
                const childMessage = await c.emitted(childTopic1, 0)
                await c.suspended(0)
                await c.childCoroutineProgress(childMessage, childHandler1, async child => {
                    await child.seen()
                    await child.suspended(0)
                    await child.committed(0)
                })
                await c.childCoroutineProgress(childMessage, childHandler2, async child => {
                    await child.seen()
                    await child.suspended(0)
                    await child.committed(0)
                })

                await c.rollingBack(throwable, 1)

                await c.verify(finalSelectChain, result => {
                    const parsed = asResult(result)
                    assert.equal(parsed.anyChildRolledBack, false)
                    assert.equal(parsed.anyChildRollbackFailed, false)
                    assert.equal(parsed.rollingBack, true)
                })
            })
        })

        test("picks up rolling backs later on", async () => {
            const throwable = new Error("A thing happened")
            await coroutineProgress(await emitRootMessage(rootTopic), rootHandler, async c => {
                await c.seen()
                const childMessage = await c.emitted(childTopic1, 0)
                await c.suspended(0)
                await c.childCoroutineProgress(childMessage, childHandler1, async child => {
                    await child.seen()
                    await child.suspended(0)
                    await child.committed(0)
                })
                await c.childCoroutineProgress(childMessage, childHandler2, async child => {
                    await child.seen()
                    await child.suspended(0)
                    await child.committed(0)
                })

                await c.rollingBack(throwable, 1)
                await c.rollbackEmitted(childMessage, c.childScopeRollback(0), throwable)
                await c.suspended(c.childScopeRollback(0))

                await c.childCoroutineProgress(childMessage, childHandler1, async child => {
                    await child.rollingBack(throwable)
                    await child.suspended(child.childScopeRollback(0))
                    await child.suspended(child.rollback(0))
                    await child.rolledBack(child.rollback(0))
                })

                await c.childCoroutineProgress(childMessage, childHandler2, async child => {
                    await child.rollingBack(throwable)
                    await child.suspended(child.childScopeRollback(0))
                    await child.suspended(child.rollback(0))
                    await child.rolledBack(child.rollback(0))
                })

                await c.verify(finalSelectChain, result => {
                    const parsed = asResult(result)
                    assert.equal(parsed.anyChildRolledBack, true)
                    assert.equal(parsed.anyChildRollbackFailed, false)
                    assert.equal(parsed.rollingBack, true)
                })
            })
        })
    })
})
