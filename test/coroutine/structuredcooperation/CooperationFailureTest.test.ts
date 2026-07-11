import assert from "node:assert/strict"
import { describe, test } from "node:test"
import {
    type CooperationException,
    type CooperationFailure,
    cooperationFailureFromThrowable,
    type StackTraceFrame,
    toCooperationException,
} from "../../../src/coroutine/structuredcooperation/CooperationFailure.js"

const UNKNOWN_CLASSNAME = "<unknown class>"
const UNKNOWN_FUNCTION = "<unknown function>"

function assertStackFramesEquivalent(expected: StackTraceFrame[], actual: StackTraceFrame[]): void {
    assert.equal(
        actual.length,
        expected.length,
        `Stack traces have different size - expected ${expected.length}, got ${actual.length}`,
    )
    for (let i = 0; i < expected.length; i++) {
        const expectedElement = expected[i]!
        const actualElement = actual[i]!
        assert.equal(actualElement.fileName, expectedElement.fileName)
        assert.equal(actualElement.lineNumber, expectedElement.lineNumber)
        assert.equal(
            actualElement.className ?? UNKNOWN_CLASSNAME,
            expectedElement.className ?? UNKNOWN_CLASSNAME,
        )
        assert.equal(
            actualElement.functionName ?? UNKNOWN_FUNCTION,
            expectedElement.functionName ?? UNKNOWN_FUNCTION,
        )
    }
}

function assertThrowablesEquivalent(expected: Error, actual: CooperationException): void {
    assert.equal(
        actual.type,
        expected.name,
        `Classes should be equal - expected ${expected.name}, got ${actual.type}`,
    )
    assert.ok(actual.message.endsWith(expected.message ?? ""))

    const expectedCause = expected.cause as Error | undefined
    const actualCause = actual.cause
    if (expectedCause && actualCause) {
        assertThrowablesEquivalent(expectedCause, actualCause)
    } else {
        assert.equal(
            expectedCause ?? null,
            actualCause ?? null,
            "Causes should both be null or both not be null",
        )
    }
}

/** Truncate a stack to frames within this test file, so the trace doesn't depend on the runner. */
function truncateStack(error: Error): void {
    const lines = (error.stack ?? "").split("\n")
    const kept = [lines[0]!]
    for (const line of lines.slice(1)) {
        if (!line.includes("CooperationFailureTest.test.ts")) {
            break
        }
        kept.push(line)
    }
    error.stack = kept.join("\n")
}

describe("CooperationFailureTest", () => {
    test("test deserialization of unknown failure type", () => {
        const cooperationFailure: CooperationFailure = {
            message: "Non-zero exit status returned from process",
            type: "ExitStatus",
            source: "external-system",
            stackTrace: [
                {
                    fileName: "script.sh",
                    lineNumber: 42,
                    className: null,
                    functionName: null,
                },
            ],
            causes: [],
        }

        const throwable = toCooperationException(cooperationFailure)

        assert.equal(
            throwable.message,
            "[external-system] ExitStatus: Non-zero exit status returned from process",
        )
        assert.equal(throwable.type, "ExitStatus")
        assert.equal(throwable.source, "external-system")
        assertStackFramesEquivalent(cooperationFailure.stackTrace, throwable.stackTraceFrames)
    })

    test("test idempotence of mapping Throwable - CooperationFailure - JSON - CooperationFailure - Throwable", () => {
        const cause = new Error("This is the cause")
        cause.name = "IllegalArgumentError"
        const originalException = new Error("Test exception", { cause })
        originalException.name = "RuntimeError"

        // Truncate stacktraces so they don't depend on the way tests are run
        truncateStack(cause)
        truncateStack(originalException)

        const originalCooperationFailure = cooperationFailureFromThrowable(
            originalException,
            "test-system",
        )

        const originalCooperationFailureJson = JSON.stringify(originalCooperationFailure, null, 2)
        const deserializedCooperationFailure = JSON.parse(
            originalCooperationFailureJson,
        ) as CooperationFailure

        const cooperationException = toCooperationException(deserializedCooperationFailure)
        const mappedCooperationFailure = cooperationFailureFromThrowable(
            cooperationException,
            cooperationException.source,
        )

        assertThrowablesEquivalent(originalException, cooperationException)
        assert.deepEqual(mappedCooperationFailure, originalCooperationFailure)

        assert.equal(cooperationException.message, "[test-system] RuntimeError: Test exception")

        // The Kotlin original pins the exact pretty-printed JSON (with JVM/Quarkus frames and
        // line numbers stripped). Runtime frames here are environment paths, so the equivalent
        // assertion pins the serialized STRUCTURE: field set, nesting, and the deterministic
        // values (see PORT-LEDGER note).
        const parsed = JSON.parse(originalCooperationFailureJson) as CooperationFailure
        assert.deepEqual(Object.keys(parsed), ["message", "type", "source", "stackTrace", "causes"])
        assert.equal(parsed.message, "Test exception")
        assert.equal(parsed.type, "RuntimeError")
        assert.equal(parsed.source, "test-system")
        assert.ok(Array.isArray(parsed.stackTrace) && parsed.stackTrace.length > 0)
        for (const frame of parsed.stackTrace) {
            assert.deepEqual(Object.keys(frame), [
                "fileName",
                "lineNumber",
                "className",
                "functionName",
            ])
            assert.equal(typeof frame.lineNumber, "number")
        }
        assert.equal(parsed.causes.length, 1)
        const parsedCause = parsed.causes[0]!
        assert.equal(parsedCause.message, "This is the cause")
        assert.equal(parsedCause.type, "IllegalArgumentError")
        assert.equal(parsedCause.source, "test-system")
        assert.deepEqual(parsedCause.causes, [])
    })
})
