import assert from "node:assert/strict"
import { describe, test } from "node:test"
import { ReconcileGate } from "../../src/coroutine/ReconcileGate.js"

const intervalSeconds = 30

// A gate driven by an explicit monotonic clock so the safety-net timing is deterministic.
function gate(startNanos = 0): ReconcileGate {
    return ReconcileGate.create(intervalSeconds * 1000, startNanos)
}

function secondsAsNanos(seconds: number): number {
    return seconds * 1_000_000_000
}

// Reconcile (consume + run + commit an empty pass) until the gate goes idle, returning how many
// passes ran. Bounded so a never-quiescing gate fails the test instead of looping forever.
function drainUntilIdle(g: ReconcileGate, nowNanos: number): number {
    let passes = 0
    while (g.shouldReconcile(nowNanos)) {
        g.reconcileSucceeded(0, nowNanos)
        passes++
        if (passes >= 100) {
            throw new Error("gate never went idle")
        }
    }
    return passes
}

describe("ReconcileGateTest", () => {
    test("ALWAYS always reconciles regardless of state", () => {
        const g = ReconcileGate.ALWAYS
        assert.ok(g.shouldReconcile())
        g.reconcileSucceeded(0)
        assert.ok(g.shouldReconcile(), "ALWAYS never gates")
    })

    test("starts armed so the first tick reconciles, then drains to idle", () => {
        const g = gate()
        // Fresh gate is armed; it drains a short quiet tail of empty passes then goes idle.
        const passes = drainUntilIdle(g, 0)
        assert.ok(
            passes >= 1 && passes <= 10,
            `should reconcile a small bounded tail, was ${passes}`,
        )
        assert.ok(!g.shouldReconcile(secondsAsNanos(1)), "idle after the tail")
    })

    test("markDirty re-arms the drain", () => {
        const g = gate()
        drainUntilIdle(g, 0)
        assert.ok(!g.shouldReconcile(secondsAsNanos(1)))
        g.markDirty()
        assert.ok(g.shouldReconcile(secondsAsNanos(1)), "a notification re-arms the gate")
    })

    test("a productive pass keeps the drain going (handles contending siblings)", () => {
        const g = gate()
        drainUntilIdle(g, 0)
        g.markDirty()
        // First pass after the notification inserts nothing (e.g. it lost a SKIP LOCKED race)...
        assert.ok(g.shouldReconcile(secondsAsNanos(1)))
        g.reconcileSucceeded(0, secondsAsNanos(1))
        // ...next pass wins the lock and inserts a row, which must reset the tail so further
        // contending work still gets drained.
        assert.ok(g.shouldReconcile(secondsAsNanos(1)))
        g.reconcileSucceeded(1, secondsAsNanos(1))
        assert.ok(g.shouldReconcile(secondsAsNanos(1)), "productive pass resets the tail")
    })

    test("safety net forces a reconcile when idle", () => {
        const g = gate()
        drainUntilIdle(g, 0)
        assert.ok(!g.shouldReconcile(secondsAsNanos(intervalSeconds - 1)))
        assert.ok(
            g.shouldReconcile(secondsAsNanos(intervalSeconds + 1)),
            "safety net fires once the interval elapses",
        )
    })

    test("reconcileFailed re-arms so the next tick retries", () => {
        const g = gate()
        drainUntilIdle(g, 0)
        // A notification wakes the gate; its reconcile then fails (throws).
        g.markDirty()
        assert.ok(g.shouldReconcile(secondsAsNanos(1)))
        g.reconcileFailed()
        assert.ok(g.shouldReconcile(secondsAsNanos(1)), "failure re-arms the retry")
    })

    test("a notification landing during a reconcile survives the consume-before-work clear", () => {
        const g = gate()
        drainUntilIdle(g, 0)
        // tick consumes the (absent) signal and finds idle
        assert.ok(!g.shouldReconcile(secondsAsNanos(1)))
        // notification lands; even though a hypothetical in-flight pass already cleared its
        // snapshot, the signal is recorded and honoured next tick.
        g.markDirty()
        g.reconcileSucceeded(0, secondsAsNanos(1))
        assert.ok(g.shouldReconcile(secondsAsNanos(2)), "late notification is not lost")
    })
})

describe("ReconcileGate drain side", () => {
    // Drain (consume + run + complete an EMPTY drain) until idle; bounded like drainUntilIdle.
    function drainDrainUntilIdle(g: ReconcileGate, nowNanos: number): number {
        let passes = 0
        while (g.shouldDrain(nowNanos)) {
            g.drainCompleted(false, nowNanos)
            passes++
            if (passes >= 100) {
                throw new Error("drain gate never went idle")
            }
        }
        return passes
    }

    test("ALWAYS always drains regardless of state", () => {
        const g = ReconcileGate.ALWAYS
        assert.ok(g.shouldDrain())
        g.drainCompleted(false)
        assert.ok(g.shouldDrain(), "ALWAYS never gates the drain")
    })

    test("starts armed, drains a bounded quiet tail, then goes idle", () => {
        const g = gate()
        const passes = drainDrainUntilIdle(g, 0)
        assert.ok(passes >= 1 && passes <= 10, `bounded tail, was ${passes}`)
        assert.ok(!g.shouldDrain(secondsAsNanos(1)), "idle after the tail")
    })

    test("markDirty re-arms the drain independently of reconcile consuming its signal", () => {
        const g = gate()
        drainUntilIdle(g, 0)
        drainDrainUntilIdle(g, 0)
        g.markDirty()
        // Reconcile consumes ITS dirty signal first — the drain must still see its own latch.
        assert.ok(g.shouldReconcile(secondsAsNanos(1)))
        assert.ok(g.shouldDrain(secondsAsNanos(1)), "drain armed despite reconcile consuming first")
    })

    test("a productive reconcile arms the drain", () => {
        const g = gate()
        drainUntilIdle(g, 0)
        drainDrainUntilIdle(g, 0)
        assert.ok(!g.shouldDrain(secondsAsNanos(1)), "idle before")
        g.reconcileSucceeded(3, secondsAsNanos(1))
        assert.ok(g.shouldDrain(secondsAsNanos(1)), "fresh continuations arm the drain")
    })

    test("a drain that resumed something stays armed; empty drains go quiet", () => {
        const g = gate()
        g.shouldDrain(0)
        g.drainCompleted(true, 0)
        assert.ok(g.shouldDrain(0), "productive drain stays armed")
        const passes = drainDrainUntilIdle(g, 0)
        assert.ok(passes >= 1, "then quiesces on empty passes")
    })

    test("the safety sweep forces a drain even with no signals", () => {
        const g = gate()
        drainDrainUntilIdle(g, 0)
        // Jitter seeds the first sweep within one interval — two intervals out is safely past it.
        assert.ok(g.shouldDrain(secondsAsNanos(2 * intervalSeconds)), "sweep fires")
    })

    test("drainFailed re-arms", () => {
        const g = gate()
        drainDrainUntilIdle(g, 0)
        g.drainFailed()
        assert.ok(g.shouldDrain(secondsAsNanos(1)))
    })
})
