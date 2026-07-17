import assert from "node:assert/strict"
import { describe, test } from "node:test"
import { ReconcileGate } from "../../src/coroutine/ReconcileGate.js"

const intervalSeconds = 30

function gate(startNanos = 0): ReconcileGate {
    return ReconcileGate.create(intervalSeconds * 1000, startNanos)
}

function secondsAsNanos(seconds: number): number {
    return seconds * 1_000_000_000
}

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
