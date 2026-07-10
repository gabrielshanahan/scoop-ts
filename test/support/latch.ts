/**
 * Async CountDownLatch — the analog of java.util.concurrent.CountDownLatch for tests. Awaiting
 * yields the event loop, so saga steps can block on a latch while other workers proceed (the
 * Kotlin tests do this with real threads).
 */
export class CountDownLatch {
    private count: number
    private readonly resolvers: Array<() => void> = []

    constructor(count: number) {
        this.count = count
    }

    countDown(): void {
        if (this.count > 0) {
            this.count--
            if (this.count === 0) {
                for (const resolve of this.resolvers.splice(0)) {
                    resolve()
                }
            }
        }
    }

    getCount(): number {
        return this.count
    }

    /** Resolves true when the latch reaches zero, false on timeout. */
    await(timeoutMillis: number): Promise<boolean> {
        if (this.count === 0) {
            return Promise.resolve(true)
        }
        return new Promise<boolean>(resolve => {
            const timer = setTimeout(() => resolve(false), timeoutMillis)
            this.resolvers.push(() => {
                clearTimeout(timer)
                resolve(true)
            })
        })
    }
}

/** Plain async sleep. */
export function sleep(millis: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, millis))
}
