/** A handle to an active message-queue subscription; closing it stops delivery. */
export interface Subscription {
    close(): Promise<void>

    /**
     * Resolves once every worker's notification channel is active (LISTEN acknowledged).
     * Subscribing wires two delivery paths: the periodic tick (correct from the moment
     * subscribe returns) and LISTEN/NOTIFY (a latency optimization whose registration is
     * asynchronous). A message published after subscribe but before the LISTEN is active is
     * still processed — but only on the reconcile safety net's schedule. Await this before
     * publishing when that latency matters (tests do).
     */
    ready(): Promise<void>
}
