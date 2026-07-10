/** A handle to an active message-queue subscription; closing it stops delivery. */
export interface Subscription {
    close(): Promise<void>
}
