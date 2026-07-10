/**
 * Control flow utility for conditional iteration with explicit continuation signaling.
 *
 * Direct port of the Kotlin `whileISaySo`, made async: the loop body must explicitly call `saySo`
 * to request another iteration. Used by the event loop to keep processing while work is found.
 */
export async function whileISaySo(
    block: (repeatCount: number, saySo: () => void) => Promise<void>,
): Promise<void> {
    let repeatCount = 0
    let repeat = true
    while (repeat) {
        repeat = false
        repeatCount++
        await block(repeatCount, () => {
            repeat = true
        })
    }
}
