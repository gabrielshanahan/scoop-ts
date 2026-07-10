import { randomBytes } from "node:crypto"
import { nowMillis } from "./Clock.js"

/**
 * Generates a UUIDv7 (Unix Epoch time-based, with random bits).
 *
 * Precise port of the Kotlin `uuidV7()`: 48-bit millisecond timestamp, 4-bit version (7), 12
 * random bits, RFC4122 variant, 62 random bits. The time-ordered prefix is load-bearing for the
 * engine (IDs sort by creation time), so the layout is reproduced bit-for-bit. The timestamp is
 * read through the injectable clock (see Clock.ts).
 */
export function uuidV7(): string {
    const timestamp = nowMillis()
    const rnd = randomBytes(10)

    const bytes = new Uint8Array(16)
    // 48-bit timestamp, big-endian
    bytes[0] = (timestamp / 2 ** 40) & 0xff
    bytes[1] = (timestamp / 2 ** 32) & 0xff
    bytes[2] = (timestamp / 2 ** 24) & 0xff
    bytes[3] = (timestamp / 2 ** 16) & 0xff
    bytes[4] = (timestamp / 2 ** 8) & 0xff
    bytes[5] = timestamp & 0xff
    // version 7 in the high nibble, 4 random bits in the low nibble + next random byte
    bytes[6] = 0x70 | (rnd[0]! & 0x0f)
    bytes[7] = rnd[1]!
    // RFC4122 variant (10) + 6 random bits
    bytes[8] = 0x80 | (rnd[2]! & 0x3f)
    bytes[9] = rnd[3]!
    bytes[10] = rnd[4]!
    bytes[11] = rnd[5]!
    bytes[12] = rnd[6]!
    bytes[13] = rnd[7]!
    bytes[14] = rnd[8]!
    bytes[15] = rnd[9]!

    const hex = Array.from(bytes, b => b.toString(16).padStart(2, "0")).join("")
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`
}
