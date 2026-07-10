import type { JsonValue } from "../JsonbHelper.js"

export interface Message {
    readonly id: string
    readonly topic: string
    readonly payload: JsonValue
    readonly createdAt: Date
}
