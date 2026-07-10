import { uuidV7 } from "../util/UuidV7.js"

/**
 * Identifies a distributed coroutine (saga) implementation, with support for horizontal scaling.
 *
 * - name: identifies the saga type/logic (shared across all instances)
 * - instance: identifies the specific service instance running that saga (time-ordered UUID,
 *   auto-generated when omitted)
 */
export class DistributedCoroutineIdentifier {
    readonly name: string
    readonly instance: string

    constructor(name: string, instance: string = uuidV7()) {
        this.name = name
        this.instance = instance
    }

    toString(): string {
        return `DistributedCoroutineIdentifier(name=${this.name}, instance=${this.instance})`
    }
}

/** Renders the identifier as "name[instance]" for logging and distributed stack traces. */
export function renderAsString(identifier: DistributedCoroutineIdentifier): string {
    return `${identifier.name}[${identifier.instance}]`
}
