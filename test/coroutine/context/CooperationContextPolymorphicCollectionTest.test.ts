import assert from "node:assert/strict"
import { describe, test } from "node:test"
import {
    CooperationContext,
    MappedElement,
    MappedKey,
} from "../../../src/coroutine/context/CooperationContext.js"
import {
    readContextJson,
    writeContextJson,
} from "../../../src/coroutine/context/CooperationContextModule.js"

/**
 * The Kotlin original uses Jackson @JsonTypeInfo/@JsonSubTypes machinery for the polymorphic
 * Animal hierarchy; the TS analog carries the same `type` discriminator explicitly in the data
 * and revives via it (see DECISIONS.md).
 */
type Animal =
    | { type: "Dog"; name: string }
    | { type: "Cat"; name: string; livesLeft: number }

function dog(name: string): Animal {
    return { type: "Dog", name }
}

function cat(name: string, livesLeft: number): Animal {
    return { type: "Cat", name, livesLeft }
}

class Zoo extends MappedElement {
    constructor(readonly animals: Animal[]) {
        super(ZooKey)
    }
}

const ZooKey = new MappedKey<Zoo>("ZooKey", json => new Zoo(json.animals))

describe("CooperationContextPolymorphicCollectionTest", () => {
    test("polymorphic list inside MappedElement round-trips through CooperationContext", () => {
        const original: CooperationContext = new Zoo([dog("Rex"), cat("Whiskers", 9)])

        const json = writeContextJson(original)

        // Sanity: discriminators must appear in the serialized form.
        assert.ok(
            json.includes('"type":"Dog"'),
            `Expected 'type:"Dog"' discriminator in serialized JSON, got: ${json}`,
        )
        assert.ok(
            json.includes('"type":"Cat"'),
            `Expected 'type:"Cat"' discriminator in serialized JSON, got: ${json}`,
        )

        // Round-trip: deserialize and lazily access the element.
        const restored = readContextJson(json)
        const zoo = restored.get(ZooKey)!

        assert.deepStrictEqual(zoo, original.get(ZooKey))
    })
})
