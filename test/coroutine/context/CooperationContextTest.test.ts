import assert from "node:assert/strict"
import { describe, test } from "node:test"
import {
    MappedElement,
    MappedKey,
    OpaqueElement,
    UnmappedKey,
} from "../../../src/coroutine/context/CooperationContext.js"
import {
    readContextJson,
    writeContextJson,
} from "../../../src/coroutine/context/CooperationContextModule.js"

const TestKey = new MappedKey<TestElement>("TestKey", json => new TestElement(json.value))

class TestElement extends MappedElement {
    constructor(readonly value: string) {
        super(TestKey)
    }
}

describe("CooperationContextTest", () => {
    test("test emtpy", () => {
        const jsonString = "{}"
        const context = readContextJson(jsonString)
        assert.equal(writeContextJson(context), jsonString)
    })

    test("test null values are preserved in round-trip", () => {
        const jsonString = '{"key-with-null":null}'
        const context = readContextJson(jsonString)
        assert.equal(writeContextJson(context), jsonString)
    })

    test("test null values mixed with other values are preserved in round-trip", () => {
        const jsonString = '{"a-before":"a","b-nullable":null,"c-after":"b"}'
        const context = readContextJson(jsonString)
        assert.equal(writeContextJson(context), jsonString)
    })

    test("test nested null values are preserved in round-trip", () => {
        const jsonString = '{"outer":{"inner":null,"range":null}}'
        const context = readContextJson(jsonString)
        assert.equal(writeContextJson(context), jsonString)
    })

    test("test null values in arrays are preserved in round-trip", () => {
        const jsonString = '{"items":[1,null,3]}'
        const context = readContextJson(jsonString)
        assert.equal(writeContextJson(context), jsonString)
    })

    test("test everything works as expected", () => {
        const jsonString = '{"TestKey":{"value":"test-value"},"unknown-key":"test-value"}'
        const context = readContextJson(jsonString)
        assert.equal(writeContextJson(context), jsonString)

        assert.deepStrictEqual(context.get(TestKey), new TestElement("test-value"))
        const unmappedKey = new UnmappedKey("unknown-key")
        assert.deepStrictEqual(
            context.get(unmappedKey),
            new OpaqueElement(unmappedKey, '"test-value"'),
        )

        const actual = writeContextJson(context.plus(new TestElement("different-test-value")))

        assert.equal(
            actual,
            '{"TestKey":{"value":"different-test-value"},"unknown-key":"test-value"}',
        )
        assert.equal(
            writeContextJson(
                new TestElement("test-value").plus(new TestElement("different-test-value")),
            ),
            '{"TestKey":{"value":"different-test-value"}}',
        )
        assert.equal(writeContextJson(context.get(TestKey)!), '{"TestKey":{"value":"test-value"}}')
        assert.equal(
            writeContextJson(context.minus(unmappedKey)),
            '{"TestKey":{"value":"test-value"}}',
        )
        assert.equal(writeContextJson(context.get(unmappedKey)!), '{"unknown-key":"test-value"}')
        assert.equal(writeContextJson(context.minus(TestKey)), '{"unknown-key":"test-value"}')
    })
})
