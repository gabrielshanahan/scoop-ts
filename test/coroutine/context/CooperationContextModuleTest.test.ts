import assert from "node:assert/strict"
import { describe, test } from "node:test"
import {
    CooperationContext,
    MappedElement,
    MappedKey,
    OpaqueElement,
    UnmappedKey,
} from "../../../src/coroutine/context/CooperationContext.js"
import {
    readContextJson,
    writeContextJson,
} from "../../../src/coroutine/context/CooperationContextModule.js"

// --- Test context element types ---

const StringKey = new MappedKey<StringElement>("StringKey", json => new StringElement(json.value))

class StringElement extends MappedElement {
    constructor(readonly value: string) {
        super(StringKey)
    }
}

const NullableKey = new MappedKey<NullableElement>(
    "NullableKey",
    json => new NullableElement(json.value),
)

class NullableElement extends MappedElement {
    constructor(readonly value: string | null) {
        super(NullableKey)
    }
}

const NumbersKey = new MappedKey<NumbersElement>(
    "NumbersKey",
    json => new NumbersElement(json.int, json.long, json.double, json.float),
)

class NumbersElement extends MappedElement {
    constructor(
        readonly int: number,
        readonly long: number,
        readonly double: number,
        readonly float: number,
    ) {
        super(NumbersKey)
    }
}

const BoolKey = new MappedKey<BoolElement>("BoolKey", json => new BoolElement(json.flag))

class BoolElement extends MappedElement {
    constructor(readonly flag: boolean) {
        super(BoolKey)
    }
}

const ListKey = new MappedKey<ListElement>("ListKey", json => new ListElement(json.items))

class ListElement extends MappedElement {
    constructor(readonly items: string[]) {
        super(ListKey)
    }
}

const NestedKey = new MappedKey<NestedElement>("NestedKey", json => new NestedElement(json.inner))

interface Inner {
    x: number
    y: string
}

class NestedElement extends MappedElement {
    constructor(readonly inner: Inner) {
        super(NestedKey)
    }
}

const DeepKey = new MappedKey<DeepElement>("DeepKey", json => new DeepElement(json.level1))

interface Level3 {
    value: string
}

interface Level2 {
    level3: Level3
}

interface Level1 {
    level2: Level2
}

class DeepElement extends MappedElement {
    constructor(readonly level1: Level1) {
        super(DeepKey)
    }
}

const MixedListKey = new MappedKey<MixedListElement>(
    "MixedListKey",
    json => new MixedListElement(json.items),
)

class MixedListElement extends MappedElement {
    constructor(readonly items: unknown[]) {
        super(MixedListKey)
    }
}

const MapKey = new MappedKey<MapElement>("MapKey", json => new MapElement(json.data))

class MapElement extends MappedElement {
    constructor(readonly data: Record<string, unknown>) {
        super(MapKey)
    }
}

const NullableFieldsKey = new MappedKey<NullableFieldsElement>(
    "NullableFieldsKey",
    json => new NullableFieldsElement(json.present, json.absent, json.alsoPresent),
)

class NullableFieldsElement extends MappedElement {
    constructor(
        readonly present: string,
        readonly absent: string | null,
        readonly alsoPresent: number,
    ) {
        super(NullableFieldsKey)
    }
}

// Referenced only through its reviver, like the unused Kotlin key definitions
void MixedListKey

/**
 * Serializes a CooperationContext, deserializes it back, and re-serializes to verify the JSON
 * round-trips exactly.
 */
function assertRoundTrip(context: CooperationContext): void {
    const json = writeContextJson(context)
    const deserialized = readContextJson(json)
    const roundTripped = writeContextJson(deserialized)
    assert.equal(roundTripped, json, "JSON changed after round-trip")
}

/**
 * Serializes a CooperationContext, deserializes it, then accesses the element via key to verify
 * typed deserialization still works after a round-trip.
 */
function assertTypedRoundTrip<E extends MappedElement>(context: E, key: MappedKey<E>): E {
    const json = writeContextJson(context)
    const deserialized = readContextJson(json)
    const element = deserialized.get(key)
    assert.notEqual(element, null, "Element should be retrievable after round-trip")
    return element!
}

describe("CooperationContextModuleTest", () => {
    // --- String escaping (the original bug) ---

    describe("StringEscaping", () => {
        test("tab character", () => {
            assertRoundTrip(new StringElement("hello\tworld"))
        })

        test("newline character", () => {
            assertRoundTrip(new StringElement("line1\nline2"))
        })

        test("carriage return", () => {
            assertRoundTrip(new StringElement("line1\rline2"))
        })

        test("carriage return plus newline", () => {
            assertRoundTrip(new StringElement("line1\r\nline2"))
        })

        test("escaped double quotes", () => {
            assertRoundTrip(new StringElement('say "hello"'))
        })

        test("backslashes", () => {
            assertRoundTrip(new StringElement("C:\\Users\\test\\file.txt"))
        })

        test("forward slash", () => {
            assertRoundTrip(new StringElement("path/to/file"))
        })

        test("backspace character", () => {
            assertRoundTrip(new StringElement("hello\bworld"))
        })

        test("form feed character", () => {
            assertRoundTrip(new StringElement("hello\fworld"))
        })

        test("null byte", () => {
            assertRoundTrip(new StringElement("hello\u0000world"))
        })

        test("all JSON escape sequences combined", () => {
            assertRoundTrip(new StringElement('"\\/\b\f\n\r\t'))
        })

        test("multiple special chars in sequence", () => {
            assertRoundTrip(new StringElement('\t\t\n\n""\\\\'))
        })

        test("special chars preserve typed value", () => {
            const result = assertTypedRoundTrip(new StringElement("hello\tworld"), StringKey)
            assert.equal(result.value, "hello\tworld")
        })
    })

    // --- Unicode ---

    describe("Unicode", () => {
        test("basic multilingual plane - CJK characters", () => {
            assertRoundTrip(new StringElement("\u4F60\u597D\u4E16\u754C"))
        })

        test("emoji", () => {
            assertRoundTrip(new StringElement("\uD83D\uDE00\uD83D\uDE80\uD83C\uDF1F"))
        })

        test("arabic text", () => {
            assertRoundTrip(new StringElement("\u0645\u0631\u062D\u0628\u0627"))
        })

        test("mixed ascii and unicode", () => {
            assertRoundTrip(new StringElement("hello \u4E16\u754C world"))
        })

        test("unicode control characters", () => {
            assertRoundTrip(new StringElement("\u0001\u001F"))
        })

        test("zero-width characters", () => {
            assertRoundTrip(new StringElement("a\u200Bb\u200Cc\uFEFFd"))
        })
    })

    // --- Empty and boundary values ---

    describe("EmptyAndBoundary", () => {
        test("empty string", () => {
            assertRoundTrip(new StringElement(""))
        })

        test("string with only spaces", () => {
            assertRoundTrip(new StringElement("   "))
        })

        test("very long string", () => {
            assertRoundTrip(new StringElement("a".repeat(10_000)))
        })

        test("long string with special chars throughout", () => {
            let s = ""
            for (let i = 1; i <= 1000; i++) {
                s += `item\t${i}\n`
            }
            assertRoundTrip(new StringElement(s))
        })

        test("string that looks like JSON", () => {
            assertRoundTrip(new StringElement('{"key": "value"}'))
        })

        test("string that looks like a number", () => {
            assertRoundTrip(new StringElement("12345"))
        })

        test("string that looks like boolean", () => {
            assertRoundTrip(new StringElement("true"))
        })

        test("string that looks like null", () => {
            assertRoundTrip(new StringElement("null"))
        })
    })

    // --- Numeric values ---

    describe("Numbers", () => {
        test("positive integers", () => {
            assertRoundTrip(new NumbersElement(42, 42, 42.0, 42.0))
        })

        test("zero", () => {
            assertRoundTrip(new NumbersElement(0, 0, 0.0, 0.0))
        })

        test("negative numbers", () => {
            assertRoundTrip(new NumbersElement(-42, -100, -3.14, -2.5))
        })

        test("large numbers", () => {
            assertRoundTrip(
                new NumbersElement(
                    2147483647,
                    9223372036854775807,
                    1.7976931348623157e308,
                    3.4028235e38,
                ),
            )
        })

        test("min value numbers", () => {
            assertRoundTrip(
                new NumbersElement(-2147483648, -9223372036854775808, 4.9e-324, 1.4e-45),
            )
        })
    })

    // --- Booleans ---

    describe("Booleans", () => {
        test("true value", () => {
            assertRoundTrip(new BoolElement(true))
        })

        test("false value", () => {
            assertRoundTrip(new BoolElement(false))
        })
    })

    // --- Null handling ---

    describe("Nulls", () => {
        test("null field value", () => {
            assertRoundTrip(new NullableElement(null))
        })

        test("non-null field value", () => {
            assertRoundTrip(new NullableElement("present"))
        })

        test("mixed null and non-null fields", () => {
            assertRoundTrip(new NullableFieldsElement("hello", null, 42))
        })
    })

    // --- Collections ---

    describe("Collections", () => {
        test("empty list", () => {
            assertRoundTrip(new ListElement([]))
        })

        test("single item list", () => {
            assertRoundTrip(new ListElement(["only"]))
        })

        test("multi item list", () => {
            assertRoundTrip(new ListElement(["a", "b", "c"]))
        })

        test("list with special characters in items", () => {
            assertRoundTrip(new ListElement(["tab\there", "newline\nhere", 'quote"here']))
        })

        test("list with empty strings", () => {
            assertRoundTrip(new ListElement(["", "", ""]))
        })

        test("nested list via map", () => {
            assertRoundTrip(
                new MapElement({
                    nested: [
                        [1, 2],
                        [3, 4],
                    ],
                }),
            )
        })
    })

    // --- Nested objects ---

    describe("NestedObjects", () => {
        test("simple nested object", () => {
            assertRoundTrip(new NestedElement({ x: 42, y: "hello" }))
        })

        test("nested object with special chars", () => {
            assertRoundTrip(new NestedElement({ x: 1, y: 'hello\tworld\n"quoted"' }))
        })

        test("deeply nested objects", () => {
            assertRoundTrip(new DeepElement({ level2: { level3: { value: "deep value" } } }))
        })

        test("deeply nested with special chars", () => {
            assertRoundTrip(
                new DeepElement({ level2: { level3: { value: 'deep\t"value"\n' } } }),
            )
        })
    })

    // --- Map/object with dynamic keys ---

    describe("DynamicMaps", () => {
        test("empty map", () => {
            assertRoundTrip(new MapElement({}))
        })

        test("map with various value types", () => {
            assertRoundTrip(
                new MapElement({
                    string: "hello",
                    number: 42,
                    bool: true,
                    list: [1, 2, 3],
                }),
            )
        })

        test("map with nested map", () => {
            assertRoundTrip(new MapElement({ outer: { inner: "value" } }))
        })
    })

    // --- Opaque elements (unknown context from other services) ---

    describe("OpaqueElements", () => {
        function roundTripOpaque(json: string): string {
            const key = new UnmappedKey("TestOpaque")
            const element = new OpaqueElement(key, json)
            const serialized = writeContextJson(element)
            const deserialized = readContextJson(serialized)
            const opaqueResult = deserialized.get(key)
            assert.notEqual(opaqueResult, null, "Opaque element should survive round-trip")
            return opaqueResult!.json
        }

        test("simple object", () => {
            const json = '{"key":"value"}'
            assert.equal(roundTripOpaque(json), json)
        })

        test("object with special chars in values", () => {
            const json = '{"key":"hello\\tworld"}'
            assert.equal(roundTripOpaque(json), json)
        })

        test("nested object", () => {
            const json = '{"a":{"b":{"c":"deep"}}}'
            assert.equal(roundTripOpaque(json), json)
        })

        test("array value", () => {
            const json = "[1,2,3]"
            assert.equal(roundTripOpaque(json), json)
        })

        test("complex mixed structure", () => {
            const json = '{"arr":[1,"two",true,null,{"nested":true}],"num":42,"bool":false}'
            assert.equal(roundTripOpaque(json), json)
        })

        test("string with all escape sequences", () => {
            const json = '{"v":"tab\\there\\nnewline\\r\\n\\"quoted\\"\\\\backslash"}'
            assert.equal(roundTripOpaque(json), json)
        })
    })

    // --- Multiple context elements ---

    describe("MultipleElements", () => {
        test("two elements combined via plus", () => {
            const ctx = new StringElement("hello\tworld").plus(new BoolElement(true))
            assertRoundTrip(ctx)
        })

        test("three elements combined", () => {
            const ctx = new StringElement("test\n")
                .plus(new BoolElement(false))
                .plus(new NullableElement(null))
            assertRoundTrip(ctx)
        })

        test("complex element plus simple element", () => {
            const ctx = new NestedElement({ x: 1, y: "nested\tvalue" }).plus(
                new ListElement(["a\nb", 'c"d']),
            )
            assertRoundTrip(ctx)
        })
    })

    // --- Double round-trip ---

    describe("DoubleRoundTrip", () => {
        function assertDoubleRoundTrip(context: CooperationContext): void {
            const json1 = writeContextJson(context)
            const ctx2 = readContextJson(json1)
            const json2 = writeContextJson(ctx2)
            const ctx3 = readContextJson(json2)
            const json3 = writeContextJson(ctx3)
            assert.equal(json2, json1, "First round-trip changed the JSON")
            assert.equal(json3, json2, "Second round-trip changed the JSON")
        }

        test("string with special chars survives double round-trip", () => {
            assertDoubleRoundTrip(new StringElement('line1\tline2\nline3\\end"done"'))
        })

        test("complex nested structure survives double round-trip", () => {
            assertDoubleRoundTrip(
                new DeepElement({ level2: { level3: { value: 'deep\t"value"\n\\path' } } }),
            )
        })

        test("multiple elements survive double round-trip", () => {
            const ctx = new StringElement("a\tb")
                .plus(new BoolElement(true))
                .plus(new ListElement(["x\ny", 'z"w']))
            assertDoubleRoundTrip(ctx)
        })
    })
})
