/**
 * Mechanical reconciliation of PORT-LEDGER.md:
 *  1. Re-derives the source-of-truth inventory from the Kotlin reference repo (if available at
 *     SCOOP_KOTLIN_REPO or the default checkout path): every main file and every @Test method.
 *  2. Verifies the ledger covers exactly that inventory, with no `pending` entries left.
 *  3. Verifies the ported TS test files contain exactly as many test() cases as the ledger says
 *     were ported from each Kotlin file.
 * Exits non-zero if anything fails to reconcile.
 */
import { existsSync, readdirSync, readFileSync, statSync } from "node:fs"
import { fileURLToPath } from "node:url"
import { dirname, join } from "node:path"

const root = join(dirname(fileURLToPath(import.meta.url)), "..")
const kotlinRepo =
    process.env.SCOOP_KOTLIN_REPO ??
    "/Users/gabriel.shanahan/projects/gaib/projects/scoop/original-repo"

const ledger = readFileSync(join(root, "PORT-LEDGER.md"), "utf-8")

let failures = 0
function check(condition: boolean, message: string): void {
    if (condition) {
        console.log(`  ok: ${message}`)
    } else {
        failures++
        console.error(`  FAIL: ${message}`)
    }
}

// --- 1. Ledger-internal invariants -------------------------------------------------------------
console.log("Ledger invariants:")
check(!ledger.includes("| pending |"), "no pending entries remain")

// Parse ledger test sections: "### <kotlin path> (N tests) → <ts path>" followed by a table.
interface LedgerSection {
    kotlinFile: string
    declaredCount: number
    tsFile: string | null
    tests: string[]
}
const sections: LedgerSection[] = []
const sectionRegex = /^### (\S+\.kt) \((\d+) tests\) → (\S+)$/gm
let match: RegExpExecArray | null
while ((match = sectionRegex.exec(ledger)) !== null) {
    const start = match.index + match[0].length
    const nextSection = ledger.indexOf("\n### ", start)
    const endOfBlock = ledger.indexOf("\n## ", start)
    const end = Math.min(
        nextSection === -1 ? ledger.length : nextSection,
        endOfBlock === -1 ? ledger.length : endOfBlock,
    )
    const body = ledger.slice(start, end)
    const tests = [...body.matchAll(/^\| \d+ \| (.+?) \| (\w+) \|/gm)].map(row => row[1]!)
    sections.push({
        kotlinFile: match[1]!,
        declaredCount: Number(match[2]),
        tsFile: match[3]!,
        tests,
    })
}

const ledgerTestTotal = sections.reduce((sum, section) => sum + section.tests.length, 0)
check(ledgerTestTotal === 195, `ledger enumerates 195 test methods (found ${ledgerTestTotal})`)
for (const section of sections) {
    check(
        section.tests.length === section.declaredCount,
        `${section.kotlinFile}: table rows (${section.tests.length}) match declared count (${section.declaredCount})`,
    )
}

const mainFileRows = [...ledger.matchAll(/^\| (\S+\.kt) \| ([^|]+) \| (\w+) \|/gm)]
check(
    mainFileRows.length === 63,
    `ledger enumerates 63 main source files (found ${mainFileRows.length})`,
)

// --- 2. Cross-check against the Kotlin reference repo ------------------------------------------
if (existsSync(kotlinRepo)) {
    console.log(`Reference repo cross-check (${kotlinRepo}):`)
    const kotlinTests = new Map<string, string[]>()
    const kotlinMain: string[] = []
    const testPattern = /@Test\b[^\n]*\n(?:\s*@\w+(?:\([^)]*\))?\s*\n)*\s*fun\s+(?:`([^`]+)`|(\w+))/g

    function walk(dir: string, files: string[]): void {
        for (const entry of readdirSync(dir)) {
            const path = join(dir, entry)
            if (statSync(path).isDirectory()) {
                walk(path, files)
            } else if (entry.endsWith(".kt")) {
                files.push(path)
            }
        }
    }

    for (const module of ["scoop-core", "scoop-quarkus"]) {
        for (const kind of ["main", "test"]) {
            const base = join(kotlinRepo, module, "src", kind, "kotlin")
            if (!existsSync(base)) continue
            const files: string[] = []
            walk(base, files)
            for (const file of files) {
                const rel = file.slice(file.indexOf("kotlin/") + "kotlin/".length)
                if (kind === "main") {
                    kotlinMain.push(rel)
                } else {
                    const source = readFileSync(file, "utf-8")
                    const names = [...source.matchAll(testPattern)].map(
                        testMatch => testMatch[1] ?? testMatch[2]!,
                    )
                    if (names.length > 0) {
                        kotlinTests.set(rel, names)
                    }
                }
            }
        }
    }

    const kotlinTestTotal = [...kotlinTests.values()].reduce((sum, names) => sum + names.length, 0)
    check(
        kotlinTestTotal === 195,
        `reference repo has 195 @Test methods (found ${kotlinTestTotal})`,
    )
    check(
        kotlinMain.length === 63,
        `reference repo has 63 main files (found ${kotlinMain.length})`,
    )

    const ledgerMainFiles = new Set(mainFileRows.map(row => row[1]!))
    for (const file of kotlinMain) {
        check(ledgerMainFiles.has(file), `main file in ledger: ${file}`)
    }

    const ledgerSectionsByFile = new Map(sections.map(section => [section.kotlinFile, section]))
    for (const [file, names] of kotlinTests) {
        const section = ledgerSectionsByFile.get(file)
        check(section !== undefined, `test file in ledger: ${file}`)
        if (section) {
            for (const name of names) {
                check(
                    section.tests.includes(name),
                    `test in ledger: ${file} :: ${name.slice(0, 80)}`,
                )
            }
        }
    }
} else {
    console.log(`Reference repo not found at ${kotlinRepo} — skipping cross-check.`)
}

// --- 3. TS-side counts --------------------------------------------------------------------------
console.log("TS test tree cross-check:")
const testsByTsFile = new Map<string, number>()
for (const section of sections) {
    if (!section.tsFile) continue
    testsByTsFile.set(
        section.tsFile,
        (testsByTsFile.get(section.tsFile) ?? 0) + section.tests.length,
    )
}
for (const [tsFile, expectedCount] of testsByTsFile) {
    const path = join(root, tsFile)
    if (!existsSync(path)) {
        failures++
        console.error(`  FAIL: missing TS test file ${tsFile}`)
        continue
    }
    const source = readFileSync(path, "utf-8")
    const actual = [...source.matchAll(/^\s*test\(/gm)].length
    check(
        actual === expectedCount,
        `${tsFile}: contains ${expectedCount} test() cases (found ${actual})`,
    )
}

// --- 4. Port-added regression tests (outside the ported inventory) ------------------------------
console.log("Port-added regression tests:")
const portAddedRegex = /^### (\S+\.test\.ts) \((\d+) tests\) — port-added$/gm
let portAddedMatch: RegExpExecArray | null
let portAddedSections = 0
while ((portAddedMatch = portAddedRegex.exec(ledger)) !== null) {
    portAddedSections++
    const tsFile = portAddedMatch[1]!
    const declaredCount = Number(portAddedMatch[2])
    const start = portAddedMatch.index + portAddedMatch[0].length
    const nextSection = ledger.indexOf("\n### ", start)
    const endOfBlock = ledger.indexOf("\n## ", start)
    const end = Math.min(
        nextSection === -1 ? ledger.length : nextSection,
        endOfBlock === -1 ? ledger.length : endOfBlock,
    )
    const rows = [...ledger.slice(start, end).matchAll(/^\| \d+ \| (.+?) \| (\w+) \|/gm)]
    check(
        rows.length === declaredCount,
        `${tsFile}: table rows (${rows.length}) match declared count (${declaredCount})`,
    )
    const path = join(root, tsFile)
    if (!existsSync(path)) {
        failures++
        console.error(`  FAIL: missing port-added test file ${tsFile}`)
        continue
    }
    const source = readFileSync(path, "utf-8")
    const actual = [...source.matchAll(/^\s*test\(/gm)].length
    check(
        actual === declaredCount,
        `${tsFile}: contains ${declaredCount} test() cases (found ${actual})`,
    )
}
check(portAddedSections > 0, "ledger declares the port-added regression test section")

console.log(failures === 0 ? "\nRECONCILED: everything accounted for." : `\n${failures} FAILURES`)
process.exit(failures === 0 ? 0 : 1)
