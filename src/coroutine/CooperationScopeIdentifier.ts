/**
 * Identifies a cooperation scope via its lineage — an array of UUIDs where prefixes are parent
 * runs. Root scopes have a single-element lineage.
 */
export type CooperationScopeIdentifier = RootScopeIdentifier | ChildScopeIdentifier

export class RootScopeIdentifier {
    readonly cooperationLineage: string[]

    constructor(readonly cooperationId: string) {
        this.cooperationLineage = [cooperationId]
    }
}

export class ChildScopeIdentifier {
    constructor(readonly cooperationLineage: string[]) {}
}
