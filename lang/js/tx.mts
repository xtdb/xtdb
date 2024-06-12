import { Query, UnifyClause } from './query.mjs'
import { Expr, date as dateExpr } from './expr.mjs'
import { Binding, ToBindings, toBindings } from './binding.mjs'

type EntityId = any

type XtdbDocument = {
    "_id": EntityId
}

class PutDocs {
    constructor(public putDocs: XtdbDocument[], public into: string) { }

    public validFrom?: Date
    public validTo?: Date

    startingFrom(validFrom: Date): PutDocs {
        this.validFrom = validFrom
        return this
    }

    until(validTo: Date): PutDocs {
        this.validTo = validTo
        return this
    }

    during(validFrom?: Date, validTo?: Date): PutDocs {
        this.validFrom = validFrom
        this.validTo = validTo
        return this
    }
}

export function putDocs(into: string, ...docs: XtdbDocument[]): PutDocs {
    return new PutDocs(docs, into)
}

class DeleteDocs {
    constructor(public deleteDocs: EntityId[], public from: string) { }

    public validFrom?: Date
    public validTo?: Date

    startingFrom(validFrom: Date): DeleteDocs {
        this.validFrom = validFrom
        return this
    }

    until(validTo: Date): DeleteDocs {
        this.validTo = validTo
        return this
    }

    during(validFrom?: Date, validTo?: Date): DeleteDocs {
        this.validFrom = validFrom
        this.validTo = validTo
        return this
    }
}

export function deleteDocs(from: string, ...entityIds: EntityId[]): DeleteDocs {
    return new DeleteDocs(entityIds, from)
}

class EraseDocs {
    constructor(public eraseDocs: EntityId[], public from: string) { }
}

export function eraseDocs(from: string, ...entityIds: EntityId[]): EraseDocs {
    return new EraseDocs(entityIds, from)
}

class Sql {
    constructor(public sql: string) {}

    public argRows?: any[][]

    public args(...argRows: any[][]): Sql {
        this.argRows = argRows
        return this
    }
}

export function sql(sql: string): Sql {
    return new Sql(sql)
}

type ArgRow = {
    [key: string]: any
}

class Insert {
    constructor(public insertInto: string, public query: Query) {}
    public argRows?: ArgRow[];

    args(...args: ArgRow[]): Insert {
        this.argRows = args
        return this
    }
}

export function insert(into: string, query: Query): Insert {
    return new Insert(into, query)
}

type TemporalExtents = "allTime" | {
    from?: Expr;
    to?: Expr;
}

function asExpr(date?: Date | Expr): Expr | undefined {
    if (date === undefined) return undefined
    if (date instanceof Date) return dateExpr(date)
    return date
}

class Update {
    constructor (public update: string) {}

    forValidTime: TemporalExtents = {}
    bind: Binding[] = [];
    set: Binding[] = [];
    unify: UnifyClause[] = [];
    argRows?: ArgRow[];

    startingFrom(from: Date | Expr): Update {
        return this.during(from, undefined)
    }

    until(to: Date | Expr): Update {
        return this.during(undefined, to)
    }

    during(from?: Date | Expr, to?: Date | Expr): Update {
        this.forValidTime = { from: asExpr(from), to: asExpr(to) }
        return this
    }

    forAllTime(): Update {
        this.forValidTime = "allTime"
        return this
    }

    binding(...bindings: ToBindings[]): Update {
        this.bind.push(...(bindings.flatMap(toBindings)))
        return this
    }

    setting(...set: ToBindings[]): Update {
        this.set.push(...(set.flatMap(toBindings)))
        return this
    }

    unifying(...clauses: UnifyClause[]): Update {
        this.unify.push(...clauses)
        return this
    }

    args(...argRows: ArgRow[]): Update {
        this.argRows = argRows
        return this
    }
}

export function update(table: string): Update {
    return new Update(table)
}

class Delete {
    constructor (public deleteFrom: string) {}

    forValidTime: TemporalExtents = {}
    bind: Binding[] = [];
    unify: UnifyClause[] = [];
    argRows?: ArgRow[];

    startingFrom(from: Date | Expr): Delete {
        return this.during(from, undefined)
    }

    until(to: Date | Expr): Delete {
        return this.during(undefined, to)
    }

    during(from?: Date | Expr, to?: Date | Expr): Delete {
        this.forValidTime = { from: asExpr(from), to: asExpr(to) }
        return this
    }

    forAllTime(): Delete {
        this.forValidTime = "allTime"
        return this
    }

    binding(...bindings: ToBindings[]): Delete {
        this.bind.push(...(bindings.flatMap(toBindings)))
        return this
    }

    unifying(...clauses: UnifyClause[]): Delete {
        this.unify.push(...clauses)
        return this
    }

    args(...argRows: ArgRow[]): Delete {
        this.argRows = argRows
        return this
    }
}

export function deleteFrom(table: string): Delete {
    return new Delete(table)
}

class Erase {
    constructor (public from: string) {}

    bind: Binding[] = [];
    unify: UnifyClause[] = [];
    argRows?: ArgRow[];

    binding(...bindings: ToBindings[]): Erase {
        this.bind.push(...(bindings.flatMap(toBindings)))
        return this
    }

    unifying(...clauses: UnifyClause[]): Erase {
        this.unify.push(...clauses)
        return this
    }

    args(...argRows: ArgRow[]): Erase {
        this.argRows = argRows
        return this
    }
}

export function eraseFrom(table: string): Erase {
    return new Erase(table)
}

class AssertExists {
    constructor(public assertExists: Query) {}
    public argRows?: ArgRow[];

    args(...argRows: ArgRow[]): AssertExists {
        this.argRows = argRows
        return this
    }
}

export function assertExists(q: Query): AssertExists {
    return new AssertExists(q)
}

class AssertNotExists {
    constructor(public assertNotExists: Query) {}
    public argRows?: ArgRow[];

    args(...argRows: ArgRow[]): AssertNotExists {
        this.argRows = argRows
        return this
    }
}

export function assertNotExists(q: Query): AssertNotExists {
    return new AssertNotExists(q)
}

export type TxOp = PutDocs | DeleteDocs | EraseDocs | Sql
    | Insert | Update | Delete | Erase | AssertExists | AssertNotExists
