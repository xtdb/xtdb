import { Binding, ToBindings, toBindings } from './binding.mjs'
import { Expr } from './expr.mjs'

type TemporalFilter = { from: Date } | { to: Date } | { in: [Date, Date] } | "allTime"

class From {
    forValidTime?: TemporalFilter;
    forSystemTime?: TemporalFilter;
    projectAllCols: boolean;

    bind: Binding[] = []

    constructor(public readonly from: string, opts: FromOpts) {
        this.forValidTime = opts.forValidTime;
        this.forSystemTime = opts.forSystemTime;
        this.projectAllCols = opts.projectAllCols;
    }

    binding(...bindings: ToBindings[]): From {
        this.bind = bindings.flatMap(toBindings)
        return this
    }
}

type FromOpts = {
    forValidTime?: TemporalFilter,
    forSystemTime?: TemporalFilter,
    projectAllCols: boolean
}

export function from(table: string, opts: FromOpts = { projectAllCols: false }) : From {
    return new From(table, opts)
}

type Where = { where: Expr[] }

export function where(...preds: Expr[]) {
    return { where: preds }
}

class Join {
    constructor(public readonly join: Query, public readonly args: Binding[]) {}

    bind: Binding[] = []

    binding(...bindings: ToBindings[]): Join {
        this.bind = bindings.flatMap(toBindings)
        return this
    }
}

export function join(query: Query, ...args: ToBindings[]): Join {
    return new Join(query, args.flatMap(toBindings))
}

class LeftJoin {
    constructor (public readonly leftJoin: Query, public readonly args: Binding[]) {}

    public bind: Binding[] = []

    binding(...bindings: ToBindings[]): LeftJoin {
        this.bind = bindings.flatMap(toBindings)
        return this
    }
}

export function leftJoin(query: Query, ...args: ToBindings[]): LeftJoin {
    return new LeftJoin(query, args.flatMap(toBindings))
}

type Aggregate = {
    aggregate: Binding[]
}

export function aggregate(...bindings: ToBindings[]): Aggregate {
    return { aggregate: bindings.flatMap(toBindings) }
}

type With = { with: Binding[] }

// TODO naming
export function with_(...bindings: ToBindings[]): With {
    return {with: bindings.flatMap(toBindings)}
}

type Return = { return: Binding[] }

export function returning(...bindings: ToBindings[]): Return {
    return {return: bindings.flatMap(toBindings)}
}

type Without = { without: string[] }

export function without(...cols: string[]): Without {
    return { without: cols }
}

type Unnest = { unnest: Binding }

export function unnest(binding: string, expr: Expr): Unnest {
    return { unnest: {binding, expr}}
}

type OrderDir = "asc" | "desc"
type OrderNulls = "first" | "last"

type OrderSpec = {
    val: Expr;
    dir: OrderDir;
    nulls: OrderNulls;
}

type OrderBy = { orderBy: OrderSpec[] }

export function orderExpr(val: Expr,
                          dir: OrderDir = 'asc',
                          nulls: OrderNulls = "first") : OrderSpec {
    return { val, dir, nulls }
}

export function orderBy(...specs: OrderSpec[]) {
    return { orderBy: specs }
}

export type Limit = { limit: number }

export function limit(limit: number) {
    return {limit}
}

export type Offset = { offset: number }

export function offset(offset: number) {
    return { offset }
}

export type Unify = { unify: UnifyClause[] }

export function unify(...clauses: UnifyClause[]): Unify {
    return { unify: clauses }
}

export type UnifyClause = From | LeftJoin | Join | Where | With | Unnest

export type QueryTail = Aggregate | Limit | Offset | OrderBy | Return | Unnest | Where | With | Without

export type Query = From | Unify | [Query, ...QueryTail[]]
