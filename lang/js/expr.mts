import { Query } from './query.mjs';

type ObjExpr = {
    "@type": string;
    "@value": any;
};

export function date(date: Date): ObjExpr {
    return { "@type": "xt:timestamptz", "@value": date.toISOString() }
}

export function uuid(uuid: string): ObjExpr {
    return { "@type": "xt:uuid", "@value": uuid }
}

type SymExpr = {
    "xt:lvar": string;
};

export function sym(sym: string): SymExpr {
    return { "xt:lvar": sym }
}

type ParamExpr = {
    "xt:param": string;
};

export function param(param: string): ParamExpr {
    return { "xt:param": param }
}

type CallExpr = {
    "xt:call": string;
    args: Expr[];
};

export function call(f: string, ...args: Expr[]): CallExpr {
    return { "xt:call": f, args }
}

type GetExpr = {
    "xt:get": Expr;
    field: string;
};

export function get(expr: Expr, field: string): GetExpr {
    return { "xt:get": expr, field }
}

type Subquery = {
    "xt:q": Query;
    args: Expr[];
};

export function q(q: Query, ...args: Expr[]): Subquery {
    return { "xt:q": q, args }
}

type Exists = {
    "xt:exists": Query;
    args: Expr[];
};

export function exists(q: Query, ...args: Expr[]): Exists {
    return { "xt:exists": q, args }
}

type Pull = {
    "xt:pull": Query;
    args: Expr[];
};

export function pull(q: Query, ...args: Expr[]): Pull {
    return { "xt:pull": q, args }
}

type PullMany = {
    "xt:pullMany": Query;
    args: Expr[];
};

export function pullMany(q: Query, ...args: Expr[]): PullMany {
    return { "xt:pullMany": q, args }
}

export type Expr = null | number | string | boolean
    | SymExpr | ParamExpr | CallExpr | GetExpr |
    { [key: string]: Expr } | Expr[]
    | Subquery | Exists | Pull | PullMany;
