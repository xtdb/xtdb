import { Expr, sym } from './expr.mjs'

export type Binding = Record<string, Expr>

export type ToBindings = string | { [key: string]: Expr }

export function toBindings(b: ToBindings): Binding[] {
    if (typeof b === 'string') return [{ [b]: sym(b) }]
   return Object.entries(b).map(([binding, expr]) => ({ [binding]: expr }))
}
