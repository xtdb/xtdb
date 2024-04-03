import {DateTime} from 'luxon'

import {TxOp} from './tx.mjs'
import {Query} from './query.mjs'

export * as tx from './tx.mjs'
export * as q from './query.mjs'
export * as ex from './expr.mjs'

type TimeZone = string
type Duration = string

type TxKey = {
    txId: number;
    systemTime: DateTime;
};

type TxOpts = {
    systemTime?: Date
    defaultTz?: TimeZone
    defaultAllValidTime?: boolean
}

type Basis = {
    atTx?: TxKey
    currentTime?: Date
}

export enum KeyFn {
    CamelCase = "CAMEL_CASE_STRING",
    SnakeCase = "SNAKE_CASE_STRING"
}

type QueryOpts = {
    args?: {} | []
    basis?: Basis
    afterTx?: TxKey
    txTimeout?: Duration
    defaultTz?: TimeZone
    defaultAllValidTime?: boolean
    explain?: boolean
    keyFn?: KeyFn
}

function parseTxResponse({txId, systemTime}: {txId: number, systemTime: string}): TxKey {
    return { txId, systemTime: DateTime.fromISO(systemTime) }
}

function latestTx(oldTxKey: TxKey | undefined, newTxKey: TxKey): TxKey {
    return (oldTxKey === undefined || oldTxKey.txId < newTxKey.txId) ? newTxKey : oldTxKey
}

function replacer(this: any, k: any, v: any) {
    if (this === null || typeof this !== "object") return v

    const obj = this[k]

    if (obj instanceof DateTime)
        return {"@type": "xt:timestamptz", "@value": obj.toISO({extendedZone: true})}

    return obj
}

function reviver(_: any, v: any) {
    if (v === null || typeof v !== 'object' || !('@type' in v)) return v

    const {'@type': t, '@value': obj} = v

    switch(t) {
        case 'xt:timestamptz': return DateTime.fromISO(obj)
        case 'xt:keyword': return obj
        case 'xt:error': return obj
        case 'xt:set': return new Set(obj)

        default:
          throw new Error(`unknown @type: ${t}`)
    }
}

export default class Xtdb {
    constructor(private url: string) {}

    private latestSubmittedTx?: TxKey

    async status(): Promise<any> {
        const resp = await fetch(`${this.url}/status`, {
            method: 'GET',
            headers: {
                accept: "application/json",
            }
        }).catch((e) => { throw new Error(`Error in status request: ${e.message}`) })

        const body = await resp.json()

        if(!resp.ok) {
            throw new Error(`Error getting status: ${resp.status} (${resp.statusText}): ${body}`)
        }

        return body
    }

    async submitTx(ops: TxOp[], opts: TxOpts = {}): Promise<TxKey> {
        const req = {
            method: 'POST',
            body: JSON.stringify({txOps: ops, ...opts}, replacer),
            headers: {
                accept: "application/json",
                "content-type": "application/json"
            }
        }

        const resp = await fetch(`${this.url}/tx`, req)
            .catch((e) => { throw new Error(`Error in submitTx request: ${e.message}`) })

        const body = await resp.json()

        if(!resp.ok) {
            throw new Error(`Error submitting tx: ${resp.status} (${resp.statusText}): ${body}`)
        }

        const txKey = parseTxResponse(body)

        this.latestSubmittedTx = latestTx(this.latestSubmittedTx, txKey)

        return txKey
    }

    async query(query: Query, opts: QueryOpts = {}): Promise<any> {
        // FIXME
        // if (opts.afterTx === undefined) opts = { afterTx: this.latestSubmittedTx, ...opts }

        opts = { keyFn: KeyFn.CamelCase, ...opts }

        const req = {
            method: 'POST',
            body: JSON.stringify({query, queryOpts: opts}, replacer),
            headers: {
                accept: "application/json",
                "content-type": "application/json"
            }
        }

        const resp =
            await fetch(`${this.url}/query`, req)
                .catch((e) => { throw new Error(`Error in query request: ${e.message}`) })

        const body = JSON.parse(await resp.text(), reviver)

        if(!resp.ok) {
            throw new Error(`Error submitting query: ${resp.status} (${resp.statusText}): ${body}`)
        }

        return body
    }
}
