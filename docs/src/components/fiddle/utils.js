import hljs from "highlight.js/lib/core"
import hljsSql from "highlight.js/lib/languages/sql"

hljs.registerLanguage('sql', hljsSql);

function gensym() {
     return Math.random().toString(36).substr(2);
}

function parseSQLTxs(s) {
    var txs = s
        .split(';')
        .map((x) => x.trim())
        .filter((x) => x.length > 0)
        .map((x) => JSON.stringify(x))
        .map((x) => "[:sql " + x + "]")
        .join('');
    return "[" + txs + "]";
}

const fiddle_url = "https://play.xtdb.com"
async function runFiddle(txs, query) {
    query.replace(/;\s$/, ""); // remove last semi-colon
    query = JSON.stringify(query);

    return await fetch(`${fiddle_url}/db-run`, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
            'tx-batches': txs,
            query: query
        })
    });
}

// Generator that yields elements with the same magic-context attribute
// Only yields elements above the one with the given id
function *magicElementsAbove(magicContext, id) {
    if (magicContext) {
        const els = document.querySelectorAll(`[data-magic-context="${magicContext}"]`);
        for (const el of els) {
            // Only consider elements above the current one
            if (el.dataset.id == id) {
                break;
            }
            yield el;
        }
    }
}

function makeError(title, message, data) {
    return `
    <div class="bg-[#fad1df] dark:bg-[#4e2232] border-l-4 border-[#f53d7d] dark:border-[#ee5389] text-[#8a0f3a] dark:text-[#f9c3d6] p-4">
      <p class="font-bold">${title}</p>
      <p class="whitespace-pre-wrap font-mono text-black dark:text-white">${message.replace(/(?:\r\n|\r|\n)/g, '<br>')}</p>
      ${
          data
          ? `<p class="pt-2 font-semibold">Data:</p>
             <p class="text-black dark:text-white">${JSON.stringify(data)}</p>`
          : ""
       }
    </div>`
}

function highlightSql(sql) {
    return hljs.highlight(sql, {language: 'sql'});
}

// Based on: https://www.30secondsofcode.org/js/s/debounce-promise/
// The difference is we throw away previous calls and only resolve the last one
// I'm honestly not sure of the memory implications
function debouncePromise(fn, ms = 0) {
    let timeoutId;
    let pending;
    return async (...args) => {
        return new Promise((res, rej) => {
            clearTimeout(timeoutId);
            timeoutId = setTimeout(() => {
                const currentPending = pending;
                pending = undefined;
                Promise.resolve(fn.apply(this, args)).then(
                    currentPending.resolve,
                    currentPending.reject
                );
            }, ms);
            pending = { resolve: res, reject: rej };
        });
    }
};

export { gensym, parseSQLTxs, fiddle_url, runFiddle, magicElementsAbove, makeError, highlightSql, debouncePromise };
