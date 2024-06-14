import hljs from "highlight.js/lib/core"
import hljsJson from "highlight.js/lib/languages/json"

hljs.registerLanguage('json', hljsJson);

function tableOrder(a, b) {
    if (a === b) {
        return 0;
    }
    if (a === '_id') {
        return -1;
    }
    if (b === '_id') {
        return 1;
    }
    return a < b ? -1 : 1;
        
}

function makeTable(rows) {
    if (rows.length === 0) {
        return "<p>No results</p>"
    }
    var allKeys = new Set();
    for (const row of rows) {
        for (const key of Object.keys(row)) {
            allKeys.add(key);
        }
    }
    allKeys = Array.from(allKeys);
    allKeys.sort(tableOrder);

    // Header
    var headerCols = "";
    for (const key of allKeys) {
        headerCols += `<th class="text-left p-1">${key}</th>`;
    }

    var header = `
    <thead>
        <tr>${headerCols}</tr>
    </thead>`;

    // Body
    var bodyRows = "";
    for (const row of rows) {
        var rowCols = "";
        for (const key of allKeys) {
            let value = key in row ? row[key] : null;
            let highlight = hljs.highlight(JSON.stringify(value), {language: 'json'});
            rowCols += `<td class="text-left p-1">${highlight.value}</Code></td>`;
        }
        bodyRows += `<tr class="border-gray-300 border-t">${rowCols}</tr>`;
    }

    var body = `<tbody>${bodyRows}</tbody>`;

    return `<table class="table-auto border-collapse w-full">${header}${body}</table>`
}

export { makeTable };
