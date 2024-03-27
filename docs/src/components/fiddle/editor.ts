import { EditorView, lineNumbers, keymap, drawSelection } from "@codemirror/view";
import { Text } from "@codemirror/state";
import { autocompletion, closeBrackets } from "@codemirror/autocomplete";
import { defaultKeymap, history, historyKeymap, indentWithTab } from "@codemirror/commands"
import { foldGutter, syntaxHighlighting, defaultHighlightStyle, bracketMatching } from "@codemirror/language";
import { sql, PostgreSQL } from "@codemirror/lang-sql";


let theme = EditorView.theme({
    "&.cm-editor": {
        height: "100%"
    },
    ".cm-content": {
        whiteSpace: "pre-wrap",
        padding: "5px 0",
        flex: "1 1 0"
    },
    "&.cm-focused": {
        outline: "0 !important"
    },
    ".cm-line": {
        lineHeight: "1.4",
        fontSize: "15px",
        fontFamily: "var(--code-font)"
    },
    ".cm-matchingBracket": {
        borderBottom: "1px solid var(--teal-color)",
        color: "inherit"
    },
    // only show cursor when focused
    ".cm-cursor": {
        visibility: "hidden"
    },
    "&.cm-focused .cm-cursor": {
        visibility: "visible"
    }
})

let extensions = [
    theme,
    history(),
    drawSelection(),
    syntaxHighlighting(defaultHighlightStyle, { fallback: true }),
    bracketMatching(),
    closeBrackets(),
    autocompletion(),
    keymap.of([
        ...defaultKeymap,
        ...historyKeymap,
        indentWithTab,
    ]),
    // TODO: Define XtdbSQL dialect?
    sql({ dialect: PostgreSQL, upperCaseKeywords: true }),
];

interface Params {
    initialText: String;
    parent: HTMLElement;
}

export function makeEditor({ initialText, parent }: Params) {
    return new EditorView({
        doc: Text.of(initialText.split("\n")),
        extensions: extensions,
        parent: parent,
    });
}
