import { EditorView, lineNumbers, keymap, drawSelection } from "@codemirror/view";
import { Compartment, Text } from "@codemirror/state";
import { autocompletion, closeBrackets } from "@codemirror/autocomplete";
import { defaultKeymap, history, historyKeymap, indentWithTab } from "@codemirror/commands"
import { foldGutter, syntaxHighlighting, defaultHighlightStyle, bracketMatching } from "@codemirror/language";
import { sql, PostgreSQL } from "@codemirror/lang-sql";
import { oneDarkHighlightStyle } from "@codemirror/theme-one-dark";
import { getTheme, onThemeChange } from "../../utils.ts";


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

let synhl = new Compartment();

let selectedTheme = undefined;
if (getTheme() == "dark") {
    selectedTheme = oneDarkHighlightStyle;
} else {
    selectedTheme = defaultHighlightStyle;
}

let extensions = [
    theme,
    history(),
    drawSelection(),
    synhl.of(syntaxHighlighting(selectedTheme, { fallback: true })),
    bracketMatching(),
    closeBrackets(),
    // Annoying in practice:
    // autocompletion(),
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
    const view = new EditorView({
        doc: Text.of(initialText.split("\n")),
        extensions: extensions,
        parent: parent,
    });

    onThemeChange((newTheme) => {
        if (newTheme == 'dark') {
            view.dispatch({ effects: synhl.reconfigure(syntaxHighlighting(oneDarkHighlightStyle, { fallback: true })) })
        } else {
            view.dispatch({ effects: synhl.reconfigure(syntaxHighlighting(defaultHighlightStyle, { fallback: true })) })
        }
    });

    return view;
}
