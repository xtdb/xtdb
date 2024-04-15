import { EditorView, lineNumbers, keymap, drawSelection } from "@codemirror/view";
import { Compartment, Text } from "@codemirror/state";
import { autocompletion, closeBrackets } from "@codemirror/autocomplete";
import { defaultKeymap, history, historyKeymap, indentWithTab } from "@codemirror/commands"
import { foldGutter, syntaxHighlighting, defaultHighlightStyle, bracketMatching } from "@codemirror/language";
import { sql, PostgreSQL } from "@codemirror/lang-sql";
import { oneDarkHighlightStyle, color as colorDark, oneDarkTheme } from "@codemirror/theme-one-dark";
import { getTheme, onThemeChange } from "../../utils.ts";


let themeObj = {
    "&.cm-editor": {
        height: "100%"
    },
    ".cm-content": {
        whiteSpace: "pre-wrap",
        padding: "5px 0",
        flex: "1 0 1"
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
    // Always show the cursor
    ".cm-cursor": {
        display: "block",
    },
    ".cm-cursorLayer": {
        animation: "steps(1) cm-blink 1.2s infinite",
    },
}

let darkThemeObj = structuredClone(themeObj);
darkThemeObj[".cm-content"]["caretColor"] = colorDark.cursor;
darkThemeObj[".cm-cursor, .cm-dropCursor"] = { backgroundColor: colorDark.background };
darkThemeObj["&.cm-focused > .cm-scroller > .cm-selectionLayer .cm-selectionBackground, .cm-selectionBackground, .cm-content ::selection"]
    = { backgroundColor: colorDark.selection };

let lightTheme = EditorView.theme(themeObj);
let darkTheme = EditorView.theme(darkThemeObj, { dark: true });

let theme = new Compartment();
let synhl = new Compartment();

let selectedHL = defaultHighlightStyle;
let selectedTheme = lightTheme;
if (getTheme() == "dark") {
    selectedHL = oneDarkHighlightStyle;
    selectedTheme = darkTheme;
}

let extensions = [
    theme.of(selectedTheme),
    history(),
    drawSelection(),
    synhl.of(syntaxHighlighting(selectedHL, { fallback: true })),
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
        doc: initialText,
        selection: { anchor: initialText.length },
        extensions: extensions,
        parent: parent,
    });

    onThemeChange((newTheme) => {
        if (newTheme == 'dark') {
            view.dispatch({
                effects: [
                    synhl.reconfigure(syntaxHighlighting(oneDarkHighlightStyle, { fallback: true })),
                    theme.reconfigure(darkTheme),
                ],
            })
        } else {
            view.dispatch({
                effects: [
                    synhl.reconfigure(syntaxHighlighting(defaultHighlightStyle, { fallback: true })),
                    theme.reconfigure(lightTheme),
                ],
            })
        }
    });

    return view;
}
