package core2.sql.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.function.Function;
import java.util.function.Predicate;
import clojure.lang.IObj;
import clojure.lang.IPersistentMap;
import clojure.lang.IPersistentVector;
import clojure.lang.ITransientCollection;
import clojure.lang.Keyword;
import clojure.lang.PersistentArrayMap;
import clojure.lang.PersistentVector;

public final class Parser {

    public static final class ParseState {
        public final IPersistentVector ast;
        public final int idx;

        public ParseState(final IPersistentVector ast, final int idx) {
            this.ast = ast;
            this.idx = idx;
        }
    }

    public interface IParseErrors {
        void addError(IPersistentVector error, int idx);

        int getIndex();

        Set<?> getErrors();
    }

    private static final IParseErrors NULL_PARSE_ERRORS = new IParseErrors() {
            public void addError(final IPersistentVector error, final int idx) {
            }

            public int getIndex () {
                return -1;
            }

            public Set<IPersistentVector> getErrors() {
                return null;
            }
        };

    public static final class ParseErrors implements IParseErrors {
        private final Set<IPersistentVector> errs;
        private int idx;

        public ParseErrors() {
            this.errs = new HashSet<>();
            this.idx = 0;
        }

        public void addError(final IPersistentVector error, final int idx) {
            if (this.idx == idx) {
                errs.add(error);
            } else if (this.idx < idx) {
                errs.clear();
                errs.add(error);
                this.idx = idx;
            }
        }

        public int getIndex() {
            return idx;
        }

        public Set<IPersistentVector> getErrors() {
            return errs;
        }
    }

    private static final IPersistentVector WS_ERROR = PersistentVector.create(Keyword.intern("expected"), "<WS>");

    private static int skipWhitespace(final Pattern pattern, final String in, final int idx, final IParseErrors errors) {
        final Matcher m = pattern.matcher(in).region(idx, in.length()).useTransparentBounds(true);
        if (m.lookingAt()) {
            return m.end();
        } else if (0 == idx) {
            return 0;
        } else {
            errors.addError(WS_ERROR, idx);
            return -1;
        }
    }

    public static abstract class AParser {
        public abstract ParseState parse(String in, int idx, ParseState[] memos, IParseErrors errors, final boolean hide);

        public AParser init(final AParser[] rules) {
            return this;
        }
    }

    public static final class EpsilonParser extends AParser {
        private static final IPersistentVector ERROR = PersistentVector.create(Keyword.intern("expected"), "<EOF>");
        private final Pattern wsPattern;

        public EpsilonParser(final Pattern wsPattern) {
            this.wsPattern = wsPattern;
        }

        public ParseState parse(final String in, int idx, final ParseState[] memos, final IParseErrors errors, final boolean hide) {
            idx = skipWhitespace(wsPattern, in, idx, errors);
            if (idx != -1) {
                if (idx == in.length()) {
                    return new ParseState(PersistentVector.EMPTY, idx);
                } else {
                    errors.addError(ERROR, idx);
                    return null;
                }
            } else {
                return null;
            }
        }
    }

    public static final class NonTerminalParser extends AParser {
        private final int ruleId;

        public NonTerminalParser(final int ruleId) {
            this.ruleId = ruleId;
        }

        public ParseState parse(final String in, final int idx, final ParseState[] memos, final IParseErrors errors, final boolean hide) {
            throw new UnsupportedOperationException();
        }

        public AParser init(final AParser[] rules) {
            return rules[ruleId];
        }
    }

    public static final Predicate<IPersistentVector> ALWAYS_RAW = new Predicate<IPersistentVector>() {
            public boolean test(final IPersistentVector ast) {
                return true;
            }
        };

    public static final Predicate<IPersistentVector> NEVER_RAW = new Predicate<IPersistentVector>() {
            public boolean test(final IPersistentVector ast) {
                return false;
            }
        };

    public static final Predicate<IPersistentVector> SINGLE_CHILD = new Predicate<IPersistentVector>() {
            public boolean test(final IPersistentVector ast) {
                return 1 == ast.count();
            }
        };

    public static final class RuleParser extends AParser {
        private static final Keyword START_IDX = Keyword.intern("start-idx");
        private static final Keyword END_IDX = Keyword.intern("end-idx");

        private final Keyword ruleName;
        private final Predicate<IPersistentVector> rawPred;
        private AParser parser;

        public RuleParser(final Keyword ruleName, Predicate<IPersistentVector> rawPred, final AParser parser) {
            this.ruleName = ruleName;
            this.rawPred = rawPred;
            this.parser = parser;
        }

        public ParseState parse(final String in, final int idx, final ParseState[] memos, final IParseErrors errors, final boolean hide) {
            final ParseState state = parser.parse(in, idx, memos, errors, hide);
            if (state != null) {
                if (hide) {
                    return new ParseState(PersistentVector.EMPTY, state.idx);
                } else if (rawPred.test(state.ast)) {
                    return state;
                } else {
                    final IPersistentMap meta = new PersistentArrayMap(new Object[] {START_IDX, idx, END_IDX, state.idx});
                    ITransientCollection newAst = PersistentVector.EMPTY.asTransient();
                    newAst = newAst.conj(ruleName);
                    for (Object x : ((List<?>) state.ast)) {
                        newAst = newAst.conj(x);
                    }
                    return new ParseState(PersistentVector.create((Object) ((IObj) newAst.persistent()).withMeta(meta)), state.idx);
                }
            } else {
                return null;
            }
        }

        public AParser init(final AParser[] rules) {
            parser = parser.init(rules);
            return this;
        }
    }

    private static final ParseState NOT_FOUND = new ParseState(null, -1);

    private static final int MAX_RULE_ID = 512;
    public static final int RULE_ID_SHIFT = Integer.numberOfTrailingZeros(MAX_RULE_ID);

    public static final class MemoizeParser extends AParser {
        private final RuleParser parser;
        private final int ruleId;

        public MemoizeParser(final RuleParser parser, final int ruleId) {
            this.parser = parser;
            this.ruleId = ruleId;
        }

        public ParseState parse(final String in, final int idx, final ParseState[] memos, final IParseErrors errors, final boolean hide) {
            final int memoIdx = ruleId | (idx << RULE_ID_SHIFT);
            ParseState state = memos[memoIdx];
            if (null == state) {
                state = parser.parse(in, idx, memos, errors, hide);
                memos[memoIdx] = state == null ? NOT_FOUND : state;
                return state;
            } else {
                return state == NOT_FOUND ? null : state;
            }
        }

        public AParser init(final AParser[] rules) {
            parser.init(rules);
            return this;
        }
    }

    public static final class MemoizeLeftRecParser extends AParser {
        private final RuleParser parser;
        private final int ruleId;

        public MemoizeLeftRecParser(final RuleParser parser, final int ruleId) {
            this.parser = parser;
            this.ruleId = ruleId;
        }

        public ParseState parse(final String in, final int idx, final ParseState[] memos, final IParseErrors errors, final boolean hide) {
            final int memoIdx = ruleId | (idx << RULE_ID_SHIFT);
            ParseState state = memos[memoIdx];
            if (null == state) {
                state = NOT_FOUND;
                while (true) {
                    memos[memoIdx] = state;
                    final ParseState newState = parser.parse(in, idx, memos, errors, hide);
                    if (newState != null) {
                        if (state != NOT_FOUND && newState.idx <= state.idx) {
                            memos[memoIdx] = null;
                            return state;
                        } else {
                            state = newState;
                        }
                    } else {
                        memos[memoIdx] = null;
                        return state == NOT_FOUND ? null : state;
                    }
                }
            } else {
                return state == NOT_FOUND ? null : state;
            }
        }

        public AParser init(final AParser[] rules) {
            parser.init(rules);
            return this;
        }
    }

    public static final class HideParser extends AParser {
        private AParser parser;

        public HideParser(final AParser parser) {
            this.parser = parser;
        }

        public ParseState parse(final String in, final int idx, final ParseState[] memos, final IParseErrors errors, final boolean hide) {
            return parser.parse(in, idx, memos, errors, true);
        }

        public AParser init(final AParser[] rules) {
            parser = parser.init(rules);
            return this;
        }
    }

    public static final class OptParser extends AParser {
        private AParser parser;

        public OptParser(final AParser parser) {
            this.parser = parser;
        }

        public ParseState parse(final String in, final int idx, final ParseState[] memos, final IParseErrors errors, final boolean hide) {
            final ParseState state = parser.parse(in, idx, memos, errors, hide);
            if (state != null) {
                return state;
            } else {
                return new ParseState(PersistentVector.EMPTY, idx);
            }
        }

        public AParser init(final AParser[] rules) {
            parser = parser.init(rules);
            return this;
        }
    }

    public static final class NegParser extends AParser {
        private static final Keyword UNEXPECTED = Keyword.intern("unexpected");

        private AParser parser;

        public NegParser(final AParser parser) {
            this.parser = parser;
        }

        public ParseState parse(final String in, final int idx, final ParseState[] memos, final IParseErrors errors, final boolean hide) {
            final ParseState state = parser.parse(in, idx, memos, NULL_PARSE_ERRORS, true);
            if (state != null) {
                errors.addError(PersistentVector.create(UNEXPECTED, in.substring(idx, state.idx)), idx);
                return null;
            } else {
                return new ParseState(PersistentVector.EMPTY, idx);
            }
        }

        public AParser init(final AParser[] rules) {
            parser = parser.init(rules);
            return this;
        }
    }

    public static final class RepeatParser extends AParser {
        private AParser parser;
        private final boolean isStar;

        public RepeatParser(final AParser parser, final boolean isStar) {
            this.parser = parser;
            this.isStar = isStar;
        }

        public ParseState parse(final String in, int idx, final ParseState[] memos, final IParseErrors errors, final boolean hide) {
            final List<Object> ast = new ArrayList<>();
            boolean isMatch = false;
            while (true) {
                final ParseState state = parser.parse(in, idx, memos, errors, hide);
                if (state != null) {
                    isMatch = true;
                    idx = state.idx;
                    if (!hide) {
                        ast.addAll((List<?>) state.ast);
                    }
                } else {
                    if (isStar || isMatch) {
                        return new ParseState(PersistentVector.create(ast), idx);
                    } else {
                        return null;
                    }
                }
            }
        }

        public AParser init(final AParser[] rules) {
            parser = parser.init(rules);
            return this;
        }
    }

    public static final class CatParser extends AParser {
        private final AParser[] parsers;

        public CatParser(final List<AParser> parsers) {
            this.parsers = parsers.toArray(new AParser[parsers.size()]);
        }

        public ParseState parse(final String in, int idx, final ParseState[] memos, final IParseErrors errors, final boolean hide) {
            final List<Object> ast = new ArrayList<>();
            for (int i = 0; i < parsers.length; i++) {
                final ParseState state = parsers[i].parse(in, idx, memos, errors, hide);
                if (state != null) {
                    idx = state.idx;
                    if (!hide) {
                        ast.addAll((List<?>) state.ast);
                    }
                } else {
                    return null;
                }
            }
            return new ParseState(PersistentVector.create(ast), idx);
        }

        public AParser init(final AParser[] rules) {
            for (int i = 0; i < parsers.length; i++) {
                parsers[i] = parsers[i].init(rules);
            }
            return this;
        }
    }

    public static final class AltParser extends AParser {
        private final AParser[] parsers;

        public AltParser(final List<AParser> parsers) {
            this.parsers = parsers.toArray(new AParser[parsers.size()]);
        }

        public ParseState parse(final String in, final int idx, final ParseState[] memos, final IParseErrors errors, final boolean hide) {
            ParseState state1 = null;
            for (int i = 0; i < parsers.length; i++) {
                final ParseState state2 = parsers[i].parse(in, idx, memos, errors, hide);
                if (state1 == null || (state2 != null && state2.idx > state1.idx)) {
                    state1 = state2;
                }
            }
            return state1;
        }

        public AParser init(final AParser[] rules) {
            for (int i = 0; i < parsers.length; i++) {
                parsers[i] = parsers[i].init(rules);
            }
            return this;
        }
    }

    public static final class OrdParser extends AParser {
        private final AParser[] parsers;

        public OrdParser(final List<AParser> parsers) {
            this.parsers = parsers.toArray(new AParser[parsers.size()]);
        }

        public ParseState parse(final String in, final int idx, final ParseState[] memos, final IParseErrors errors, final boolean hide) {
            for (int i = 0; i < parsers.length; i++) {
                final ParseState state = parsers[i].parse(in, idx, memos, errors, hide);
                if (state != null) {
                    return state;
                }
            }
            return null;
        }

        public AParser init(final AParser[] rules) {
            for (int i = 0; i < parsers.length; i++) {
                parsers[i] = parsers[i].init(rules);
            }
            return this;
        }
    }

    public static final class StringParser extends AParser {
        private final String string;
        private final IPersistentVector ast;
        private final IPersistentVector err;
        private final Pattern wsPattern;

        public StringParser(final String string, final Pattern wsPattern) {
            this.string = string;
            this.ast = PersistentVector.create(string);
            this.err = PersistentVector.create(Keyword.intern("expected"), string);
            this.wsPattern = wsPattern;
        }

        public ParseState parse(final String in, int idx, final ParseState[] memos, final IParseErrors errors, final boolean hide) {
            idx = skipWhitespace(wsPattern, in, idx, errors);
            if (idx != -1) {
                if (in.regionMatches(true, idx, string, 0, string.length())) {
                    return new ParseState(hide ? PersistentVector.EMPTY : ast, idx + string.length());
                } else {
                    errors.addError(err, idx);
                    return null;
                }
            } else {
                return null;
            }
        }
    }

    public static final class RegexpParser extends AParser {
        private final Pattern pattern;
        private final IPersistentVector err;
        private final Function<Matcher, IPersistentVector> matcherFn;
        private final Pattern wsPattern;

        public RegexpParser(final Pattern pattern, final IPersistentVector err, final Function<Matcher, IPersistentVector> matcherFn, final Pattern wsPattern) {
            this.pattern = pattern;
            this.err = err;
            this.matcherFn = matcherFn;
            this.wsPattern = wsPattern;
        }

        public ParseState parse(final String in, int idx, final ParseState[] memos, final IParseErrors errors, final boolean hide) {
            idx = skipWhitespace(wsPattern, in, idx, errors);
            if (idx != -1) {
                final Matcher m = pattern.matcher(in).region(idx, in.length()).useTransparentBounds(true);
                if (m.lookingAt()) {
                    return new ParseState(hide ? PersistentVector.EMPTY : matcherFn.apply(m), m.end());
                } else {
                    errors.addError(err, idx);
                    return null;
                }
            } else {
                return null;
            }
        }
    }
}
