package xtdb.query;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class QueryUtil {
    static <T> List<T> unmodifiableList(List<T> list) {
        if (list == null) return null;
        return Collections.unmodifiableList(list);
    }

    static String stringifyList(List<?> list) {
        if (list == null || list.isEmpty()) return null;
        return list.stream().map(Object::toString).collect(Collectors.joining(" "));
    }

    static String stringifyMap(Map<?, ?> map) {
        if (map == null) return null;
        return String.format("{%s}",
                map.entrySet().stream()
                        .map(e -> String.format("%s %s", e.getKey(), e.getValue()))
                        .collect(Collectors.joining(", ")));
    }

    static String stringifyArgs(Object obj, List<?> params) {
        return params == null ? obj.toString() : String.format("[%s %s]", obj, stringifyList(params));
    }

    static String stringifyOpts(Object obj, Map<?, ?> opts) {
        return opts == null ? obj.toString() : String.format("[%s %s]", obj, stringifyMap(opts));
    }

    static String stringifySeq(Object... strings) {
        return String.format("(%s)",
                Stream.of(strings).filter(Objects::nonNull).map(Object::toString).collect(Collectors.joining(" ")));
    }
}
