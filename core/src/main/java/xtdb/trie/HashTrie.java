package xtdb.trie;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public interface HashTrie<N extends HashTrie.Node<N>> {

    Node<N> rootNode();

    default List<? extends Node<N>> leaves() {
        return rootNode().leaves();
    }

    interface Node<N extends Node<N>> {
        byte[] path();

        N[] children();

        default Stream<? extends HashTrie.Node<N>> leafStream() {
            var children = children();
            return children == null
                    ? Stream.of(this)
                    : Arrays.stream(children).flatMap(child -> child == null ? null : child.leafStream());
        }

        default List<? extends Node<N>> leaves() {
            return leafStream().toList();
        }
    }
}
