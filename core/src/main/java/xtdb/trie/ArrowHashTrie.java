package xtdb.trie;

import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowBlock;

import java.io.IOException;
import java.util.List;
import java.util.stream.IntStream;

public class ArrowHashTrie {

    private static final byte BRANCH_TYPE_ID = 1;
    private static final byte LEAF_TYPE_ID = 2;

    private final DenseUnionVector nodesVec;
    private final ListVector branchVec;
    private final IntVector branchElVec;
    private final StructVector leafVec;
    private final IntVector pageIdxVec;

    private final ArrowFileReader leafReader;
    private final List<ArrowBlock> leafBlocks;
    private final VectorSchemaRoot leafRoot;

    static class Branch implements HashTrie {

        private final HashTrie[] children;

        public Branch(HashTrie[] children) {
            this.children = children;
        }

        @Override
        public HashTrie add(int idx) {
            throw new UnsupportedOperationException("read only trie");
        }

        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visitBranch(children);
        }
    }

    class Leaf implements HashTrie {

        private final ArrowBlock leafBlock;

        public Leaf(ArrowBlock leafBlock) {
            this.leafBlock = leafBlock;
        }

        @Override
        public HashTrie add(int idx) {
            throw new UnsupportedOperationException("read only trie");
        }

        @Override
        public <R> R accept(Visitor<R> visitor) {
            try {
                leafReader.loadRecordBatch(leafBlock);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            var rowCount = leafRoot.getRowCount();

            return visitor.visitLeaf(new Leaf() {
                @Override
                public int size() {
                    return rowCount;
                }

                @Override
                public int get(int idx) {
                    return idx;
                }

                @Override
                public IntStream indices() {
                    return IntStream.range(0, rowCount);
                }
            });
        }
    }

    private ArrowHashTrie(VectorSchemaRoot trieRoot, ArrowFileReader leafReader) throws IOException {
        nodesVec = ((DenseUnionVector) trieRoot.getVector("nodes"));
        branchVec = (ListVector) nodesVec.getVectorByType(BRANCH_TYPE_ID);
        branchElVec = (IntVector) branchVec.getDataVector();
        leafVec = (StructVector) nodesVec.getVectorByType(LEAF_TYPE_ID);
        pageIdxVec = leafVec.getChild("page-idx", IntVector.class);

        this.leafReader = leafReader;
        leafBlocks = leafReader.getRecordBlocks();
        leafRoot = leafReader.getVectorSchemaRoot();
    }

    private HashTrie forIndex(int idx) {
        var nodeOffset = nodesVec.getOffset(idx);
        return switch (nodesVec.getTypeId(idx)) {
            case 0 -> null;

            case BRANCH_TYPE_ID -> new Branch(
                    IntStream.range(branchVec.getElementStartIndex(nodeOffset), branchVec.getElementEndIndex(nodeOffset))
                            .mapToObj(childIdx -> forIndex(branchElVec.get(childIdx)))
                            .toArray(HashTrie[]::new));

            case LEAF_TYPE_ID -> new Leaf(leafBlocks.get(pageIdxVec.get(nodeOffset)));

            default -> throw new UnsupportedOperationException();
        };
    }

    public static HashTrie from(VectorSchemaRoot trieRoot, ArrowFileReader leafReader) throws IOException {
        return new ArrowHashTrie(trieRoot, leafReader).forIndex(trieRoot.getRowCount() - 1);
    }
}
