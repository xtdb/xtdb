package xtdb.trie;

import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;

import java.util.stream.IntStream;

public class ArrowHashTrie {

    private static final byte BRANCH_TYPE_ID = 1;
    private static final byte LEAF_TYPE_ID = 2;

    private final DenseUnionVector nodesVec;
    private final ListVector branchVec;
    private final IntVector branchElVec;
    private final StructVector leafVec;
    private final IntVector pageIdxVec;

    private ArrowHashTrie(VectorSchemaRoot trieRoot) {
        nodesVec = ((DenseUnionVector) trieRoot.getVector("nodes"));
        branchVec = (ListVector) nodesVec.getVectorByType(BRANCH_TYPE_ID);
        branchElVec = (IntVector) branchVec.getDataVector();
        leafVec = (StructVector) nodesVec.getVectorByType(LEAF_TYPE_ID);
        pageIdxVec = leafVec.getChild("page-idx", IntVector.class);
    }

    class Branch implements HashTrie {

        private final int branchVecIdx;

        public Branch(int branchVecIdx) {
            this.branchVecIdx = branchVecIdx;
        }

        @Override
        public HashTrie add(int idx) {
            throw new UnsupportedOperationException("read only trie");
        }

        private HashTrie[] getChildren() {
            return IntStream.range(branchVec.getElementStartIndex(branchVecIdx), branchVec.getElementEndIndex(branchVecIdx))
                    .mapToObj(childIdx -> forIndex(branchElVec.get(childIdx)))
                    .toArray(HashTrie[]::new);
        }

        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visitBranch(getChildren());
        }
    }

    class Leaf implements HashTrie {

        private final int leafVecIdx;

        public Leaf(int leafVecIdx) {
            this.leafVecIdx = leafVecIdx;
        }

        @Override
        public HashTrie add(int idx) {
            throw new UnsupportedOperationException("read only trie");
        }

        @Override
        public <R> R accept(Visitor<R> visitor) {
            return visitor.visitLeaf(pageIdxVec.get(leafVecIdx), null);
        }
    }
    
    private HashTrie forIndex(int idx) {
        var nodeOffset = nodesVec.getOffset(idx);

        return switch (nodesVec.getTypeId(idx)) {
            case 0 -> null;
            case BRANCH_TYPE_ID -> new Branch(nodeOffset);
            case LEAF_TYPE_ID -> new Leaf(nodeOffset);
            default -> throw new UnsupportedOperationException();
        };
    }

    public static HashTrie from(VectorSchemaRoot trieRoot) {
        return new ArrowHashTrie(trieRoot).forIndex(trieRoot.getRowCount() - 1);
    }
}
