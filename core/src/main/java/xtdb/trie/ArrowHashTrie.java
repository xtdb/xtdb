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

    public interface NodeVisitor<R> {
        R visitBranch(Branch branch);

        R visitLeaf(Leaf leaf);
    }

    public sealed interface Node {
        <R> R accept(NodeVisitor<R> visitor);
    }

    public final class Branch implements Node {

        private final int branchVecIdx;

        public Branch(int branchVecIdx) {
            this.branchVecIdx = branchVecIdx;
        }

        public Node[] getChildren() {
            return IntStream.range(branchVec.getElementStartIndex(branchVecIdx), branchVec.getElementEndIndex(branchVecIdx))
                    .mapToObj(childIdx -> forIndex(branchElVec.get(childIdx)))
                    .toArray(Node[]::new);
        }

        @Override
        public <R> R accept(NodeVisitor<R> visitor) {
            return visitor.visitBranch(this);
        }
    }

    public final class Leaf implements Node {

        private final int leafVecIdx;

        public Leaf(int leafVecIdx) {
            this.leafVecIdx = leafVecIdx;
        }

        public int getPageIndex() {
            return pageIdxVec.get(leafVecIdx);
        }

        @Override
        public <R> R accept(NodeVisitor<R> visitor) {
            return visitor.visitLeaf(this);
        }
    }
    
    private Node forIndex(int idx) {
        var nodeOffset = nodesVec.getOffset(idx);

        return switch (nodesVec.getTypeId(idx)) {
            case 0 -> null;
            case BRANCH_TYPE_ID -> new Branch(nodeOffset);
            case LEAF_TYPE_ID -> new Leaf(nodeOffset);
            default -> throw new UnsupportedOperationException();
        };
    }

    public static Node from(VectorSchemaRoot trieRoot) {
        return new ArrowHashTrie(trieRoot).forIndex(trieRoot.getRowCount() - 1);
    }
}
