package core2.relation;

public interface IRowAppender {
    void appendRow(int sourceIdx);
    void appendRow(int sourceIdx, int parentIdx);
}
