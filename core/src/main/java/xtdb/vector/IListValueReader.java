package xtdb.vector;

public interface IListValueReader {
    int size();

    IValueReader nth(int idx);
}
