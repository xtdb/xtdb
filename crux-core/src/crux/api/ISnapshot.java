package crux.api;

import java.io.Closeable;

public interface ISnapshot extends IFixedInstantQueryAPI, IReadDocumentsAPI, Closeable {}
