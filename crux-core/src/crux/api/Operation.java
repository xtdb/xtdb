package crux.api;

import clojure.lang.PersistentVector;
import java.util.Date;
import java.util.UUID;
import java.util.List;
import java.net.URI;
import java.net.URL;

public interface Operation {
    public interface OperationBuilder {
	public OperationBuilder putValidTime(Date validTime);
	public OperationBuilder putId(String id);
	public OperationBuilder putId(UUID id);
	public OperationBuilder putId(URL id);
	public OperationBuilder putId(URI id);
	public List<Object> build();
    }
}
