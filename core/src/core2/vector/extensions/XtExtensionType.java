package core2.vector.extensions;

import org.apache.arrow.vector.types.pojo.ArrowType;

public abstract class XtExtensionType extends ArrowType.ExtensionType {
    private final String extensionName;
    private final ArrowType storageType;

    protected XtExtensionType(String extensionName, ArrowType storageType) {
        this.extensionName = extensionName;
        this.storageType = storageType;
    }

    @Override
    public ArrowType storageType() {
        return storageType;
    }

    @Override
    public String extensionName() {
        return extensionName;
    }

    @Override
    public boolean extensionEquals(ExtensionType other) {
        return getClass().isAssignableFrom(other.getClass());
    }

    @Override
    public String serialize() {
        return "";
    }

    @Override
    public ArrowType deserialize(ArrowType storageType, String serializedData) {
        if (!storageType.equals(this.storageType)) {
            throw new UnsupportedOperationException("Cannot construct XtExtensionType from underlying type " + storageType);
        } else {
            return deserialize(serializedData);
        }
    }

    protected abstract ArrowType deserialize(String serializedData);

}
