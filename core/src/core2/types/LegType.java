package core2.types;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.Set;

public class LegType {
    public static final LegType NULL = new LegType(ArrowType.Null.INSTANCE);
    public static final LegType BOOL = new LegType(ArrowType.Bool.INSTANCE);
    public static final LegType TINYINT = new LegType(MinorType.TINYINT.getType());
    public static final LegType SMALLINT = new LegType(MinorType.SMALLINT.getType());
    public static final LegType INT = new LegType(MinorType.INT.getType());
    public static final LegType BIGINT = new LegType(MinorType.BIGINT.getType());
    public static final LegType FLOAT4 = new LegType(MinorType.FLOAT4.getType());
    public static final LegType FLOAT8 = new LegType(MinorType.FLOAT8.getType());
    public static final LegType TIMESTAMPMICROTZ = new LegType(new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"));
    public static final LegType DURATIONMICRO = new LegType(new ArrowType.Duration(TimeUnit.MICROSECOND));
    public static final LegType DATEDAY = new LegType(new ArrowType.Date(DateUnit.DAY));
    public static final LegType BINARY = new LegType(ArrowType.Binary.INSTANCE);
    public static final LegType UTF8 = new LegType(ArrowType.Utf8.INSTANCE);
    public static final LegType LIST = new LegType(ArrowType.List.INSTANCE);
    public static final LegType MAP = new LegType(new ArrowType.Map(false));

    public static LegType structOfKeys(Set<String> keys) {
        return new StructLegType(keys);
    }

    public final ArrowType arrowType;

    public LegType(ArrowType arrowType) {
        this.arrowType = arrowType;
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) return true;
        if (that == null || getClass() != that.getClass()) return false;

        return arrowType.equals(((LegType) that).arrowType);
    }

    @Override
    public int hashCode() {
        return arrowType.hashCode();
    }

    @Override
    public String toString() {
        return String.format("(LegType %s)", arrowType);
    }

    public static class StructLegType extends LegType {

        public final Set<String> keys;

        public StructLegType(Set<String> keys) {
            super(ArrowType.Struct.INSTANCE);
            this.keys = keys;
        }

        @Override
        public boolean equals(Object that) {
            if (this == that) return true;
            if (that == null || getClass() != that.getClass()) return false;
            return keys.equals(((StructLegType) that).keys);
        }

        @Override
        public int hashCode() {
            return keys.hashCode();
        }

        @Override
        public String toString() {
            return String.format("(StructLegType %s)", keys);
        }
    }
}
