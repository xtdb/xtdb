package xtdb.vector;

import clojure.lang.*;
import org.apache.arrow.memory.util.ArrowBufPointer;
import org.apache.arrow.memory.util.ByteFunctionHelpers;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalMonthDayNanoVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.*;
import org.apache.arrow.vector.holders.NullableIntervalDayHolder;
import org.apache.arrow.vector.holders.NullableIntervalMonthDayNanoHolder;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import xtdb.Types;
import xtdb.api.query.IKeyFn;
import xtdb.arrow.*;
import xtdb.types.IntervalDayTime;
import xtdb.types.IntervalMonthDayNano;
import xtdb.types.IntervalYearMonth;
import xtdb.util.Hasher;
import xtdb.vector.extensions.KeywordVector;
import xtdb.vector.extensions.SetVector;
import xtdb.vector.extensions.TransitVector;
import xtdb.vector.extensions.TsTzRangeVector;
import xtdb.vector.extensions.UuidVector;
import xtdb.vector.extensions.*;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.time.temporal.ChronoUnit.MICROS;
import static java.time.temporal.ChronoUnit.NANOS;
import static xtdb.time.Time.*;

public class ValueVectorReader implements IVectorReader {

    public static IVectorReader from(ValueVector v) {
        return ValueVectorReadersKt.from(v);
    }

    private final ValueVector vector;


    public ValueVectorReader(ValueVector vector) {
        this.vector = vector;
    }

    @Override
    public int valueCount() {
        return vector.getValueCount();
    }

    @Override
    public String getName() {
        return vector.getName();
    }

    @Override
    public Field getField() {
        return vector.getField();
    }

    protected int hashCode0(int idx, Hasher hasher) {
        return hasher.hash(getBytes(idx));
    }

    @Override
    public int hashCode(int idx, Hasher hasher) {
        if (isNull(idx)) return ArrowBufPointer.NULL_HASH_CODE;
        return hashCode0(idx, hasher);
    }

    private RuntimeException unsupported() {
        throw new UnsupportedOperationException(vector.getClass().getName());
    }

    @Override
    public boolean isNull(int idx) {
        return vector.isNull(idx);
    }

    @Override
    public boolean getBoolean(int idx) {
        throw unsupported();
    }

    @Override
    public byte getByte(int idx) {
        throw unsupported();
    }

    @Override
    public short getShort(int idx) {
        throw unsupported();
    }

    @Override
    public int getInt(int idx) {
        throw unsupported();
    }

    @Override
    public long getLong(int idx) {
        throw unsupported();
    }

    @Override
    public float getFloat(int idx) {
        throw unsupported();
    }

    @Override
    public double getDouble(int idx) {
        throw unsupported();
    }

    @Override
    public ByteBuffer getBytes(int idx) {
        throw unsupported();
    }


    @Override
    public ArrowBufPointer getPointer(int idx) {
        if (vector instanceof ElementAddressableVector eav) {
            return eav.getDataPointer(idx);
        } else {
            throw unsupported();
        }
    }

    @Override
    public ArrowBufPointer getPointer(int idx, ArrowBufPointer reuse) {
        if (vector instanceof ElementAddressableVector eav) {
            return eav.getDataPointer(idx, reuse);
        } else {
            throw unsupported();
        }
    }

    @Override
    public Object getObject(int idx) {
        return getObject(idx, (k) -> k);
    }

    @Override
    public Object getObject(int idx, IKeyFn<?> keyFn) {
        return vector.isNull(idx) ? null : getObject0(idx, keyFn);
    }

    protected Object getObject0(int idx, IKeyFn<?> keyFn) {
        return vector.getObject(idx);
    }

    @Override
    public IVectorReader structKeyReader(String colName) {
        throw unsupported();
    }

    @Override
    public Collection<String> structKeys() {
        return null;
    }

    @Override
    public IVectorReader listElementReader() {
        throw unsupported();
    }

    @Override
    public int getListStartIndex(int idx) {
        throw unsupported();
    }

    @Override
    public int getListCount(int idx) {
        throw unsupported();
    }

    @Override
    public IVectorReader mapKeyReader() {
        throw unsupported();
    }

    @Override
    public IVectorReader mapValueReader() {
        throw unsupported();
    }

    @Override
    public String getLeg(int idx) {
        return Types.toLeg(vector.getField().getFieldType().getType());
    }

    @Override
    public List<String> legs() {
        return List.of(Types.toLeg(vector.getField().getFieldType().getType()));
    }

    @Override
    public IVectorReader legReader(String legKey) {
        throw unsupported();
    }

    @Override
    public IVectorReader copyTo(ValueVector vector) {
        this.vector.makeTransferPair(vector).splitAndTransfer(0, valueCount());
        return from(vector);
    }

    @Override
    public RowCopier rowCopier(IVectorWriter writer) {
        return writer.rowCopier(vector);
    }

    class BaseValueReader implements ValueReader {
        private final VectorPosition pos;

        public BaseValueReader(VectorPosition pos) {
            this.pos = pos;
        }

        @Override
        public String getLeg() {
            return ValueVectorReader.this.getLeg(pos.getPosition());
        }

        @Override
        public boolean isNull() {
            return ValueVectorReader.this.isNull(pos.getPosition());
        }

        @Override
        public boolean readBoolean() {
            return ValueVectorReader.this.getBoolean(pos.getPosition());
        }

        @Override
        public byte readByte() {
            return ValueVectorReader.this.getByte(pos.getPosition());
        }

        @Override
        public short readShort() {
            return ValueVectorReader.this.getShort(pos.getPosition());
        }

        @Override
        public int readInt() {
            return ValueVectorReader.this.getInt(pos.getPosition());
        }

        @Override
        public long readLong() {
            return ValueVectorReader.this.getLong(pos.getPosition());
        }

        @Override
        public float readFloat() {
            return ValueVectorReader.this.getFloat(pos.getPosition());
        }

        @Override
        public double readDouble() {
            return ValueVectorReader.this.getDouble(pos.getPosition());
        }

        @Override
        public ByteBuffer readBytes() {
            return ValueVectorReader.this.getBytes(pos.getPosition());
        }

        @Override
        public Object readObject() {
            return ValueVectorReader.this.getObject(pos.getPosition());
        }
    }

    @Override
    public ValueReader valueReader(VectorPosition pos) {
        return new BaseValueReader(pos);
    }

    @Override
    public void close() {
        vector.close();
    }

    @Override
    public String toString() {
        return "(ValueVectorReader {vector=%s})".formatted(vector);
    }

    public static IVectorReader bitVector(BitVector v) {
        return new ValueVectorReader(v) {
            @Override
            public boolean getBoolean(int idx) {
                return v.get(idx) != 0;
            }

            @Override
            protected Object getObject0(int idx, IKeyFn<?> keyFn) {
                return getBoolean(idx);
            }

            @Override
            protected int hashCode0(int idx, Hasher hasher) {
               return hasher.hash(getBoolean(idx) ? 1 : 0);
            }
        };
    }

    public static IVectorReader tinyIntVector(TinyIntVector v) {
        return new ValueVectorReader(v) {
            @Override
            public byte getByte(int idx) {
                return v.get(idx);
            }
            @Override
            public short getShort(int idx) {
                return v.get(idx);
            }
            @Override
            public int getInt(int idx) {
                return v.get(idx);
            }
            @Override
            public long getLong(int idx) {
                return v.get(idx);
            }
            @Override
            public int hashCode0(int idx, Hasher hasher) {
                return hasher.hash((double)v.get(idx));
            }
        };
    }

    public static IVectorReader smallIntVector(SmallIntVector v) {
        return new ValueVectorReader(v) {
            @Override
            public short getShort(int idx) {
                return v.get(idx);
            }
            @Override
            public int getInt(int idx) {
                return v.get(idx);
            }
            @Override
            public long getLong(int idx) {
                return v.get(idx);
            }
            @Override
            public int hashCode0(int idx, Hasher hasher) {
                return hasher.hash((double)v.get(idx));
            }
        };
    }

    public static IVectorReader intVector(IntVector v) {
        return new ValueVectorReader(v) {
            @Override
            public int getInt(int idx) {
                return v.get(idx);
            }
            @Override
            public long getLong(int idx) {
                return v.get(idx);
            }
            @Override
            public int hashCode0(int idx, Hasher hasher) {
                return hasher.hash((double)v.get(idx));
            }
        };
    }

    public static IVectorReader bigIntVector(BigIntVector v) {
        return new ValueVectorReader(v) {
            @Override
            public long getLong(int idx) {
                return v.get(idx);
            }
            @Override
            public int hashCode0(int idx, Hasher hasher) {
                return hasher.hash((double)v.get(idx));
            }
        };
    }

    public static IVectorReader float4Vector(Float4Vector v) {
        return new ValueVectorReader(v) {
            @Override
            public float getFloat(int idx) {
                return v.get(idx);
            }
            @Override
            public double getDouble(int idx) {
                return v.get(idx);
            }
            @Override
            public int hashCode0(int idx, Hasher hasher) {
                return hasher.hash(v.get(idx));
            }
        };
    }

    public static IVectorReader float8Vector(Float8Vector v) {
        return new ValueVectorReader(v) {
            @Override
            public double getDouble(int idx) {
                return v.get(idx);
            }
            @Override
            public int hashCode0(int idx, Hasher hasher) {
                return hasher.hash(v.get(idx));
            }
        };
    }

    static ByteBuffer getBytes(ElementAddressableVector v, int idx) {
        if (v.isNull(idx)) return null;
        var abp = v.getDataPointer(idx);
        return abp.getBuf().nioBuffer(abp.getOffset(), (int) abp.getLength());
    }

    public static IVectorReader varCharVector(VarCharVector v) {
        return new ValueVectorReader(v) {
            @Override
            public ByteBuffer getBytes(int idx) {
                return getBytes(v, idx);
            }

            @Override
            protected Object getObject0(int idx, IKeyFn<?> keyFn) {
                return new String(v.get(idx), StandardCharsets.UTF_8);
            }
        };
    }

    public static IVectorReader keywordVector(KeywordVector v) {
        var underlyingVec = varCharVector(v.getUnderlyingVector());

        return new ValueVectorReader(v) {
            @Override
            public ByteBuffer getBytes(int idx) {
                return underlyingVec.getBytes(idx);
            }

            @Override
            protected Object getObject0(int idx, IKeyFn<?> keyFn) {
                return Keyword.intern((String) underlyingVec.getObject(idx));
            }
        };
    }

    public static IVectorReader uriVector(UriVector v) {
        var underlyingVec = varCharVector(v.getUnderlyingVector());

        return new ValueVectorReader(v) {
            @Override
            public ByteBuffer getBytes(int idx) {
                return underlyingVec.getBytes(idx);
            }

            @Override
            protected Object getObject0(int idx, IKeyFn<?> keyFn) {
                return URI.create((String) underlyingVec.getObject(idx));
            }
        };
    }

    public static IVectorReader transitVector(TransitVector v) {
        return new ValueVectorReader(v) {
            private final VarBinaryVector underlyingVector = v.getUnderlyingVector();

            @Override
            public ByteBuffer getBytes(int idx) {
                return getBytes(underlyingVector, idx);
            }

        };
    }

    public static IVectorReader varBinaryVector(VarBinaryVector v) {
        return new ValueVectorReader(v) {
            @Override
            public ByteBuffer getBytes(int idx) {
                return getBytes(v, idx);
            }

            @Override
            protected Object getObject0(int idx, IKeyFn<?> keyFn) {
                return ByteBuffer.wrap(v.getObject(idx));
            }
        };
    }

    public static IVectorReader fixedSizeBinaryVector(FixedSizeBinaryVector v) {
        return new ValueVectorReader(v) {
            @Override
            public ByteBuffer getBytes(int idx) {
                return getBytes(v, idx);
            }

            @Override
            protected Object getObject0(int idx, IKeyFn<?> keyFn) {
                return ByteBuffer.wrap(v.getObject(idx));
            }
        };
    }

    public static IVectorReader uuidVector(UuidVector v) {
        return new ValueVectorReader(v) {
            private final FixedSizeBinaryVector underlyingVector = v.getUnderlyingVector();

            @Override
            public ByteBuffer getBytes(int idx) {
                return getBytes(underlyingVector, idx);
            }

            @Override
            public ArrowBufPointer getPointer(int idx) {
                return underlyingVector.getDataPointer(idx);
            }

            @Override
            public ArrowBufPointer getPointer(int idx, ArrowBufPointer reuse) {
                return underlyingVector.getDataPointer(idx, reuse);
            }
        };
    }

    public static IVectorReader dateDayVector(DateDayVector v) {
        return new ValueVectorReader(v) {
            @Override
            public int getInt(int idx) {
                return v.get(idx);
            }

            @Override
            protected Object getObject0(int idx, IKeyFn<?> keyFn) {
                return LocalDate.ofEpochDay(v.get(idx));
            }

            @Override
            public int hashCode0(int idx, Hasher hasher) {
                return hasher.hash(getInt(idx) * 86_400_000L * 1_000_000L);
            }
        };
    }

    public static IVectorReader dateMilliVector(DateMilliVector v) {
        return new ValueVectorReader(v) {
            @Override
            public int getInt(int idx) {
                return (int) (v.get(idx) / 86_400_000);
            }

            @Override
            protected Object getObject0(int idx, IKeyFn<?> keyFn) {
                return LocalDate.ofEpochDay(getInt(idx));
            }

            @Override
            public int hashCode0(int idx, Hasher hasher) {
                return hasher.hash(getInt(idx) * 1_000_000L);
            }
        };
    }

    private static ZoneId zoneId(ValueVector v) {
        return ZoneId.of(((ArrowType.Timestamp) v.getField().getType()).getTimezone());
    }

    public static IVectorReader timestampVector(TimeStampVector v) {
        TimeUnit unit = ((ArrowType.Timestamp) v.getField().getType()).getUnit();

        return new ValueVectorReader(v) {
            @Override
            public long getLong(int idx) {
                return v.get(idx);
            }

            @Override
            protected Object getObject0(int idx, IKeyFn<?> keyFn) {
                return v.getObject(idx);
            }

            @Override
            public int hashCode0(int idx, Hasher hasher) {
                return hasher.hash(
                        switch (unit) {
                            case SECOND -> getLong(idx) * NANO_HZ;
                            case MILLISECOND -> getLong(idx) * MICRO_HZ;
                            case MICROSECOND -> getLong(idx) * MILLI_HZ;
                            case NANOSECOND -> getLong(idx);
                            default -> throw new IllegalStateException("Unexpected time unit: " + unit);
                        });
            }
        };
    }

    public static IVectorReader timestampSecTzVector(TimeStampSecTZVector v) {
        return new ValueVectorReader(v) {
            @Override
            public long getLong(int idx) {
                return v.get(idx);
            }

            @Override
            protected Object getObject0(int idx, IKeyFn<?> keyFn) {
                return Instant.ofEpochSecond(getLong(idx)).atZone(zoneId(v));
            }

            @Override
            public int hashCode0(int idx, Hasher hasher) {
                return hasher.hash(getLong(idx) * 1_000_000_000L);
            }
        };
    }

    public static IVectorReader timestampMilliTzVector(TimeStampMilliTZVector v) {
        return new ValueVectorReader(v) {
            @Override
            public long getLong(int idx) {
                return v.get(idx);
            }

            @Override
            protected Object getObject0(int idx, IKeyFn<?> keyFn) {
                return Instant.ofEpochMilli(getLong(idx)).atZone(zoneId(v));
            }

            @Override
            public int hashCode0(int idx, Hasher hasher) {
                return hasher.hash(getLong(idx) * 1_000_000L);
            }
        };
    }

    public static IVectorReader timestampMicroTzVector(TimeStampMicroTZVector v) {
        return new ValueVectorReader(v) {
            @Override
            public long getLong(int idx) {
                return v.get(idx);
            }

            @Override
            protected Object getObject0(int idx, IKeyFn<?> keyFn) {
                return Instant.EPOCH.plus(getLong(idx), MICROS).atZone(zoneId(v));
            }

            @Override
            public int hashCode0(int idx, Hasher hasher) {
                return hasher.hash(getLong(idx) * 1_000L);
            }
        };
    }

    public static IVectorReader timestampNanoTzVector(TimeStampNanoTZVector v) {
        return new ValueVectorReader(v) {
            @Override
            public long getLong(int idx) {
                return v.get(idx);
            }

            @Override
            protected Object getObject0(int idx, IKeyFn<?> keyFn) {
                return Instant.EPOCH.plus(getLong(idx), NANOS).atZone(zoneId(v));
            }

            @Override
            public int hashCode0(int idx, Hasher hasher) {
                return hasher.hash(getLong(idx));
            }
        };
    }

    public static IVectorReader tstzRangeVector(TsTzRangeVector v) {
        var inner = fixedSizeListVector(v.getUnderlyingVector());

        return new ValueVectorReader(v) {
            @Override
            public int hashCode0(int idx, Hasher hasher) {
                return inner.hashCode(idx, hasher);
            }

            @Override
            public IVectorReader listElementReader() {
                return inner.listElementReader();
            }

            @Override
            public int getListStartIndex(int idx) {
                return inner.getListStartIndex(idx);
            }

            @Override
            public int getListCount(int idx) {
                return inner.getListCount(idx);
            }

            @Override
            public ValueReader valueReader(VectorPosition pos) {
                return inner.valueReader(pos);
            }
        };
    }

    public static IVectorReader timeSecVector(TimeSecVector v) {
        return new ValueVectorReader(v) {
            @Override
            public long getLong(int idx) {
                return v.get(idx);
            }

            @Override
            protected Object getObject0(int idx, IKeyFn<?> keyFn) {
                return LocalTime.ofSecondOfDay(v.get(idx));
            }

            @Override
            public int hashCode0(int idx, Hasher hasher) {
                return hasher.hash(getLong(idx) * NANO_HZ);
            }
        };
    }

    public static IVectorReader timeMilliVector(TimeMilliVector v) {
        return new ValueVectorReader(v) {
            @Override
            public long getLong(int idx) {
                return v.get(idx);
            }

            @Override
            protected Object getObject0(int idx, IKeyFn<?> keyFn) {
                return LocalTime.ofNanoOfDay((long) v.get(idx) * 1_000_000L);
            }

            @Override
            public int hashCode0(int idx, Hasher hasher) {
                return hasher.hash(getLong(idx) * 1_000_000L);
            }
        };
    }

    public static IVectorReader timeMicroVector(TimeMicroVector v) {
        return new ValueVectorReader(v) {
            @Override
            public long getLong(int idx) {
                return v.get(idx);
            }

            @Override
            protected Object getObject0(int idx, IKeyFn<?> keyFn) {
                return LocalTime.ofNanoOfDay(v.get(idx) * 1_000L);
            }

            @Override
            public int hashCode0(int idx, Hasher hasher) {
                return hasher.hash(getLong(idx) * 1_000L);
            }
        };
    }

    public static IVectorReader timeNanoVector(TimeNanoVector v) {
        return new ValueVectorReader(v) {
            @Override
            public long getLong(int idx) {
                return v.get(idx);
            }

            @Override
            protected Object getObject0(int idx, IKeyFn<?> keyFn) {
                return LocalTime.ofNanoOfDay(v.get(idx));
            }

            @Override
            public int hashCode0(int idx, Hasher hasher) {
                return hasher.hash(getLong(idx));
            }
        };
    }

    public static IVectorReader intervalYearVector(IntervalYearVector v) {
        return new ValueVectorReader(v) {
            @Override
            public int getInt(int idx) {
                return v.get(idx);
            }

            @Override
            protected Object getObject0(int idx, IKeyFn<?> keyFn) {
                return new IntervalYearMonth(Period.ofMonths(getInt(idx)));
            }

            @Override
            public ValueReader valueReader(VectorPosition pos) {
                return new BaseValueReader(pos) {
                    @Override
                    public Object readObject() {
                        // return PeriodDuration as it's still required by the EE
                        return new PeriodDuration(v.getObject(pos.getPosition()), Duration.ZERO);
                    }
                };
            }

            @Override
            public int hashCode0(int idx, Hasher hasher) {
                return hasher.hash(getInt(idx));
            }
        };
    }

    public static IVectorReader intervalDayVector(IntervalDayVector v) {
        var holder = new NullableIntervalDayHolder();

        return new ValueVectorReader(v) {
            @Override
            protected IntervalDayTime getObject0(int idx, IKeyFn<?> keyFn) {
                v.get(idx, holder);
                return new IntervalDayTime(Period.ofDays(holder.days), Duration.ofMillis(holder.milliseconds));
            }

            @Override
            public ValueReader valueReader(VectorPosition pos) {
                return new BaseValueReader(pos) {
                    @Override
                    public PeriodDuration readObject() {
                        // return PeriodDuration as it's still required by the EE
                        IntervalDayTime idt = getObject0(pos.getPosition(), (k) -> k);
                        return new PeriodDuration(idt.period, idt.duration);
                    }
                };
            }

            @Override
            public int hashCode0(int idx, Hasher hasher) {
                v.get(idx, holder);
                return hasher.hash(holder.days + holder.milliseconds);
            }
        };
    }

    public static IVectorReader intervalMdnVector(IntervalMonthDayNanoVector v) {
        var holder = new NullableIntervalMonthDayNanoHolder();

        return new ValueVectorReader(v) {
            @Override
            protected Object getObject0(int idx, IKeyFn<?> keyFn) {
                v.get(idx, holder);
                return new IntervalMonthDayNano(Period.of(0, holder.months, holder.days), Duration.ofNanos(holder.nanoseconds));
            }

            @Override
            public ValueReader valueReader(VectorPosition pos) {
                return new BaseValueReader(pos) {
                    @Override
                    public PeriodDuration readObject() {
                        // return PeriodDuration as it's still required by the EE
                        return v.getObject(pos.getPosition());
                    }
                };
            }

            @Override
            public int hashCode0(int idx, Hasher hasher) {
                v.get(idx, holder);
                return hasher.hash(holder.months + holder.days + holder.nanoseconds);
            }
        };
    }

    public static IVectorReader durationVector(DurationVector v) {
        return new ValueVectorReader(v) {
            @Override
            public long getLong(int idx) {
                return DurationVector.get(v.getDataBuffer(), idx);
            }

            @Override
            public ValueReader valueReader(VectorPosition pos) {
                return new BaseValueReader(pos) {
                    @Override
                    public Object readObject() {
                        // return PeriodDuration as it's still required by the EE
                        return new PeriodDuration(Period.ZERO, v.getObject(pos.getPosition()));
                    }
                };
            }

            @Override
            public int hashCode0(int idx, Hasher hasher) {
                return hasher.hash(getLong(idx));
            }
        };
    }

    public static IVectorReader structVector(NonNullableStructVector v) {
        var childVecs = v.getChildrenFromFields();
        var rdrs = childVecs.stream().collect(Collectors.toMap(ValueVector::getName, ValueVectorReader::from));

        return new ValueVectorReader(v) {
            @Override
            public Collection<String> structKeys() {
                return rdrs.keySet();
            }

            @Override
            public IVectorReader structKeyReader(String colName) {
                return rdrs.get(colName);
            }

            @Override
            protected Object getObject0(int idx, IKeyFn<?> keyFn) {
                var res = new HashMap<>();

                rdrs.forEach((k, reader) -> {
                    Object v = reader.getObject(idx, keyFn);
                    if (v != null)
                        res.put(keyFn.denormalize(k), v);
                });

                return PersistentArrayMap.create(res);
            }

            @Override
            public ValueReader valueReader(VectorPosition pos) {
                @SuppressWarnings("resource")
                var readers = structKeys().stream().collect(Collectors.toMap(k -> k, k -> structKeyReader(k).valueReader(pos)));

                return new BaseValueReader(pos) {
                    @Override
                    public Map<String, ValueReader> readObject() {
                        return readers;
                    }
                };
            }

            @Override
            public int hashCode0(int idx, Hasher hasher) {
                int hash = 0;
                for (var reader : rdrs.values()) {
                    hash = ByteFunctionHelpers.combineHash(hash, reader.hashCode(idx, hasher));
                }
                return hash;
            }
        };
    }

    private static class ListVectorReader extends ValueVectorReader {
        private final ListVector v;
        private final IVectorReader elReader;

        public ListVectorReader(ListVector v) {
            super(v);
            this.v = v;
            this.elReader = from(v.getDataVector());
        }

        @Override
        protected Object getObject0(int idx, IKeyFn<?> keyFn) {
            var startIdx = getListStartIndex(idx);
            return PersistentVector.create(
                    IntStream.range(0, getListCount(idx))
                            .mapToObj(elIdx -> elReader.getObject(startIdx + elIdx, keyFn))
                            .toList());
        }

        @Override
        public IVectorReader listElementReader() {
            return elReader;
        }

        @Override
        public int getListStartIndex(int idx) {
            return v.getElementStartIndex(idx);
        }

        @Override
        public int getListCount(int idx) {
            return v.getElementEndIndex(idx) - v.getElementStartIndex(idx);
        }

        @Override
        public ValueReader valueReader(VectorPosition pos) {
            var elPos = VectorPosition.build();
            var elValueReader = elReader.valueReader(elPos);

            return new BaseValueReader(pos) {
                @Override
                public Object readObject() {
                    var startIdx = getListStartIndex(pos.getPosition());
                    var valueCount = getListCount(pos.getPosition());

                    return new ListValueReader() {
                        @Override
                        public int size() {
                            return valueCount;
                        }

                        @Override
                        public ValueReader nth(int elIdx) {
                            elPos.setPosition(startIdx + elIdx);
                            return elValueReader;
                        }
                    };
                }
            };
        }

        @Override
        public int hashCode0(int idx, Hasher hasher) {
            int hash = 0;
            int startIndex = getListStartIndex(idx);
            for (int i = 0; i < getListCount(idx); i++) {
                hash = ByteFunctionHelpers.combineHash(hash, elReader.hashCode(startIndex + i, hasher));
            }
            return hash;
        }
    }

    public static IVectorReader listVector(ListVector v) {
        return new ListVectorReader(v);
    }

    private static class FixedSizeListVectorReader extends ValueVectorReader {
        private final FixedSizeListVector v;
        private final IVectorReader elReader;

        public FixedSizeListVectorReader(FixedSizeListVector v) {
            super(v);
            this.v = v;
            this.elReader = from(v.getDataVector());
        }

        @Override
        protected Object getObject0(int idx, IKeyFn<?> keyFn) {
            var startIdx = getListStartIndex(idx);
            return PersistentVector.create(
                    IntStream.range(0, getListCount(idx))
                            .mapToObj(elIdx -> elReader.getObject(startIdx + elIdx, keyFn))
                            .toList());
        }

        @Override
        public IVectorReader listElementReader() {
            return elReader;
        }

        @Override
        public int getListStartIndex(int idx) {
            return v.getElementStartIndex(idx);
        }

        @Override
        public int getListCount(int idx) {
            return v.getElementEndIndex(idx) - v.getElementStartIndex(idx);
        }

        @Override
        public ValueReader valueReader(VectorPosition pos) {
            var elPos = VectorPosition.build();
            var elValueReader = elReader.valueReader(elPos);

            return new BaseValueReader(pos) {
                @Override
                public Object readObject() {
                    var startIdx = getListStartIndex(pos.getPosition());
                    var valueCount = getListCount(pos.getPosition());

                    return new ListValueReader() {
                        @Override
                        public int size() {
                            return valueCount;
                        }

                        @Override
                        public ValueReader nth(int elIdx) {
                            elPos.setPosition(startIdx + elIdx);
                            return elValueReader;
                        }
                    };
                }
            };
        }

        @Override
        public int hashCode0(int idx, Hasher hasher) {
            int hash = 0;
            int startIndex = getListStartIndex(idx);
            for (int i = 0; i < getListCount(idx); i++) {
                hash = ByteFunctionHelpers.combineHash(hash, elReader.hashCode(startIndex + i, hasher));
            }
            return hash;
        }
    }

    public static IVectorReader fixedSizeListVector(FixedSizeListVector v) {
        return new FixedSizeListVectorReader(v);
    }

    public static IVectorReader setVector(SetVector v) {
        var listReader = listVector(v.getUnderlyingVector());
        var elReader = from(v.getUnderlyingVector().getDataVector());

        return new ValueVectorReader(v) {
            @Override
            protected Object getObject0(int idx, IKeyFn<?> keyFn) {
                return PersistentHashSet.create((List<?>) listReader.getObject(idx, keyFn));
            }

            @Override
            public IVectorReader listElementReader() {
                return listReader.listElementReader();
            }

            @Override
            public int getListStartIndex(int idx) {
                return listReader.getListStartIndex(idx);
            }

            @Override
            public int getListCount(int idx) {
                return listReader.getListCount(idx);
            }

            @Override
            public ValueReader valueReader(VectorPosition pos) {
                return new BaseValueReader(pos) {
                    @Override
                    public Object readObject() {
                        return PersistentHashSet.create((List<?>) listReader.getObject(pos.getPosition()));
                    }
                };
            }

            @Override
            public int hashCode0(int idx, Hasher hasher) {
                int hash = 0;
                int startIndex = getListStartIndex(idx);
                for (int i = 0; i < getListCount(idx); i++) {
                    hash += elReader.hashCode(startIndex + i, hasher);
                }
                return hash;
            }
        };
    }

    public static IVectorReader mapVector(MapVector v) {
        var listReader = listVector(v);

        StructVector dataVector = (StructVector) v.getDataVector();
        var keyReader = from(dataVector.getVectorById(0));
        var valueReader = from(dataVector.getVectorById(1));

        return new ValueVectorReader(v) {
            @Override
            protected Object getObject0(int idx, IKeyFn<?> keyFn) {
                var startIdx = listReader.getListStartIndex(idx);
                int entryCount = listReader.getListCount(idx);

                Map<Object, Object> acc = new HashMap<>();
                for (int entryIdx = 0; entryIdx < entryCount; entryIdx++) {
                    acc.put(
                            keyReader.getObject(startIdx + entryIdx, keyFn),
                            valueReader.getObject(startIdx + entryIdx, keyFn));
                }
                return PersistentHashMap.create(acc);
            }

            @Override
            public IVectorReader mapKeyReader() {
                return keyReader;
            }

            @Override
            public IVectorReader mapValueReader() {
                return valueReader;
            }

            @Override
            public IVectorReader listElementReader() {
                return listReader.listElementReader();
            }

            @Override
            public int getListStartIndex(int idx) {
                return listReader.getListStartIndex(idx);
            }

            @Override
            public int getListCount(int idx) {
                return listReader.getListCount(idx);
            }

            @Override
            // see the comment in MapVector.kt
            public int hashCode0(int idx, Hasher hasher) {
                return listReader.hashCode(idx, hasher);
            }
        };
    }

    private record DuvIndirection(DenseUnionVector v, byte typeId) implements VectorIndirection {
        @Override
        public int valueCount() {
            return v.getValueCount();
        }

        @Override
        public int getIndex(int idx) {
            return v.getTypeId(idx) == typeId ? v.getOffset(idx) : -1;
        }
    }

    public static class DuvReader extends ValueVectorReader {
        private final DenseUnionVector v;

        private final List<String> legs;
        private final Map<String, IVectorReader> legReaders = new ConcurrentHashMap<>();

        private DuvReader(DenseUnionVector v) {
            super(v);
            this.v = v;

            // only using getChildrenFromFields because DUV.getField is so expensive.
            List<? extends ValueVector> children = v.getChildrenFromFields();
            this.legs = children.stream().map(ValueVector::getName).toList();
        }

        @Override
        public boolean isNull(int idx) {
            byte typeId = getTypeId(idx);
            return typeId < 0 || v.getVectorByType(typeId).isNull(v.getOffset(idx));
        }

        @SuppressWarnings("resource")
        @Override
        public boolean getBoolean(int idx) {
            return legReader(getLeg(idx)).getBoolean(idx);
        }

        @SuppressWarnings("resource")
        @Override
        public byte getByte(int idx) {
            return legReader(getLeg(idx)).getByte(idx);
        }

        @SuppressWarnings("resource")
        @Override
        public short getShort(int idx) {
            return legReader(getLeg(idx)).getShort(idx);
        }

        @SuppressWarnings("resource")
        @Override
        public int getInt(int idx) {
            return legReader(getLeg(idx)).getInt(idx);
        }

        @SuppressWarnings("resource")
        @Override
        public long getLong(int idx) {
            return legReader(getLeg(idx)).getLong(idx);
        }

        @SuppressWarnings("resource")
        @Override
        public float getFloat(int idx) {
            return legReader(getLeg(idx)).getFloat(idx);
        }

        @SuppressWarnings("resource")
        @Override
        public double getDouble(int idx) {
            return legReader(getLeg(idx)).getDouble(idx);
        }

        @SuppressWarnings("resource")
        @Override
        public ByteBuffer getBytes(int idx) {
            return legReader(getLeg(idx)).getBytes(idx);
        }

        @SuppressWarnings("resource")
        protected Object getObject0(int idx, IKeyFn<?> keyFn) {
            return legReader(getLeg(idx)).getObject(idx, keyFn);
        }

        @Override
        public int hashCode0(int idx, Hasher hasher) {
            return legReader(getLeg(idx)).hashCode(idx, hasher);
        }

        private byte getTypeId(int idx) {
            return v.getTypeId(idx);
        }

        @Override
        public String getLeg(int idx) {
            return legs.get(getTypeId(idx));
        }

        @SuppressWarnings("resource")
        @Override
        public IVectorReader structKeyReader(String colName) {
            // HACK - assumes that we only have one struct leg.
            IVectorReader structLeg = legReader("struct");
            if (structLeg == null) return null;

            IVectorReader structKeyReader = structLeg.structKeyReader(colName);
            if (structKeyReader == null) return null;

            return new IndirectVectorReader(structKeyReader, new DuvIndirection(v, (byte) legs.indexOf("struct")));
        }

        @Override
        public IVectorReader legReader(String legKey) {
            return legReaders.computeIfAbsent(legKey, k -> {
                var child = v.getChild(k);
                if (child == null) return null;
                return new IndirectVectorReader(ValueVectorReader.from(child), new DuvIndirection(v, ((byte) legs.indexOf(k))));
            });
        }

        @Override
        public List<String> legs() {
            return legs;
        }

        @Override
        public ValueReader valueReader(VectorPosition pos) {
            var legReaders = legs.stream().collect(Collectors.toMap(l -> l, l -> this.legReader(l).valueReader(pos)));

            return new ValueReader() {
                @Override
                public String getLeg() {
                    return DuvReader.this.getLeg(pos.getPosition());
                }

                private ValueReader legReader() {
                    return legReaders.get(getLeg());
                }

                @Override
                public boolean isNull() {
                    return legReader().isNull();
                }

                @Override
                public boolean readBoolean() {
                    return legReader().readBoolean();
                }

                @Override
                public byte readByte() {
                    return legReader().readByte();
                }

                @Override
                public short readShort() {
                    return legReader().readShort();
                }

                @Override
                public int readInt() {
                    return legReader().readInt();
                }

                @Override
                public long readLong() {
                    return legReader().readLong();
                }

                @Override
                public float readFloat() {
                    return legReader().readFloat();
                }

                @Override
                public double readDouble() {
                    return legReader().readDouble();
                }

                @Override
                public ByteBuffer readBytes() {
                    return legReader().readBytes();
                }

                @Override
                public Object readObject() {
                    return legReader().readObject();
                }
            };
        }
    }

    public static IVectorReader denseUnionVector(DenseUnionVector v) {
        return new DuvReader(v);
    }
}

