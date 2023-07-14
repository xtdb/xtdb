package xtdb.vector;

import clojure.java.api.Clojure;
import clojure.lang.*;
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.holders.NullableIntervalDayHolder;
import org.apache.arrow.vector.holders.NullableIntervalMonthDayNanoHolder;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import xtdb.types.IntervalDayTime;
import xtdb.types.IntervalMonthDayNano;
import xtdb.types.IntervalYearMonth;
import xtdb.vector.extensions.AbsentVector;
import xtdb.vector.extensions.SetVector;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.String.format;
import static java.time.temporal.ChronoUnit.MICROS;

public class ValueVectorReader implements IVectorReader {
    private static final IFn MONO_READER = Clojure.var("xtdb.vector", "->mono-reader");
    private static final IFn POLY_READER = Clojure.var("xtdb.vector", "->poly-reader");
    private static final IFn VEC_TO_READER = Clojure.var("xtdb.vector.reader", "vec->reader");
    private static final IFn FIELD_TO_COL_TYPE = Clojure.var("xtdb.types", "field->col-type");
    private static final IFn COL_TYPE_TO_LEG = Clojure.var("xtdb.types", "col-type->leg");

    private static final Keyword NULL_KEYWORD = Keyword.intern("null");
    private static final Keyword ABSENT_KEYWORD = Keyword.intern("xtdb", "absent");

    public static IVectorReader from(ValueVector v) {
        return (IVectorReader) VEC_TO_READER.invoke(v);
    }

    private final ValueVector vector;
    private final Keyword leg;

    public ValueVectorReader(ValueVector vector) {
        this.vector = vector;
        this.leg = (Keyword) COL_TYPE_TO_LEG.invoke(FIELD_TO_COL_TYPE.invoke(getField()));
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
    public IVectorReader withName(String colName) {
        return new RenamedVectorReader(this, colName);
    }

    @Override
    public Field getField() {
        return vector.getField();
    }

    @Override
    public int hashCode(int idx, ArrowBufHasher hasher) {
        return vector.hashCode(idx, hasher);
    }

    private RuntimeException unsupported() {
        throw new UnsupportedOperationException(vector.getClass().getName());
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
    public Object getObject(int idx) {
        return vector.isNull(idx) ? null : getObject0(idx);
    }

    Object getObject0(int idx) {
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
    public byte getTypeId(int idx) {
        throw unsupported();
    }

    @Override
    public Keyword getLeg(int idx) {
        return vector.isNull(idx) ? NULL_KEYWORD : leg;
    }

    @Override
    public Collection<Keyword> legs() {
        return null;
    }

    @Override
    public IVectorReader typeIdReader(byte typeId) {
        throw unsupported();
    }

    @Override
    public IVectorReader legReader(Keyword legKey) {
        if (this.leg.equals(legKey)) return this;
//        throw unsupported();
        throw new UnsupportedOperationException(format("leg=%s, legKey=%s", leg, legKey));
    }

    @Override
    public IVectorReader copyTo(ValueVector vector) {
        this.vector.makeTransferPair(vector).splitAndTransfer(0, valueCount());
        return from(vector);
    }

    @Override
    public IVectorReader transferTo(ValueVector vector) {
        if (this.vector instanceof NullVector) return this;

        this.vector.makeTransferPair(vector).transfer();
        return from(vector);
    }

    @Override
    public IRowCopier rowCopier(IVectorWriter writer) {
        return writer.rowCopier(vector);
    }

    @Override
    public IMonoVectorReader monoReader(Object colType) {
        return (IMonoVectorReader) MONO_READER.invoke(vector, colType);
    }

    @Override
    public IPolyVectorReader polyReader(Object colType) {
        return (IPolyVectorReader) POLY_READER.invoke(vector, colType);
    }

    @Override
    public Collection<? extends IVectorReader> metadataReaders() {
        return null;
    }

    @Override
    public void close() throws Exception {
        vector.close();
    }

    @Override
    public String toString() {
        return "(ValueVectorReader {vector=%s})".formatted(vector);
    }

    public static IVectorReader nullVector(NullVector v) {
        return new ValueVectorReader(v) {
            @Override
            public int hashCode(int idx, ArrowBufHasher hasher) {
                // Until https://github.com/apache/arrow/pull/35590 is merged, Arrow 13.
                return 31;
            }
        };
    }

    public static IVectorReader absentVector(AbsentVector v) {
        return new ValueVectorReader(v) {
            @Override
            public Object getObject(int idx) {
                return ABSENT_KEYWORD;
            }

            @Override
            public int hashCode(int idx, ArrowBufHasher hasher) {
                return 33;
            }
        };
    }

    public static IVectorReader bitVector(BitVector v) {
        return new ValueVectorReader(v) {
            @Override
            public boolean getBoolean(int idx) {
                return v.get(idx) != 0;
            }

            @Override
            Object getObject0(int idx) {
                return getBoolean(idx);
            }
        };
    }

    public static IVectorReader tinyIntVector(TinyIntVector v) {
        return new ValueVectorReader(v) {
            @Override
            public byte getByte(int idx) {
                return v.get(idx);
            }
        };
    }

    public static IVectorReader smallIntVector(SmallIntVector v) {
        return new ValueVectorReader(v) {
            @Override
            public short getShort(int idx) {
                return v.get(idx);
            }
        };
    }

    public static IVectorReader intVector(IntVector v) {
        return new ValueVectorReader(v) {
            @Override
            public int getInt(int idx) {
                return v.get(idx);
            }
        };
    }

    public static IVectorReader bigIntVector(BigIntVector v) {
        return new ValueVectorReader(v) {
            @Override
            public long getLong(int idx) {
                return v.get(idx);
            }
        };
    }

    public static IVectorReader float4Vector(Float4Vector v) {
        return new ValueVectorReader(v) {
            @Override
            public float getFloat(int idx) {
                return v.get(idx);
            }
        };
    }

    public static IVectorReader float8Vector(Float8Vector v) {
        return new ValueVectorReader(v) {
            @Override
            public double getDouble(int idx) {
                return v.get(idx);
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
            Object getObject0(int idx) {
                return new String(v.get(idx), StandardCharsets.UTF_8);
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
            Object getObject0(int idx) {
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
            Object getObject0(int idx) {
                return ByteBuffer.wrap(v.getObject(idx));
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
            Object getObject0(int idx) {
                return LocalDate.ofEpochDay(v.get(idx));
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
            Object getObject0(int idx) {
                return LocalDate.ofEpochDay(getInt(idx));
            }
        };
    }

    private static ZoneId zoneId(ValueVector v) {
        return ZoneId.of(((ArrowType.Timestamp) v.getField().getType()).getTimezone());
    }

    public static IVectorReader timestampSecTzVector(TimeStampSecTZVector v) {
        return new ValueVectorReader(v) {
            @Override
            public long getLong(int idx) {
                return v.get(idx);
            }

            @Override
            Object getObject0(int idx) {
                return Instant.ofEpochSecond(v.get(idx)).atZone(zoneId(v));
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
            Object getObject0(int idx) {
                return Instant.ofEpochMilli(v.get(idx)).atZone(zoneId(v));
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
            Object getObject0(int idx) {
                return Instant.EPOCH.plus(v.get(idx), MICROS).atZone(zoneId(v));
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
            Object getObject0(int idx) {
                return Instant.ofEpochSecond(0, v.get(idx)).atZone(zoneId(v));
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
            Object getObject0(int idx) {
                return LocalTime.ofSecondOfDay(v.get(idx));
            }
        };
    }

    public static IVectorReader timeMilliVector(TimeMilliVector v) {
        return new ValueVectorReader(v) {
            @Override
            public int getInt(int idx) {
                return v.get(idx);
            }

            @Override
            Object getObject0(int idx) {
                return LocalTime.ofNanoOfDay(v.get(idx) * 1_000_000L);
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
            Object getObject0(int idx) {
                return LocalTime.ofNanoOfDay(v.get(idx) * 1_000L);
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
            Object getObject0(int idx) {
                return LocalTime.ofNanoOfDay(v.get(idx));
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
            Object getObject0(int idx) {
                return new IntervalYearMonth(Period.ofMonths(getInt(idx)));
            }
        };
    }

    public static IVectorReader intervalDayVector(IntervalDayVector v) {
        var holder = new NullableIntervalDayHolder();

        return new ValueVectorReader(v) {
            @Override
            Object getObject0(int idx) {
                v.get(idx, holder);
                return new IntervalDayTime(Period.ofDays(holder.days), Duration.ofMillis(holder.milliseconds));
            }
        };
    }

    public static IVectorReader intervalMdnVector(IntervalMonthDayNanoVector v) {
        var holder = new NullableIntervalMonthDayNanoHolder();

        return new ValueVectorReader(v) {
            @Override
            Object getObject0(int idx) {
                v.get(idx, holder);
                return new IntervalMonthDayNano(Period.of(0, holder.months, holder.days), Duration.ofNanos(holder.nanoseconds));
            }
        };
    }

    public static IVectorReader structVector(StructVector v) {
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
            Object getObject0(int idx) {
                var res = new HashMap<Keyword, Object>();

                rdrs.forEach((k, v) -> {
                    Object val = v.getObject(idx);
                    if (!ABSENT_KEYWORD.equals(val)) res.put(Keyword.intern(k), val);
                });

                return PersistentArrayMap.create(res);
            }
        };
    }

    public static IVectorReader listVector(ListVector v) {
        var elReader = from(v.getDataVector());

        return new ValueVectorReader(v) {
            @Override
            Object getObject0(int idx) {
                var startIdx = getListStartIndex(idx);
                return PersistentVector.create(
                        IntStream.range(0, getListCount(idx))
                                .mapToObj(elIdx -> elReader.getObject(startIdx + elIdx))
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
        };
    }

    public static IVectorReader setVector(SetVector v) {
        var listReader = listVector(v.getUnderlyingVector());

        return new ValueVectorReader(v) {
            @Override
            Object getObject0(int idx) {
                return PersistentHashSet.create((List<?>) listReader.getObject(idx));
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
        };
    }

    private record DuvIndirection(DenseUnionVector v, byte typeId) implements IVectorIndirection {
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

        private final Keyword[] legs;
        private final IndirectVectorReader[] legReaders;
        private final Map<Keyword, IVectorReader> legReadersByLeg;

        private DuvReader(DenseUnionVector v) {
            super(v);
            this.v = v;

            var children = v.getChildrenFromFields();

            this.legReaders = new IndirectVectorReader[children.size()];

            for (int typeId = 0; typeId < legReaders.length; typeId++) {
                legReaders[typeId] = new IndirectVectorReader(from(children.get(typeId)), new DuvIndirection(v, (byte) typeId));
            }

            this.legReadersByLeg = Arrays.stream(legReaders).collect(Collectors.toMap(vr -> Keyword.intern(vr.getName()), vr -> vr));

            this.legs = Arrays.stream(legReaders).map(vr -> Keyword.intern(vr.getName())).toArray(Keyword[]::new);
        }

        @Override
        Object getObject0(int idx) {
            return legReaders[v.getTypeId(idx)].getObject(idx);
        }

        @Override
        public byte getTypeId(int idx) {
            return v.getTypeId(idx);
        }

        @Override
        public Keyword getLeg(int idx) {
            return legs[v.getTypeId(idx)];
        }

        @Override
        public IVectorReader legReader(Keyword legKey) {
            return legReadersByLeg.get(legKey);
        }

        @Override
        public Collection<Keyword> legs() {
            return Arrays.stream(legs).toList();
        }

        @Override
        public Collection<? extends IVectorReader> metadataReaders() {
            return Arrays.stream(legReaders).map(IndirectVectorReader::removeNegativeIndices).toList();
        }
    }

    public static IVectorReader denseUnionVector(DenseUnionVector v) {
        return new DuvReader(v);
    }
}
