

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.vector.complex;


import static org.apache.arrow.util.Preconditions.checkArgument;
import static org.apache.arrow.util.Preconditions.checkState;

import com.google.flatbuffers.FlatBufferBuilder;

import org.apache.arrow.memory.*;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.Types.*;
import org.apache.arrow.vector.types.pojo.*;
import org.apache.arrow.vector.types.pojo.ArrowType.*;
import org.apache.arrow.vector.types.*;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.holders.*;
import org.apache.arrow.vector.util.*;
import org.apache.arrow.vector.complex.*;
import org.apache.arrow.vector.complex.reader.*;
import org.apache.arrow.vector.complex.impl.*;
import org.apache.arrow.vector.complex.writer.*;
import org.apache.arrow.vector.complex.writer.BaseWriter.StructWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter.MapWriter;
import org.apache.arrow.vector.util.JsonStringArrayList;

import java.util.Arrays;
import java.util.Random;
import java.util.List;

import java.io.Closeable;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.ZonedDateTime;


import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.util.CommonUtil;
import org.apache.arrow.vector.compare.VectorVisitor;
import org.apache.arrow.vector.complex.impl.ComplexCopier;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.ValueVectorUtility;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.memory.util.ArrowBufPointer;
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.util.OversizedAllocationException;
import org.apache.arrow.util.Preconditions;

import static org.apache.arrow.vector.types.UnionMode.Sparse;
import static org.apache.arrow.memory.util.LargeMemoryUtil.checkedCastToInt;
import static org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt;


/*
 * This class is generated using freemarker and the UnionVector.java template.
 */
@SuppressWarnings("unused")


/**
 * A vector which can hold values of different types. It does so by using a StructVector which contains a vector for each
 * primitive type that is stored. StructVector is used in order to take advantage of its serialization/deserialization methods,
 * as well as the addOrGet method.
 *
 * For performance reasons, UnionVector stores a cached reference to each subtype vector, to avoid having to do the struct lookup
 * each time the vector is accessed.
 * Source code generated using FreeMarker template UnionVector.java
 */
public class UnionVector extends AbstractContainerVector implements FieldVector {
  int valueCount;

  NonNullableStructVector internalStruct;
  protected ArrowBuf typeBuffer;

  private StructVector structVector;
  private ListVector listVector;
  private MapVector mapVector;

  private FieldReader reader;

  private int singleType = 0;
  private ValueVector singleVector;

  private int typeBufferAllocationSizeInBytes;

  private final FieldType fieldType;
  private final Field[] typeIds = new Field[Byte.MAX_VALUE + 1];

  public static final byte TYPE_WIDTH = 1;
  private static final FieldType INTERNAL_STRUCT_TYPE = new FieldType(false /*nullable*/,
      ArrowType.Struct.INSTANCE, null /*dictionary*/, null /*metadata*/);

  public static UnionVector empty(String name, BufferAllocator allocator) {
    FieldType fieldType = FieldType.nullable(new ArrowType.Union(
        UnionMode.Sparse, null));
    return new UnionVector(name, allocator, fieldType, null);
  }

  public UnionVector(String name, BufferAllocator allocator, FieldType fieldType, CallBack callBack) {
    super(name, allocator, callBack);
    this.fieldType = fieldType;
    this.internalStruct = new NonNullableStructVector(
        "internal",
        allocator,
        INTERNAL_STRUCT_TYPE,
        callBack,
        AbstractStructVector.ConflictPolicy.CONFLICT_REPLACE,
        false);
    this.typeBuffer = allocator.getEmpty();
    this.typeBufferAllocationSizeInBytes = BaseValueVector.INITIAL_VALUE_ALLOCATION * TYPE_WIDTH;
  }

  public BufferAllocator getAllocator() {
    return allocator;
  }

  @Override
  public MinorType getMinorType() {
    return MinorType.UNION;
  }

  @Override
  public void initializeChildrenFromFields(List<Field> children) {
    int count = 0;
    for (Field child: children) {
      int typeId = Types.getMinorTypeForArrowType(child.getType()).ordinal();
      if (this.fieldType != null) {
        int[] typeIds = ((ArrowType.Union)this.fieldType.getType()).getTypeIds();
        if (typeIds != null) {
          typeId = typeIds[count++];
        }
      }
      typeIds[typeId] = child;
    }
    internalStruct.initializeChildrenFromFields(children);
  }

  @Override
  public List<FieldVector> getChildrenFromFields() {
    return internalStruct.getChildrenFromFields();
  }

  @Override
  public void loadFieldBuffers(ArrowFieldNode fieldNode, List<ArrowBuf> ownBuffers) {
    if (ownBuffers.size() != 1) {
      throw new IllegalArgumentException("Illegal buffer count, expected 1, got: " + ownBuffers.size());
    }
    ArrowBuf buffer = ownBuffers.get(0);
    typeBuffer.getReferenceManager().release();
    typeBuffer = buffer.getReferenceManager().retain(buffer, allocator);
    typeBufferAllocationSizeInBytes = checkedCastToInt(typeBuffer.capacity());
    this.valueCount = fieldNode.getLength();
  }

  @Override
  public List<ArrowBuf> getFieldBuffers() {
    List<ArrowBuf> result = new ArrayList<>(1);
    setReaderAndWriterIndex();
    result.add(typeBuffer);

    return result;
  }

  private void setReaderAndWriterIndex() {
    typeBuffer.readerIndex(0);
    typeBuffer.writerIndex(valueCount * TYPE_WIDTH);
  }

  /**
   * Get the inner vectors.
   *
   * @deprecated This API will be removed as the current implementations no longer support inner vectors.
   *
   * @return the inner vectors for this field as defined by the TypeLayout
   */
  @Deprecated
  @Override
  public List<BufferBacked> getFieldInnerVectors() {
     throw new UnsupportedOperationException("There are no inner vectors. Use geFieldBuffers");
  }

  private String fieldName(MinorType type) {
    return type.name().toLowerCase();
  }

  private FieldType fieldType(MinorType type) {
    return FieldType.nullable(type.getType());
  }

  private <T extends FieldVector> T addOrGet(Types.MinorType minorType, Class<T> c) {
    return addOrGet(null, minorType, c);
  }

  private <T extends FieldVector> T addOrGet(String name, Types.MinorType minorType, ArrowType arrowType, Class<T> c) {
    return internalStruct.addOrGet(name == null ? fieldName(minorType) : name, FieldType.nullable(arrowType), c);
  }

  private <T extends FieldVector> T addOrGet(String name, Types.MinorType minorType, Class<T> c) {
    return internalStruct.addOrGet(name == null ? fieldName(minorType) : name, fieldType(minorType), c);
  }


  @Override
  public long getValidityBufferAddress() {
    throw new UnsupportedOperationException();
  }

  public long getTypeBufferAddress() {
    return typeBuffer.memoryAddress();
  }

  @Override
  public long getDataBufferAddress() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getOffsetBufferAddress() {
    throw new UnsupportedOperationException();
  }

  public ArrowBuf getTypeBuffer() {
    return typeBuffer;
  }

  @Override
  public ArrowBuf getValidityBuffer() { throw new UnsupportedOperationException(); }

  @Override
  public ArrowBuf getDataBuffer() { throw new UnsupportedOperationException(); }

  @Override
  public ArrowBuf getOffsetBuffer() { throw new UnsupportedOperationException(); }

  public StructVector getStruct() {
    if (structVector == null) {
      int vectorCount = internalStruct.size();
      structVector = addOrGet(MinorType.STRUCT, StructVector.class);
      if (internalStruct.size() > vectorCount) {
        structVector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return structVector;
  }

  private TinyIntVector tinyIntVector;

  public TinyIntVector getTinyIntVector() {
    return getTinyIntVector(null);
  }

  public TinyIntVector getTinyIntVector(String name) {
    if (tinyIntVector == null) {
      int vectorCount = internalStruct.size();
      tinyIntVector = addOrGet(name, MinorType.TINYINT, TinyIntVector.class);
      if (internalStruct.size() > vectorCount) {
        tinyIntVector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return tinyIntVector;
  }

  private UInt1Vector uInt1Vector;

  public UInt1Vector getUInt1Vector() {
    return getUInt1Vector(null);
  }

  public UInt1Vector getUInt1Vector(String name) {
    if (uInt1Vector == null) {
      int vectorCount = internalStruct.size();
      uInt1Vector = addOrGet(name, MinorType.UINT1, UInt1Vector.class);
      if (internalStruct.size() > vectorCount) {
        uInt1Vector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return uInt1Vector;
  }

  private UInt2Vector uInt2Vector;

  public UInt2Vector getUInt2Vector() {
    return getUInt2Vector(null);
  }

  public UInt2Vector getUInt2Vector(String name) {
    if (uInt2Vector == null) {
      int vectorCount = internalStruct.size();
      uInt2Vector = addOrGet(name, MinorType.UINT2, UInt2Vector.class);
      if (internalStruct.size() > vectorCount) {
        uInt2Vector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return uInt2Vector;
  }

  private SmallIntVector smallIntVector;

  public SmallIntVector getSmallIntVector() {
    return getSmallIntVector(null);
  }

  public SmallIntVector getSmallIntVector(String name) {
    if (smallIntVector == null) {
      int vectorCount = internalStruct.size();
      smallIntVector = addOrGet(name, MinorType.SMALLINT, SmallIntVector.class);
      if (internalStruct.size() > vectorCount) {
        smallIntVector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return smallIntVector;
  }

  private IntVector intVector;

  public IntVector getIntVector() {
    return getIntVector(null);
  }

  public IntVector getIntVector(String name) {
    if (intVector == null) {
      int vectorCount = internalStruct.size();
      intVector = addOrGet(name, MinorType.INT, IntVector.class);
      if (internalStruct.size() > vectorCount) {
        intVector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return intVector;
  }

  private UInt4Vector uInt4Vector;

  public UInt4Vector getUInt4Vector() {
    return getUInt4Vector(null);
  }

  public UInt4Vector getUInt4Vector(String name) {
    if (uInt4Vector == null) {
      int vectorCount = internalStruct.size();
      uInt4Vector = addOrGet(name, MinorType.UINT4, UInt4Vector.class);
      if (internalStruct.size() > vectorCount) {
        uInt4Vector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return uInt4Vector;
  }

  private Float4Vector float4Vector;

  public Float4Vector getFloat4Vector() {
    return getFloat4Vector(null);
  }

  public Float4Vector getFloat4Vector(String name) {
    if (float4Vector == null) {
      int vectorCount = internalStruct.size();
      float4Vector = addOrGet(name, MinorType.FLOAT4, Float4Vector.class);
      if (internalStruct.size() > vectorCount) {
        float4Vector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return float4Vector;
  }

  private DateDayVector dateDayVector;

  public DateDayVector getDateDayVector() {
    return getDateDayVector(null);
  }

  public DateDayVector getDateDayVector(String name) {
    if (dateDayVector == null) {
      int vectorCount = internalStruct.size();
      dateDayVector = addOrGet(name, MinorType.DATEDAY, DateDayVector.class);
      if (internalStruct.size() > vectorCount) {
        dateDayVector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return dateDayVector;
  }

  private IntervalYearVector intervalYearVector;

  public IntervalYearVector getIntervalYearVector() {
    return getIntervalYearVector(null);
  }

  public IntervalYearVector getIntervalYearVector(String name) {
    if (intervalYearVector == null) {
      int vectorCount = internalStruct.size();
      intervalYearVector = addOrGet(name, MinorType.INTERVALYEAR, IntervalYearVector.class);
      if (internalStruct.size() > vectorCount) {
        intervalYearVector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return intervalYearVector;
  }

  private TimeSecVector timeSecVector;

  public TimeSecVector getTimeSecVector() {
    return getTimeSecVector(null);
  }

  public TimeSecVector getTimeSecVector(String name) {
    if (timeSecVector == null) {
      int vectorCount = internalStruct.size();
      timeSecVector = addOrGet(name, MinorType.TIMESEC, TimeSecVector.class);
      if (internalStruct.size() > vectorCount) {
        timeSecVector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return timeSecVector;
  }

  private TimeMilliVector timeMilliVector;

  public TimeMilliVector getTimeMilliVector() {
    return getTimeMilliVector(null);
  }

  public TimeMilliVector getTimeMilliVector(String name) {
    if (timeMilliVector == null) {
      int vectorCount = internalStruct.size();
      timeMilliVector = addOrGet(name, MinorType.TIMEMILLI, TimeMilliVector.class);
      if (internalStruct.size() > vectorCount) {
        timeMilliVector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return timeMilliVector;
  }

  private BigIntVector bigIntVector;

  public BigIntVector getBigIntVector() {
    return getBigIntVector(null);
  }

  public BigIntVector getBigIntVector(String name) {
    if (bigIntVector == null) {
      int vectorCount = internalStruct.size();
      bigIntVector = addOrGet(name, MinorType.BIGINT, BigIntVector.class);
      if (internalStruct.size() > vectorCount) {
        bigIntVector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return bigIntVector;
  }

  private UInt8Vector uInt8Vector;

  public UInt8Vector getUInt8Vector() {
    return getUInt8Vector(null);
  }

  public UInt8Vector getUInt8Vector(String name) {
    if (uInt8Vector == null) {
      int vectorCount = internalStruct.size();
      uInt8Vector = addOrGet(name, MinorType.UINT8, UInt8Vector.class);
      if (internalStruct.size() > vectorCount) {
        uInt8Vector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return uInt8Vector;
  }

  private Float8Vector float8Vector;

  public Float8Vector getFloat8Vector() {
    return getFloat8Vector(null);
  }

  public Float8Vector getFloat8Vector(String name) {
    if (float8Vector == null) {
      int vectorCount = internalStruct.size();
      float8Vector = addOrGet(name, MinorType.FLOAT8, Float8Vector.class);
      if (internalStruct.size() > vectorCount) {
        float8Vector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return float8Vector;
  }

  private DateMilliVector dateMilliVector;

  public DateMilliVector getDateMilliVector() {
    return getDateMilliVector(null);
  }

  public DateMilliVector getDateMilliVector(String name) {
    if (dateMilliVector == null) {
      int vectorCount = internalStruct.size();
      dateMilliVector = addOrGet(name, MinorType.DATEMILLI, DateMilliVector.class);
      if (internalStruct.size() > vectorCount) {
        dateMilliVector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return dateMilliVector;
  }

  private DurationVector durationVector;

  public DurationVector getDurationVector() {
    if (durationVector == null) {
      throw new IllegalArgumentException("No Duration present. Provide ArrowType argument to create a new vector");
    }
    return durationVector;
  }
  public DurationVector getDurationVector(ArrowType arrowType) {
    return getDurationVector(null, arrowType);
  }
  public DurationVector getDurationVector(String name, ArrowType arrowType) {
    if (durationVector == null) {
      int vectorCount = internalStruct.size();
      durationVector = addOrGet(name, MinorType.DURATION, arrowType, DurationVector.class);
      if (internalStruct.size() > vectorCount) {
        durationVector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return durationVector;
  }

  private TimeStampSecVector timeStampSecVector;

  public TimeStampSecVector getTimeStampSecVector() {
    return getTimeStampSecVector(null);
  }

  public TimeStampSecVector getTimeStampSecVector(String name) {
    if (timeStampSecVector == null) {
      int vectorCount = internalStruct.size();
      timeStampSecVector = addOrGet(name, MinorType.TIMESTAMPSEC, TimeStampSecVector.class);
      if (internalStruct.size() > vectorCount) {
        timeStampSecVector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return timeStampSecVector;
  }

  private TimeStampMilliVector timeStampMilliVector;

  public TimeStampMilliVector getTimeStampMilliVector() {
    return getTimeStampMilliVector(null);
  }

  public TimeStampMilliVector getTimeStampMilliVector(String name) {
    if (timeStampMilliVector == null) {
      int vectorCount = internalStruct.size();
      timeStampMilliVector = addOrGet(name, MinorType.TIMESTAMPMILLI, TimeStampMilliVector.class);
      if (internalStruct.size() > vectorCount) {
        timeStampMilliVector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return timeStampMilliVector;
  }

  private TimeStampMicroVector timeStampMicroVector;

  public TimeStampMicroVector getTimeStampMicroVector() {
    return getTimeStampMicroVector(null);
  }

  public TimeStampMicroVector getTimeStampMicroVector(String name) {
    if (timeStampMicroVector == null) {
      int vectorCount = internalStruct.size();
      timeStampMicroVector = addOrGet(name, MinorType.TIMESTAMPMICRO, TimeStampMicroVector.class);
      if (internalStruct.size() > vectorCount) {
        timeStampMicroVector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return timeStampMicroVector;
  }

  private TimeStampNanoVector timeStampNanoVector;

  public TimeStampNanoVector getTimeStampNanoVector() {
    return getTimeStampNanoVector(null);
  }

  public TimeStampNanoVector getTimeStampNanoVector(String name) {
    if (timeStampNanoVector == null) {
      int vectorCount = internalStruct.size();
      timeStampNanoVector = addOrGet(name, MinorType.TIMESTAMPNANO, TimeStampNanoVector.class);
      if (internalStruct.size() > vectorCount) {
        timeStampNanoVector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return timeStampNanoVector;
  }

  private TimeStampSecTZVector timeStampSecTZVector;

  public TimeStampSecTZVector getTimeStampSecTZVector() {
    if (timeStampSecTZVector == null) {
      throw new IllegalArgumentException("No TimeStampSecTZ present. Provide ArrowType argument to create a new vector");
    }
    return timeStampSecTZVector;
  }
  public TimeStampSecTZVector getTimeStampSecTZVector(ArrowType arrowType) {
    return getTimeStampSecTZVector(null, arrowType);
  }
  public TimeStampSecTZVector getTimeStampSecTZVector(String name, ArrowType arrowType) {
    if (timeStampSecTZVector == null) {
      int vectorCount = internalStruct.size();
      timeStampSecTZVector = addOrGet(name, MinorType.TIMESTAMPSECTZ, arrowType, TimeStampSecTZVector.class);
      if (internalStruct.size() > vectorCount) {
        timeStampSecTZVector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return timeStampSecTZVector;
  }

  private TimeStampMilliTZVector timeStampMilliTZVector;

  public TimeStampMilliTZVector getTimeStampMilliTZVector() {
    if (timeStampMilliTZVector == null) {
      throw new IllegalArgumentException("No TimeStampMilliTZ present. Provide ArrowType argument to create a new vector");
    }
    return timeStampMilliTZVector;
  }
  public TimeStampMilliTZVector getTimeStampMilliTZVector(ArrowType arrowType) {
    return getTimeStampMilliTZVector(null, arrowType);
  }
  public TimeStampMilliTZVector getTimeStampMilliTZVector(String name, ArrowType arrowType) {
    if (timeStampMilliTZVector == null) {
      int vectorCount = internalStruct.size();
      timeStampMilliTZVector = addOrGet(name, MinorType.TIMESTAMPMILLITZ, arrowType, TimeStampMilliTZVector.class);
      if (internalStruct.size() > vectorCount) {
        timeStampMilliTZVector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return timeStampMilliTZVector;
  }

  private TimeStampMicroTZVector timeStampMicroTZVector;

  public TimeStampMicroTZVector getTimeStampMicroTZVector() {
    if (timeStampMicroTZVector == null) {
      throw new IllegalArgumentException("No TimeStampMicroTZ present. Provide ArrowType argument to create a new vector");
    }
    return timeStampMicroTZVector;
  }
  public TimeStampMicroTZVector getTimeStampMicroTZVector(ArrowType arrowType) {
    return getTimeStampMicroTZVector(null, arrowType);
  }
  public TimeStampMicroTZVector getTimeStampMicroTZVector(String name, ArrowType arrowType) {
    if (timeStampMicroTZVector == null) {
      int vectorCount = internalStruct.size();
      timeStampMicroTZVector = addOrGet(name, MinorType.TIMESTAMPMICROTZ, arrowType, TimeStampMicroTZVector.class);
      if (internalStruct.size() > vectorCount) {
        timeStampMicroTZVector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return timeStampMicroTZVector;
  }

  private TimeStampNanoTZVector timeStampNanoTZVector;

  public TimeStampNanoTZVector getTimeStampNanoTZVector() {
    if (timeStampNanoTZVector == null) {
      throw new IllegalArgumentException("No TimeStampNanoTZ present. Provide ArrowType argument to create a new vector");
    }
    return timeStampNanoTZVector;
  }
  public TimeStampNanoTZVector getTimeStampNanoTZVector(ArrowType arrowType) {
    return getTimeStampNanoTZVector(null, arrowType);
  }
  public TimeStampNanoTZVector getTimeStampNanoTZVector(String name, ArrowType arrowType) {
    if (timeStampNanoTZVector == null) {
      int vectorCount = internalStruct.size();
      timeStampNanoTZVector = addOrGet(name, MinorType.TIMESTAMPNANOTZ, arrowType, TimeStampNanoTZVector.class);
      if (internalStruct.size() > vectorCount) {
        timeStampNanoTZVector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return timeStampNanoTZVector;
  }

  private TimeMicroVector timeMicroVector;

  public TimeMicroVector getTimeMicroVector() {
    return getTimeMicroVector(null);
  }

  public TimeMicroVector getTimeMicroVector(String name) {
    if (timeMicroVector == null) {
      int vectorCount = internalStruct.size();
      timeMicroVector = addOrGet(name, MinorType.TIMEMICRO, TimeMicroVector.class);
      if (internalStruct.size() > vectorCount) {
        timeMicroVector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return timeMicroVector;
  }

  private TimeNanoVector timeNanoVector;

  public TimeNanoVector getTimeNanoVector() {
    return getTimeNanoVector(null);
  }

  public TimeNanoVector getTimeNanoVector(String name) {
    if (timeNanoVector == null) {
      int vectorCount = internalStruct.size();
      timeNanoVector = addOrGet(name, MinorType.TIMENANO, TimeNanoVector.class);
      if (internalStruct.size() > vectorCount) {
        timeNanoVector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return timeNanoVector;
  }

  private IntervalDayVector intervalDayVector;

  public IntervalDayVector getIntervalDayVector() {
    return getIntervalDayVector(null);
  }

  public IntervalDayVector getIntervalDayVector(String name) {
    if (intervalDayVector == null) {
      int vectorCount = internalStruct.size();
      intervalDayVector = addOrGet(name, MinorType.INTERVALDAY, IntervalDayVector.class);
      if (internalStruct.size() > vectorCount) {
        intervalDayVector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return intervalDayVector;
  }

  private IntervalMonthDayNanoVector intervalMonthDayNanoVector;

  public IntervalMonthDayNanoVector getIntervalMonthDayNanoVector() {
    return getIntervalMonthDayNanoVector(null);
  }

  public IntervalMonthDayNanoVector getIntervalMonthDayNanoVector(String name) {
    if (intervalMonthDayNanoVector == null) {
      int vectorCount = internalStruct.size();
      intervalMonthDayNanoVector = addOrGet(name, MinorType.INTERVALMONTHDAYNANO, IntervalMonthDayNanoVector.class);
      if (internalStruct.size() > vectorCount) {
        intervalMonthDayNanoVector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return intervalMonthDayNanoVector;
  }

  private Decimal256Vector decimal256Vector;

  public Decimal256Vector getDecimal256Vector() {
    if (decimal256Vector == null) {
      throw new IllegalArgumentException("No Decimal256 present. Provide ArrowType argument to create a new vector");
    }
    return decimal256Vector;
  }
  public Decimal256Vector getDecimal256Vector(ArrowType arrowType) {
    return getDecimal256Vector(null, arrowType);
  }
  public Decimal256Vector getDecimal256Vector(String name, ArrowType arrowType) {
    if (decimal256Vector == null) {
      int vectorCount = internalStruct.size();
      decimal256Vector = addOrGet(name, MinorType.DECIMAL256, arrowType, Decimal256Vector.class);
      if (internalStruct.size() > vectorCount) {
        decimal256Vector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return decimal256Vector;
  }

  private DecimalVector decimalVector;

  public DecimalVector getDecimalVector() {
    if (decimalVector == null) {
      throw new IllegalArgumentException("No Decimal present. Provide ArrowType argument to create a new vector");
    }
    return decimalVector;
  }
  public DecimalVector getDecimalVector(ArrowType arrowType) {
    return getDecimalVector(null, arrowType);
  }
  public DecimalVector getDecimalVector(String name, ArrowType arrowType) {
    if (decimalVector == null) {
      int vectorCount = internalStruct.size();
      decimalVector = addOrGet(name, MinorType.DECIMAL, arrowType, DecimalVector.class);
      if (internalStruct.size() > vectorCount) {
        decimalVector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return decimalVector;
  }

  private FixedSizeBinaryVector fixedSizeBinaryVector;

  public FixedSizeBinaryVector getFixedSizeBinaryVector() {
    if (fixedSizeBinaryVector == null) {
      throw new IllegalArgumentException("No FixedSizeBinary present. Provide ArrowType argument to create a new vector");
    }
    return fixedSizeBinaryVector;
  }
  public FixedSizeBinaryVector getFixedSizeBinaryVector(ArrowType arrowType) {
    return getFixedSizeBinaryVector(null, arrowType);
  }
  public FixedSizeBinaryVector getFixedSizeBinaryVector(String name, ArrowType arrowType) {
    if (fixedSizeBinaryVector == null) {
      int vectorCount = internalStruct.size();
      fixedSizeBinaryVector = addOrGet(name, MinorType.FIXEDSIZEBINARY, arrowType, FixedSizeBinaryVector.class);
      if (internalStruct.size() > vectorCount) {
        fixedSizeBinaryVector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return fixedSizeBinaryVector;
  }

  private VarBinaryVector varBinaryVector;

  public VarBinaryVector getVarBinaryVector() {
    return getVarBinaryVector(null);
  }

  public VarBinaryVector getVarBinaryVector(String name) {
    if (varBinaryVector == null) {
      int vectorCount = internalStruct.size();
      varBinaryVector = addOrGet(name, MinorType.VARBINARY, VarBinaryVector.class);
      if (internalStruct.size() > vectorCount) {
        varBinaryVector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return varBinaryVector;
  }

  private VarCharVector varCharVector;

  public VarCharVector getVarCharVector() {
    return getVarCharVector(null);
  }

  public VarCharVector getVarCharVector(String name) {
    if (varCharVector == null) {
      int vectorCount = internalStruct.size();
      varCharVector = addOrGet(name, MinorType.VARCHAR, VarCharVector.class);
      if (internalStruct.size() > vectorCount) {
        varCharVector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return varCharVector;
  }

  private LargeVarCharVector largeVarCharVector;

  public LargeVarCharVector getLargeVarCharVector() {
    return getLargeVarCharVector(null);
  }

  public LargeVarCharVector getLargeVarCharVector(String name) {
    if (largeVarCharVector == null) {
      int vectorCount = internalStruct.size();
      largeVarCharVector = addOrGet(name, MinorType.LARGEVARCHAR, LargeVarCharVector.class);
      if (internalStruct.size() > vectorCount) {
        largeVarCharVector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return largeVarCharVector;
  }

  private LargeVarBinaryVector largeVarBinaryVector;

  public LargeVarBinaryVector getLargeVarBinaryVector() {
    return getLargeVarBinaryVector(null);
  }

  public LargeVarBinaryVector getLargeVarBinaryVector(String name) {
    if (largeVarBinaryVector == null) {
      int vectorCount = internalStruct.size();
      largeVarBinaryVector = addOrGet(name, MinorType.LARGEVARBINARY, LargeVarBinaryVector.class);
      if (internalStruct.size() > vectorCount) {
        largeVarBinaryVector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return largeVarBinaryVector;
  }

  private BitVector bitVector;

  public BitVector getBitVector() {
    return getBitVector(null);
  }

  public BitVector getBitVector(String name) {
    if (bitVector == null) {
      int vectorCount = internalStruct.size();
      bitVector = addOrGet(name, MinorType.BIT, BitVector.class);
      if (internalStruct.size() > vectorCount) {
        bitVector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return bitVector;
  }

  public ListVector getList() {
    if (listVector == null) {
      int vectorCount = internalStruct.size();
      listVector = addOrGet(MinorType.LIST, ListVector.class);
      if (internalStruct.size() > vectorCount) {
        listVector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return listVector;
  }

  public MapVector getMap() {
    if (mapVector == null) {
      throw new IllegalArgumentException("No map present. Provide ArrowType argument to create a new vector");
    }
    return mapVector;
  }

  public MapVector getMap(ArrowType arrowType) {
    return getMap(null, arrowType);
  }

  public MapVector getMap(String name, ArrowType arrowType) {
    if (mapVector == null) {
      int vectorCount = internalStruct.size();
      mapVector = addOrGet(name, MinorType.MAP, arrowType, MapVector.class);
      if (internalStruct.size() > vectorCount) {
        mapVector.allocateNew();
        if (callBack != null) {
          callBack.doWork();
        }
      }
    }
    return mapVector;
  }

  public int getTypeValue(int index) {
    return typeBuffer.getByte(index * TYPE_WIDTH);
  }

  @Override
  public void allocateNew() throws OutOfMemoryException {
    /* new allocation -- clear the current buffers */
    clear();
    internalStruct.allocateNew();
    try {
      allocateTypeBuffer();
    } catch (Exception e) {
      clear();
      throw e;
    }
  }

  @Override
  public boolean allocateNewSafe() {
    /* new allocation -- clear the current buffers */
    clear();
    boolean safe = internalStruct.allocateNewSafe();
    if (!safe) { return false; }
    try {
      allocateTypeBuffer();
    } catch (Exception e) {
      clear();
      return  false;
    }

    return true;
  }

  private void allocateTypeBuffer() {
    typeBuffer = allocator.buffer(typeBufferAllocationSizeInBytes);
    typeBuffer.readerIndex(0);
    typeBuffer.setZero(0, typeBuffer.capacity());
  }

  @Override
  public void reAlloc() {
    internalStruct.reAlloc();
    reallocTypeBuffer();
  }

  private void reallocTypeBuffer() {
    final long currentBufferCapacity = typeBuffer.capacity();
    long newAllocationSize = currentBufferCapacity * 2;
    if (newAllocationSize == 0) {
      if (typeBufferAllocationSizeInBytes > 0) {
        newAllocationSize = typeBufferAllocationSizeInBytes;
      } else {
        newAllocationSize = BaseValueVector.INITIAL_VALUE_ALLOCATION * TYPE_WIDTH * 2;
      }
    }
    newAllocationSize = CommonUtil.nextPowerOfTwo(newAllocationSize);
    assert newAllocationSize >= 1;

    if (newAllocationSize > BaseValueVector.MAX_ALLOCATION_SIZE) {
      throw new OversizedAllocationException("Unable to expand the buffer");
    }

    final ArrowBuf newBuf = allocator.buffer(checkedCastToInt(newAllocationSize));
    newBuf.setBytes(0, typeBuffer, 0, currentBufferCapacity);
    newBuf.setZero(currentBufferCapacity, newBuf.capacity() - currentBufferCapacity);
    typeBuffer.getReferenceManager().release(1);
    typeBuffer = newBuf;
    typeBufferAllocationSizeInBytes = (int)newAllocationSize;
  }

  @Override
  public void setInitialCapacity(int numRecords) { }

  @Override
  public int getValueCapacity() {
    return Math.min(getTypeBufferValueCapacity(), internalStruct.getValueCapacity());
  }

  @Override
  public void close() {
    clear();
  }

  @Override
  public void clear() {
    valueCount = 0;
    typeBuffer.getReferenceManager().release();
    typeBuffer = allocator.getEmpty();
    internalStruct.clear();
  }

  @Override
  public void reset() {
    valueCount = 0;
    typeBuffer.setZero(0, typeBuffer.capacity());
    internalStruct.reset();
  }

  @Override
  public Field getField() {
    List<org.apache.arrow.vector.types.pojo.Field> childFields = new ArrayList<>();
    List<FieldVector> children = internalStruct.getChildren();
    int[] typeIds = new int[children.size()];
    for (ValueVector v : children) {
      typeIds[childFields.size()] = v.getMinorType().ordinal();
      childFields.add(v.getField());
    }

    FieldType fieldType;
    if (this.fieldType == null) {
      fieldType = FieldType.nullable(new ArrowType.Union(Sparse, typeIds));
    } else {
      final UnionMode mode = ((ArrowType.Union)this.fieldType.getType()).getMode();
      fieldType = new FieldType(this.fieldType.isNullable(), new ArrowType.Union(mode, typeIds),
          this.fieldType.getDictionary(), this.fieldType.getMetadata());
    }

    return new Field(name, fieldType, childFields);
  }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    return getTransferPair(name, allocator);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator) {
    return getTransferPair(ref, allocator, null);
  }

  @Override
  public TransferPair getTransferPair(String ref, BufferAllocator allocator, CallBack callBack) {
    return new org.apache.arrow.vector.complex.UnionVector.TransferImpl(ref, allocator, callBack);
  }

  @Override
  public TransferPair getTransferPair(Field field, BufferAllocator allocator) {
    return getTransferPair(field, allocator, null);
  }

  @Override
  public TransferPair getTransferPair(Field field, BufferAllocator allocator, CallBack callBack) {
    return new org.apache.arrow.vector.complex.UnionVector.TransferImpl(field, allocator, callBack);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector target) {
    return new TransferImpl((UnionVector) target);
  }

  @Override
  public void copyFrom(int inIndex, int outIndex, ValueVector from) {
    Preconditions.checkArgument(this.getMinorType() == from.getMinorType());
    UnionVector fromCast = (UnionVector) from;
    fromCast.getReader().setPosition(inIndex);
    getWriter().setPosition(outIndex);
    ComplexCopier.copy(fromCast.reader, writer);
  }

  @Override
  public void copyFromSafe(int inIndex, int outIndex, ValueVector from) {
    copyFrom(inIndex, outIndex, from);
  }

  public FieldVector addVector(FieldVector v) {
    final String name = v.getName().isEmpty() ? fieldName(v.getMinorType()) : v.getName();
    Preconditions.checkState(internalStruct.getChild(name) == null, String.format("%s vector already exists", name));
    final FieldVector newVector = internalStruct.addOrGet(name, v.getField().getFieldType(), v.getClass());
    v.makeTransferPair(newVector).transfer();
    internalStruct.putChild(name, newVector);
    if (callBack != null) {
      callBack.doWork();
    }
    return newVector;
  }

  /**
   * Directly put a vector to internalStruct without creating a new one with same type.
   */
  public void directAddVector(FieldVector v) {
    String name = fieldName(v.getMinorType());
    Preconditions.checkState(internalStruct.getChild(name) == null, String.format("%s vector already exists", name));
    internalStruct.putChild(name, v);
    if (callBack != null) {
      callBack.doWork();
    }
  }

  private class TransferImpl implements TransferPair {
    private final TransferPair internalStructVectorTransferPair;
    private final UnionVector to;

    public TransferImpl(String name, BufferAllocator allocator, CallBack callBack) {
      to = new UnionVector(name, allocator, /* field type */ null, callBack);
      internalStructVectorTransferPair = internalStruct.makeTransferPair(to.internalStruct);
    }

    public TransferImpl(Field field, BufferAllocator allocator, CallBack callBack) {
      to = new UnionVector(field.getName(), allocator, null, callBack);
      internalStructVectorTransferPair = internalStruct.makeTransferPair(to.internalStruct);
    }

    public TransferImpl(UnionVector to) {
      this.to = to;
      internalStructVectorTransferPair = internalStruct.makeTransferPair(to.internalStruct);
    }

    @Override
    public void transfer() {
      to.clear();
      ReferenceManager refManager = typeBuffer.getReferenceManager();
      to.typeBuffer = refManager.transferOwnership(typeBuffer, to.allocator).getTransferredBuffer();
      internalStructVectorTransferPair.transfer();
      to.valueCount = valueCount;
      clear();
    }

    @Override
    public void splitAndTransfer(int startIndex, int length) {
      Preconditions.checkArgument(startIndex >= 0 && length >= 0 && startIndex + length <= valueCount,
          "Invalid parameters startIndex: %s, length: %s for valueCount: %s", startIndex, length, valueCount);
      to.clear();

      internalStructVectorTransferPair.splitAndTransfer(startIndex, length);
      final int startPoint = startIndex * TYPE_WIDTH;
      final int sliceLength = length * TYPE_WIDTH;
      final ArrowBuf slicedBuffer = typeBuffer.slice(startPoint, sliceLength);
      final ReferenceManager refManager = slicedBuffer.getReferenceManager();
      to.typeBuffer = refManager.transferOwnership(slicedBuffer, to.allocator).getTransferredBuffer();
      to.setValueCount(length);
    }

    @Override
    public ValueVector getTo() {
      return to;
    }

    @Override
    public void copyValueSafe(int from, int to) {
      this.to.copyFrom(from, to, UnionVector.this);
    }
  }

  @Override
  public FieldReader getReader() {
    if (reader == null) {
      reader = new UnionReader(this);
    }
    return reader;
  }

  public FieldWriter getWriter() {
    if (writer == null) {
      writer = new UnionWriter(this);
    }
    return writer;
  }

  @Override
  public int getBufferSize() {
    if (valueCount == 0) { return 0; }

    return (valueCount * TYPE_WIDTH) + internalStruct.getBufferSize();
  }

  @Override
  public int getBufferSizeFor(final int valueCount) {
    if (valueCount == 0) {
      return 0;
    }

    long bufferSize = 0;
    for (final ValueVector v : (Iterable<ValueVector>) this) {
      bufferSize += v.getBufferSizeFor(valueCount);
    }

    return (int) bufferSize + (valueCount * TYPE_WIDTH);
  }

  @Override
  public ArrowBuf[] getBuffers(boolean clear) {
    List<ArrowBuf> list = new java.util.ArrayList<>();
    setReaderAndWriterIndex();
    if (getBufferSize() != 0) {
      list.add(typeBuffer);
      list.addAll(java.util.Arrays.asList(internalStruct.getBuffers(clear)));
    }
    if (clear) {
      valueCount = 0;
      typeBuffer.getReferenceManager().retain();
      typeBuffer.getReferenceManager().release();
      typeBuffer = allocator.getEmpty();
    }
    return list.toArray(new ArrowBuf[list.size()]);
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return internalStruct.iterator();
  }

  public ValueVector getVector(int index) {
    return getVector(index, null);
  }

  public ValueVector getVector(int index, ArrowType arrowType) {
    int type = typeBuffer.getByte(index * TYPE_WIDTH);
    return getVectorByType(type, arrowType);
  }

  public ValueVector getVectorByType(int typeId) {
    return getVectorByType(typeId, null);
  }

    public ValueVector getVectorByType(int typeId, ArrowType arrowType) {
      Field type = typeIds[typeId];
      Types.MinorType minorType;
      String name = null;
      if (type == null) {
        minorType = Types.MinorType.values()[typeId];
      } else {
        minorType = Types.getMinorTypeForArrowType(type.getType());
        name = type.getName();
      }
      switch (minorType) {
        case NULL:
          return null;
        case TINYINT:
        return getTinyIntVector(name);
        case UINT1:
        return getUInt1Vector(name);
        case UINT2:
        return getUInt2Vector(name);
        case SMALLINT:
        return getSmallIntVector(name);
        case INT:
        return getIntVector(name);
        case UINT4:
        return getUInt4Vector(name);
        case FLOAT4:
        return getFloat4Vector(name);
        case DATEDAY:
        return getDateDayVector(name);
        case INTERVALYEAR:
        return getIntervalYearVector(name);
        case TIMESEC:
        return getTimeSecVector(name);
        case TIMEMILLI:
        return getTimeMilliVector(name);
        case BIGINT:
        return getBigIntVector(name);
        case UINT8:
        return getUInt8Vector(name);
        case FLOAT8:
        return getFloat8Vector(name);
        case DATEMILLI:
        return getDateMilliVector(name);
        case DURATION:
        return getDurationVector(name, arrowType);
        case TIMESTAMPSEC:
        return getTimeStampSecVector(name);
        case TIMESTAMPMILLI:
        return getTimeStampMilliVector(name);
        case TIMESTAMPMICRO:
        return getTimeStampMicroVector(name);
        case TIMESTAMPNANO:
        return getTimeStampNanoVector(name);
        case TIMESTAMPSECTZ:
        return getTimeStampSecTZVector(name, arrowType);
        case TIMESTAMPMILLITZ:
        return getTimeStampMilliTZVector(name, arrowType);
        case TIMESTAMPMICROTZ:
        return getTimeStampMicroTZVector(name, arrowType);
        case TIMESTAMPNANOTZ:
        return getTimeStampNanoTZVector(name, arrowType);
        case TIMEMICRO:
        return getTimeMicroVector(name);
        case TIMENANO:
        return getTimeNanoVector(name);
        case INTERVALDAY:
        return getIntervalDayVector(name);
        case INTERVALMONTHDAYNANO:
        return getIntervalMonthDayNanoVector(name);
        case DECIMAL256:
        return getDecimal256Vector(name, arrowType);
        case DECIMAL:
        return getDecimalVector(name, arrowType);
        case FIXEDSIZEBINARY:
        return getFixedSizeBinaryVector(name, arrowType);
        case VARBINARY:
        return getVarBinaryVector(name);
        case VARCHAR:
        return getVarCharVector(name);
        case LARGEVARCHAR:
        return getLargeVarCharVector(name);
        case LARGEVARBINARY:
        return getLargeVarBinaryVector(name);
        case BIT:
        return getBitVector(name);
        case STRUCT:
          return getStruct();
        case LIST:
          return getList();
        case MAP:
          return getMap(name, arrowType);
        default:
          throw new UnsupportedOperationException("Cannot support type: " + MinorType.values()[typeId]);
      }
    }

    public Object getObject(int index) {
      ValueVector vector = getVector(index);
      if (vector != null) {
        return vector.isNull(index) ? null : vector.getObject(index);
      }
      return null;
    }

    public byte[] get(int index) {
      return null;
    }

    public void get(int index, ComplexHolder holder) {
    }

    public void get(int index, UnionHolder holder) {
      FieldReader reader = new UnionReader(UnionVector.this);
      reader.setPosition(index);
      holder.reader = reader;
    }

    public int getValueCount() {
      return valueCount;
    }

    /**
     * IMPORTANT: Union types always return non null as there is no validity buffer.
     *
     * To check validity correctly you must check the underlying vector.
     */
    public boolean isNull(int index) {
      return false;
    }

    @Override
    public int getNullCount() {
      return 0;
    }

    public int isSet(int index) {
      return isNull(index) ? 0 : 1;
    }

    UnionWriter writer;

    public void setValueCount(int valueCount) {
      this.valueCount = valueCount;
      while (valueCount > getTypeBufferValueCapacity()) {
        reallocTypeBuffer();
      }
      internalStruct.setValueCount(valueCount);
    }

    public void setSafe(int index, UnionHolder holder) {
      setSafe(index, holder, null);
    }

    public void setSafe(int index, UnionHolder holder, ArrowType arrowType) {
      FieldReader reader = holder.reader;
      if (writer == null) {
        writer = new UnionWriter(UnionVector.this);
      }
      writer.setPosition(index);
      MinorType type = reader.getMinorType();
      switch (type) {
      case TINYINT:
        NullableTinyIntHolder tinyIntHolder = new NullableTinyIntHolder();
        reader.read(tinyIntHolder);
        setSafe(index, tinyIntHolder);
        break;
      case UINT1:
        NullableUInt1Holder uInt1Holder = new NullableUInt1Holder();
        reader.read(uInt1Holder);
        setSafe(index, uInt1Holder);
        break;
      case UINT2:
        NullableUInt2Holder uInt2Holder = new NullableUInt2Holder();
        reader.read(uInt2Holder);
        setSafe(index, uInt2Holder);
        break;
      case SMALLINT:
        NullableSmallIntHolder smallIntHolder = new NullableSmallIntHolder();
        reader.read(smallIntHolder);
        setSafe(index, smallIntHolder);
        break;
      case INT:
        NullableIntHolder intHolder = new NullableIntHolder();
        reader.read(intHolder);
        setSafe(index, intHolder);
        break;
      case UINT4:
        NullableUInt4Holder uInt4Holder = new NullableUInt4Holder();
        reader.read(uInt4Holder);
        setSafe(index, uInt4Holder);
        break;
      case FLOAT4:
        NullableFloat4Holder float4Holder = new NullableFloat4Holder();
        reader.read(float4Holder);
        setSafe(index, float4Holder);
        break;
      case DATEDAY:
        NullableDateDayHolder dateDayHolder = new NullableDateDayHolder();
        reader.read(dateDayHolder);
        setSafe(index, dateDayHolder);
        break;
      case INTERVALYEAR:
        NullableIntervalYearHolder intervalYearHolder = new NullableIntervalYearHolder();
        reader.read(intervalYearHolder);
        setSafe(index, intervalYearHolder);
        break;
      case TIMESEC:
        NullableTimeSecHolder timeSecHolder = new NullableTimeSecHolder();
        reader.read(timeSecHolder);
        setSafe(index, timeSecHolder);
        break;
      case TIMEMILLI:
        NullableTimeMilliHolder timeMilliHolder = new NullableTimeMilliHolder();
        reader.read(timeMilliHolder);
        setSafe(index, timeMilliHolder);
        break;
      case BIGINT:
        NullableBigIntHolder bigIntHolder = new NullableBigIntHolder();
        reader.read(bigIntHolder);
        setSafe(index, bigIntHolder);
        break;
      case UINT8:
        NullableUInt8Holder uInt8Holder = new NullableUInt8Holder();
        reader.read(uInt8Holder);
        setSafe(index, uInt8Holder);
        break;
      case FLOAT8:
        NullableFloat8Holder float8Holder = new NullableFloat8Holder();
        reader.read(float8Holder);
        setSafe(index, float8Holder);
        break;
      case DATEMILLI:
        NullableDateMilliHolder dateMilliHolder = new NullableDateMilliHolder();
        reader.read(dateMilliHolder);
        setSafe(index, dateMilliHolder);
        break;
      case DURATION:
        NullableDurationHolder durationHolder = new NullableDurationHolder();
        reader.read(durationHolder);
        setSafe(index, durationHolder, arrowType);
        break;
      case TIMESTAMPSEC:
        NullableTimeStampSecHolder timeStampSecHolder = new NullableTimeStampSecHolder();
        reader.read(timeStampSecHolder);
        setSafe(index, timeStampSecHolder);
        break;
      case TIMESTAMPMILLI:
        NullableTimeStampMilliHolder timeStampMilliHolder = new NullableTimeStampMilliHolder();
        reader.read(timeStampMilliHolder);
        setSafe(index, timeStampMilliHolder);
        break;
      case TIMESTAMPMICRO:
        NullableTimeStampMicroHolder timeStampMicroHolder = new NullableTimeStampMicroHolder();
        reader.read(timeStampMicroHolder);
        setSafe(index, timeStampMicroHolder);
        break;
      case TIMESTAMPNANO:
        NullableTimeStampNanoHolder timeStampNanoHolder = new NullableTimeStampNanoHolder();
        reader.read(timeStampNanoHolder);
        setSafe(index, timeStampNanoHolder);
        break;
      case TIMESTAMPSECTZ:
        NullableTimeStampSecTZHolder timeStampSecTZHolder = new NullableTimeStampSecTZHolder();
        reader.read(timeStampSecTZHolder);
        setSafe(index, timeStampSecTZHolder, arrowType);
        break;
      case TIMESTAMPMILLITZ:
        NullableTimeStampMilliTZHolder timeStampMilliTZHolder = new NullableTimeStampMilliTZHolder();
        reader.read(timeStampMilliTZHolder);
        setSafe(index, timeStampMilliTZHolder, arrowType);
        break;
      case TIMESTAMPMICROTZ:
        NullableTimeStampMicroTZHolder timeStampMicroTZHolder = new NullableTimeStampMicroTZHolder();
        reader.read(timeStampMicroTZHolder);
        setSafe(index, timeStampMicroTZHolder, arrowType);
        break;
      case TIMESTAMPNANOTZ:
        NullableTimeStampNanoTZHolder timeStampNanoTZHolder = new NullableTimeStampNanoTZHolder();
        reader.read(timeStampNanoTZHolder);
        setSafe(index, timeStampNanoTZHolder, arrowType);
        break;
      case TIMEMICRO:
        NullableTimeMicroHolder timeMicroHolder = new NullableTimeMicroHolder();
        reader.read(timeMicroHolder);
        setSafe(index, timeMicroHolder);
        break;
      case TIMENANO:
        NullableTimeNanoHolder timeNanoHolder = new NullableTimeNanoHolder();
        reader.read(timeNanoHolder);
        setSafe(index, timeNanoHolder);
        break;
      case INTERVALDAY:
        NullableIntervalDayHolder intervalDayHolder = new NullableIntervalDayHolder();
        reader.read(intervalDayHolder);
        setSafe(index, intervalDayHolder);
        break;
      case INTERVALMONTHDAYNANO:
        NullableIntervalMonthDayNanoHolder intervalMonthDayNanoHolder = new NullableIntervalMonthDayNanoHolder();
        reader.read(intervalMonthDayNanoHolder);
        setSafe(index, intervalMonthDayNanoHolder);
        break;
      case DECIMAL256:
        NullableDecimal256Holder decimal256Holder = new NullableDecimal256Holder();
        reader.read(decimal256Holder);
        setSafe(index, decimal256Holder, arrowType);
        break;
      case DECIMAL:
        NullableDecimalHolder decimalHolder = new NullableDecimalHolder();
        reader.read(decimalHolder);
        setSafe(index, decimalHolder, arrowType);
        break;
      case FIXEDSIZEBINARY:
        NullableFixedSizeBinaryHolder fixedSizeBinaryHolder = new NullableFixedSizeBinaryHolder();
        reader.read(fixedSizeBinaryHolder);
        setSafe(index, fixedSizeBinaryHolder, arrowType);
        break;
      case VARBINARY:
        NullableVarBinaryHolder varBinaryHolder = new NullableVarBinaryHolder();
        reader.read(varBinaryHolder);
        setSafe(index, varBinaryHolder);
        break;
      case VARCHAR:
        NullableVarCharHolder varCharHolder = new NullableVarCharHolder();
        reader.read(varCharHolder);
        setSafe(index, varCharHolder);
        break;
      case LARGEVARCHAR:
        NullableLargeVarCharHolder largeVarCharHolder = new NullableLargeVarCharHolder();
        reader.read(largeVarCharHolder);
        setSafe(index, largeVarCharHolder);
        break;
      case LARGEVARBINARY:
        NullableLargeVarBinaryHolder largeVarBinaryHolder = new NullableLargeVarBinaryHolder();
        reader.read(largeVarBinaryHolder);
        setSafe(index, largeVarBinaryHolder);
        break;
      case BIT:
        NullableBitHolder bitHolder = new NullableBitHolder();
        reader.read(bitHolder);
        setSafe(index, bitHolder);
        break;
      case STRUCT: {
        ComplexCopier.copy(reader, writer);
        break;
      }
      case LIST: {
        ComplexCopier.copy(reader, writer);
        break;
      }
      default:
        throw new UnsupportedOperationException();
      }
    }

    public void setSafe(int index, NullableTinyIntHolder holder) {
      setType(index, MinorType.TINYINT);
      getTinyIntVector(null).setSafe(index, holder);
    }
    public void setSafe(int index, NullableUInt1Holder holder) {
      setType(index, MinorType.UINT1);
      getUInt1Vector(null).setSafe(index, holder);
    }
    public void setSafe(int index, NullableUInt2Holder holder) {
      setType(index, MinorType.UINT2);
      getUInt2Vector(null).setSafe(index, holder);
    }
    public void setSafe(int index, NullableSmallIntHolder holder) {
      setType(index, MinorType.SMALLINT);
      getSmallIntVector(null).setSafe(index, holder);
    }
    public void setSafe(int index, NullableIntHolder holder) {
      setType(index, MinorType.INT);
      getIntVector(null).setSafe(index, holder);
    }
    public void setSafe(int index, NullableUInt4Holder holder) {
      setType(index, MinorType.UINT4);
      getUInt4Vector(null).setSafe(index, holder);
    }
    public void setSafe(int index, NullableFloat4Holder holder) {
      setType(index, MinorType.FLOAT4);
      getFloat4Vector(null).setSafe(index, holder);
    }
    public void setSafe(int index, NullableDateDayHolder holder) {
      setType(index, MinorType.DATEDAY);
      getDateDayVector(null).setSafe(index, holder);
    }
    public void setSafe(int index, NullableIntervalYearHolder holder) {
      setType(index, MinorType.INTERVALYEAR);
      getIntervalYearVector(null).setSafe(index, holder);
    }
    public void setSafe(int index, NullableTimeSecHolder holder) {
      setType(index, MinorType.TIMESEC);
      getTimeSecVector(null).setSafe(index, holder);
    }
    public void setSafe(int index, NullableTimeMilliHolder holder) {
      setType(index, MinorType.TIMEMILLI);
      getTimeMilliVector(null).setSafe(index, holder);
    }
    public void setSafe(int index, NullableBigIntHolder holder) {
      setType(index, MinorType.BIGINT);
      getBigIntVector(null).setSafe(index, holder);
    }
    public void setSafe(int index, NullableUInt8Holder holder) {
      setType(index, MinorType.UINT8);
      getUInt8Vector(null).setSafe(index, holder);
    }
    public void setSafe(int index, NullableFloat8Holder holder) {
      setType(index, MinorType.FLOAT8);
      getFloat8Vector(null).setSafe(index, holder);
    }
    public void setSafe(int index, NullableDateMilliHolder holder) {
      setType(index, MinorType.DATEMILLI);
      getDateMilliVector(null).setSafe(index, holder);
    }
    public void setSafe(int index, NullableDurationHolder holder, ArrowType arrowType) {
      setType(index, MinorType.DURATION);
      getDurationVector(null, arrowType).setSafe(index, holder);
    }
    public void setSafe(int index, NullableTimeStampSecHolder holder) {
      setType(index, MinorType.TIMESTAMPSEC);
      getTimeStampSecVector(null).setSafe(index, holder);
    }
    public void setSafe(int index, NullableTimeStampMilliHolder holder) {
      setType(index, MinorType.TIMESTAMPMILLI);
      getTimeStampMilliVector(null).setSafe(index, holder);
    }
    public void setSafe(int index, NullableTimeStampMicroHolder holder) {
      setType(index, MinorType.TIMESTAMPMICRO);
      getTimeStampMicroVector(null).setSafe(index, holder);
    }
    public void setSafe(int index, NullableTimeStampNanoHolder holder) {
      setType(index, MinorType.TIMESTAMPNANO);
      getTimeStampNanoVector(null).setSafe(index, holder);
    }
    public void setSafe(int index, NullableTimeStampSecTZHolder holder, ArrowType arrowType) {
      setType(index, MinorType.TIMESTAMPSECTZ);
      getTimeStampSecTZVector(null, arrowType).setSafe(index, holder);
    }
    public void setSafe(int index, NullableTimeStampMilliTZHolder holder, ArrowType arrowType) {
      setType(index, MinorType.TIMESTAMPMILLITZ);
      getTimeStampMilliTZVector(null, arrowType).setSafe(index, holder);
    }
    public void setSafe(int index, NullableTimeStampMicroTZHolder holder, ArrowType arrowType) {
      setType(index, MinorType.TIMESTAMPMICROTZ);
      getTimeStampMicroTZVector(null, arrowType).setSafe(index, holder);
    }
    public void setSafe(int index, NullableTimeStampNanoTZHolder holder, ArrowType arrowType) {
      setType(index, MinorType.TIMESTAMPNANOTZ);
      getTimeStampNanoTZVector(null, arrowType).setSafe(index, holder);
    }
    public void setSafe(int index, NullableTimeMicroHolder holder) {
      setType(index, MinorType.TIMEMICRO);
      getTimeMicroVector(null).setSafe(index, holder);
    }
    public void setSafe(int index, NullableTimeNanoHolder holder) {
      setType(index, MinorType.TIMENANO);
      getTimeNanoVector(null).setSafe(index, holder);
    }
    public void setSafe(int index, NullableIntervalDayHolder holder) {
      setType(index, MinorType.INTERVALDAY);
      getIntervalDayVector(null).setSafe(index, holder);
    }
    public void setSafe(int index, NullableIntervalMonthDayNanoHolder holder) {
      setType(index, MinorType.INTERVALMONTHDAYNANO);
      getIntervalMonthDayNanoVector(null).setSafe(index, holder);
    }
    public void setSafe(int index, NullableDecimal256Holder holder, ArrowType arrowType) {
      setType(index, MinorType.DECIMAL256);
      getDecimal256Vector(null, arrowType).setSafe(index, holder);
    }
    public void setSafe(int index, NullableDecimalHolder holder, ArrowType arrowType) {
      setType(index, MinorType.DECIMAL);
      getDecimalVector(null, arrowType).setSafe(index, holder);
    }
    public void setSafe(int index, NullableFixedSizeBinaryHolder holder, ArrowType arrowType) {
      setType(index, MinorType.FIXEDSIZEBINARY);
      getFixedSizeBinaryVector(null, arrowType).setSafe(index, holder);
    }
    public void setSafe(int index, NullableVarBinaryHolder holder) {
      setType(index, MinorType.VARBINARY);
      getVarBinaryVector(null).setSafe(index, holder);
    }
    public void setSafe(int index, NullableVarCharHolder holder) {
      setType(index, MinorType.VARCHAR);
      getVarCharVector(null).setSafe(index, holder);
    }
    public void setSafe(int index, NullableLargeVarCharHolder holder) {
      setType(index, MinorType.LARGEVARCHAR);
      getLargeVarCharVector(null).setSafe(index, holder);
    }
    public void setSafe(int index, NullableLargeVarBinaryHolder holder) {
      setType(index, MinorType.LARGEVARBINARY);
      getLargeVarBinaryVector(null).setSafe(index, holder);
    }
    public void setSafe(int index, NullableBitHolder holder) {
      setType(index, MinorType.BIT);
      getBitVector(null).setSafe(index, holder);
    }

    public void setType(int index, MinorType type) {
      setTypeId(index, (byte) type.ordinal());
    }

    public void setTypeId(int index, byte typeId) {
      while (index >= getTypeBufferValueCapacity()) {
        reallocTypeBuffer();
      }
      typeBuffer.setByte(index * TYPE_WIDTH , typeId);
    }

    private int getTypeBufferValueCapacity() {
      return capAtMaxInt(typeBuffer.capacity() / TYPE_WIDTH);
    }

    @Override
    public int hashCode(int index) {
      return hashCode(index, null);
    }

    @Override
    public int hashCode(int index, ArrowBufHasher hasher) {
      ValueVector vec = getVector(index);
      if (vec == null) {
        return ArrowBufPointer.NULL_HASH_CODE;
      }
      return vec.hashCode(index, hasher);
    }

    @Override
    public <OUT, IN> OUT accept(VectorVisitor<OUT, IN> visitor, IN value) {
      return visitor.visit(this, value);
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public String toString() {
      return ValueVectorUtility.getToString(this, 0, getValueCount());
    }

    @Override
    public <T extends FieldVector> T addOrGet(String name, FieldType fieldType, Class<T> clazz) {
      return internalStruct.addOrGet(name, fieldType, clazz);
    }

    @Override
    public <T extends FieldVector> T getChild(String name, Class<T> clazz) {
      return internalStruct.getChild(name, clazz);
    }

    @Override
    public VectorWithOrdinal getChildVectorWithOrdinal(String name) {
      return internalStruct.getChildVectorWithOrdinal(name);
    }

    @Override
    public int size() {
      return internalStruct.size();
    }

    @Override
    public void setInitialCapacity(int valueCount, double density) {
      for (final ValueVector vector : internalStruct) {
        if (vector instanceof DensityAwareVector) {
          ((DensityAwareVector) vector).setInitialCapacity(valueCount, density);
        } else {
          vector.setInitialCapacity(valueCount);
        }
      }
    }

  /**
   * Set the element at the given index to null. For UnionVector, it throws an UnsupportedOperationException
   * as nulls are not supported at the top level and isNull() always returns false.
   *
   * @param index position of element
   * @throws UnsupportedOperationException whenever invoked
   */
  @Override
  public void setNull(int index) {
    throw new UnsupportedOperationException("The method setNull() is not supported on UnionVector.");
  }
}
