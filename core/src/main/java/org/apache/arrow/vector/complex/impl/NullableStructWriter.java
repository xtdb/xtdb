
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

package org.apache.arrow.vector.complex.impl;


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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.HashMap;

import org.apache.arrow.vector.holders.RepeatedStructHolder;
import org.apache.arrow.vector.AllocationHelper;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.complex.writer.FieldWriter;


/*
 * This class is generated using FreeMarker and the StructWriters.java template.
 */
@SuppressWarnings("unused")
public class NullableStructWriter extends AbstractFieldWriter {

  protected final StructVector container;
  private int initialCapacity;
  private final Map<String, FieldWriter> fields = new HashMap<>();
  public NullableStructWriter(StructVector container) {
    this.container = container;
    this.initialCapacity = 0;
    for (Field child : container.getField().getChildren()) {
      MinorType minorType = Types.getMinorTypeForArrowType(child.getType());
      addVectorAsNullable = child.isNullable();
      switch (minorType) {
      case STRUCT:
        struct(child.getName());
        break;
      case LIST:
        list(child.getName());
        break;
      case LISTVIEW:
        listView(child.getName());
        break;
      case MAP: {
        ArrowType.Map arrowType = (ArrowType.Map) child.getType();
        map(child.getName(), arrowType.getKeysSorted());
        break;
      }
      case DENSEUNION: {
        FieldType fieldType = new FieldType(addVectorAsNullable, MinorType.DENSEUNION.getType(), null, null);
        DenseUnionWriter writer = new DenseUnionWriter(container.addOrGet(child.getName(), fieldType, DenseUnionVector.class), getNullableStructWriterFactory());
        fields.put(handleCase(child.getName()), writer);
        break;
      }
      case EXTENSIONTYPE:
        extension(child.getName(), child.getType());
        break;
      case UNION:
        FieldType fieldType = new FieldType(addVectorAsNullable, MinorType.UNION.getType(), null, null);
        UnionWriter writer = new UnionWriter(container.addOrGet(child.getName(), fieldType, UnionVector.class), getNullableStructWriterFactory());
        fields.put(handleCase(child.getName()), writer);
        break;
      case TINYINT: {
        tinyInt(child.getName());
        break;
      }
      case UINT1: {
        uInt1(child.getName());
        break;
      }
      case UINT2: {
        uInt2(child.getName());
        break;
      }
      case SMALLINT: {
        smallInt(child.getName());
        break;
      }
      case FLOAT2: {
        float2(child.getName());
        break;
      }
      case INT: {
        integer(child.getName());
        break;
      }
      case UINT4: {
        uInt4(child.getName());
        break;
      }
      case FLOAT4: {
        float4(child.getName());
        break;
      }
      case DATEDAY: {
        dateDay(child.getName());
        break;
      }
      case INTERVALYEAR: {
        intervalYear(child.getName());
        break;
      }
      case TIMESEC: {
        timeSec(child.getName());
        break;
      }
      case TIMEMILLI: {
        timeMilli(child.getName());
        break;
      }
      case BIGINT: {
        bigInt(child.getName());
        break;
      }
      case UINT8: {
        uInt8(child.getName());
        break;
      }
      case FLOAT8: {
        float8(child.getName());
        break;
      }
      case DATEMILLI: {
        dateMilli(child.getName());
        break;
      }
      case DURATION: {
        org.apache.arrow.vector.types.pojo.ArrowType.Duration arrowType = (org.apache.arrow.vector.types.pojo.ArrowType.Duration)child.getType();
        duration(child.getName(), arrowType.getUnit());
        break;
      }
      case TIMESTAMPSEC: {
        timeStampSec(child.getName());
        break;
      }
      case TIMESTAMPMILLI: {
        timeStampMilli(child.getName());
        break;
      }
      case TIMESTAMPMICRO: {
        timeStampMicro(child.getName());
        break;
      }
      case TIMESTAMPNANO: {
        timeStampNano(child.getName());
        break;
      }
      case TIMESTAMPSECTZ: {
        org.apache.arrow.vector.types.pojo.ArrowType.Timestamp arrowType = (org.apache.arrow.vector.types.pojo.ArrowType.Timestamp)child.getType();
        timeStampSecTZ(child.getName(), arrowType.getTimezone());
        break;
      }
      case TIMESTAMPMILLITZ: {
        org.apache.arrow.vector.types.pojo.ArrowType.Timestamp arrowType = (org.apache.arrow.vector.types.pojo.ArrowType.Timestamp)child.getType();
        timeStampMilliTZ(child.getName(), arrowType.getTimezone());
        break;
      }
      case TIMESTAMPMICROTZ: {
        org.apache.arrow.vector.types.pojo.ArrowType.Timestamp arrowType = (org.apache.arrow.vector.types.pojo.ArrowType.Timestamp)child.getType();
        timeStampMicroTZ(child.getName(), arrowType.getTimezone());
        break;
      }
      case TIMESTAMPNANOTZ: {
        org.apache.arrow.vector.types.pojo.ArrowType.Timestamp arrowType = (org.apache.arrow.vector.types.pojo.ArrowType.Timestamp)child.getType();
        timeStampNanoTZ(child.getName(), arrowType.getTimezone());
        break;
      }
      case TIMEMICRO: {
        timeMicro(child.getName());
        break;
      }
      case TIMENANO: {
        timeNano(child.getName());
        break;
      }
      case INTERVALDAY: {
        intervalDay(child.getName());
        break;
      }
      case INTERVALMONTHDAYNANO: {
        intervalMonthDayNano(child.getName());
        break;
      }
      case DECIMAL256: {
        org.apache.arrow.vector.types.pojo.ArrowType.Decimal arrowType = (org.apache.arrow.vector.types.pojo.ArrowType.Decimal)child.getType();
        decimal256(child.getName(), arrowType.getScale(), arrowType.getPrecision());
        break;
      }
      case DECIMAL: {
        org.apache.arrow.vector.types.pojo.ArrowType.Decimal arrowType = (org.apache.arrow.vector.types.pojo.ArrowType.Decimal)child.getType();
        decimal(child.getName(), arrowType.getScale(), arrowType.getPrecision());
        break;
      }
      case FIXEDSIZEBINARY: {
        org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeBinary arrowType = (org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeBinary)child.getType();
        fixedSizeBinary(child.getName(), arrowType.getByteWidth());
        break;
      }
      case VARBINARY: {
        varBinary(child.getName());
        break;
      }
      case VARCHAR: {
        varChar(child.getName());
        break;
      }
      case VIEWVARBINARY: {
        viewVarBinary(child.getName());
        break;
      }
      case VIEWVARCHAR: {
        viewVarChar(child.getName());
        break;
      }
      case LARGEVARCHAR: {
        largeVarChar(child.getName());
        break;
      }
      case LARGEVARBINARY: {
        largeVarBinary(child.getName());
        break;
      }
      case BIT: {
        bit(child.getName());
        break;
      }
        default:
          throw new UnsupportedOperationException("Unknown type: " + minorType);
      }
    }
  }

  protected String handleCase(final String input) {
    return input.toLowerCase();
  }

  protected NullableStructWriterFactory getNullableStructWriterFactory() {
    return NullableStructWriterFactory.getNullableStructWriterFactoryInstance();
  }

  @Override
  public int getValueCapacity() {
    return container.getValueCapacity();
  }

  public void setInitialCapacity(int initialCapacity) {
    this.initialCapacity = initialCapacity;
    container.setInitialCapacity(initialCapacity);
  }

  @Override
  public boolean isEmptyStruct() {
    return 0 == container.size();
  }

  @Override
  public Field getField() {
      return container.getField();
  }

  @Override
  public StructWriter struct(String name) {
    String finalName = handleCase(name);
    FieldWriter writer = fields.get(finalName);
    if(writer == null){
      int vectorCount=container.size();
      FieldType fieldType = new FieldType(addVectorAsNullable, MinorType.STRUCT.getType(), null, null);
      StructVector vector = container.addOrGet(name, fieldType, StructVector.class);
      writer = new PromotableWriter(vector, container, getNullableStructWriterFactory());
      if(vectorCount != container.size()) {
        writer.allocate();
      }
      writer.setPosition(idx());
      fields.put(finalName, writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.STRUCT);
      }
    }
    return writer;
  }

  @Override
  public ExtensionWriter extension(String name, ArrowType arrowType) {
    String finalName = handleCase(name);
    FieldWriter writer = fields.get(finalName);
    if(writer == null){
      int vectorCount=container.size();
      FieldType fieldType = new FieldType(addVectorAsNullable, arrowType, null, null);
      ExtensionTypeVector vector = container.addOrGet(name, fieldType, ExtensionTypeVector.class);
      writer = new PromotableWriter(vector, container, getNullableStructWriterFactory());
      if(vectorCount != container.size()) {
        writer.allocate();
      }
      writer.setPosition(idx());
      fields.put(finalName, writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.EXTENSIONTYPE, arrowType);
      }
    }
    return (ExtensionWriter) writer;
  }

  @Override
  public void close() throws Exception {
    clear();
    container.close();
  }

  @Override
  public void allocate() {
    container.allocateNew();
    for(final FieldWriter w : fields.values()) {
      w.allocate();
    }
  }

  @Override
  public void clear() {
    container.clear();
    for(final FieldWriter w : fields.values()) {
      w.clear();
    }
  }

  @Override
  public ListWriter list(String name) {
    String finalName = handleCase(name);
    FieldWriter writer = fields.get(finalName);
    int vectorCount = container.size();
    if(writer == null) {
      FieldType fieldType = new FieldType(addVectorAsNullable, MinorType.LIST.getType(), null, null);
      writer = new PromotableWriter(container.addOrGet(name, fieldType, ListVector.class), container, getNullableStructWriterFactory());
      if (container.size() > vectorCount) {
        writer.allocate();
      }
      writer.setPosition(idx());
      fields.put(finalName, writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.LIST);
      }
    }
    return writer;
  }

  @Override
  public ListWriter listView(String name) {
    String finalName = handleCase(name);
    FieldWriter writer = fields.get(finalName);
    int vectorCount = container.size();
    if(writer == null) {
      FieldType fieldType = new FieldType(addVectorAsNullable, MinorType.LISTVIEW.getType(), null, null);
      writer = new PromotableViewWriter(container.addOrGet(name, fieldType, ListViewVector.class), container, getNullableStructWriterFactory());
      if (container.size() > vectorCount) {
        writer.allocate();
      }
      writer.setPosition(idx());
      fields.put(finalName, writer);
    } else {
      if (writer instanceof PromotableViewWriter) {
        // ensure writers are initialized
        ((PromotableViewWriter) writer).getWriter(MinorType.LISTVIEW);
      } else {
        writer = ((PromotableWriter) writer).toViewWriter();
        ((PromotableViewWriter) writer).getWriter(MinorType.LISTVIEW);
      }
    }
    return writer;
  }

  @Override
  public MapWriter map(String name) {
    return map(name, false);
  }

  @Override
  public MapWriter map(String name, boolean keysSorted) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      MapVector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            new ArrowType.Map(keysSorted)
          ,null, null),
          MapVector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.MAP, new ArrowType.Map(keysSorted));
      }
    }
    return writer;
  }

  public void setValueCount(int count) {
    container.setValueCount(count);
  }

  @Override
  public void setPosition(int index) {
    super.setPosition(index);
    for(final FieldWriter w: fields.values()) {
      w.setPosition(index);
    }
  }

  @Override
  public void writeNull() {
    container.setNull(idx());
    setValueCount(idx()+1);
    super.setPosition(idx()+1);
  }

  @Override
  public void start() {
    container.setIndexDefined(idx());
  }

  @Override
  public void end() {
    setPosition(idx()+1);
  }


  @Override
  public TinyIntWriter tinyInt(String name) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      TinyIntVector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            MinorType.TINYINT.getType()
          ,null, null),
          TinyIntVector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.TINYINT);
      }
    }
    return writer;
  }


  @Override
  public UInt1Writer uInt1(String name) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      UInt1Vector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            MinorType.UINT1.getType()
          ,null, null),
          UInt1Vector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.UINT1);
      }
    }
    return writer;
  }


  @Override
  public UInt2Writer uInt2(String name) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      UInt2Vector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            MinorType.UINT2.getType()
          ,null, null),
          UInt2Vector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.UINT2);
      }
    }
    return writer;
  }


  @Override
  public SmallIntWriter smallInt(String name) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      SmallIntVector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            MinorType.SMALLINT.getType()
          ,null, null),
          SmallIntVector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.SMALLINT);
      }
    }
    return writer;
  }


  @Override
  public Float2Writer float2(String name) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      Float2Vector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            MinorType.FLOAT2.getType()
          ,null, null),
          Float2Vector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.FLOAT2);
      }
    }
    return writer;
  }


  @Override
  public IntWriter integer(String name) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      IntVector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            MinorType.INT.getType()
          ,null, null),
          IntVector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.INT);
      }
    }
    return writer;
  }


  @Override
  public UInt4Writer uInt4(String name) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      UInt4Vector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            MinorType.UINT4.getType()
          ,null, null),
          UInt4Vector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.UINT4);
      }
    }
    return writer;
  }


  @Override
  public Float4Writer float4(String name) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      Float4Vector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            MinorType.FLOAT4.getType()
          ,null, null),
          Float4Vector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.FLOAT4);
      }
    }
    return writer;
  }


  @Override
  public DateDayWriter dateDay(String name) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      DateDayVector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            MinorType.DATEDAY.getType()
          ,null, null),
          DateDayVector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.DATEDAY);
      }
    }
    return writer;
  }


  @Override
  public IntervalYearWriter intervalYear(String name) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      IntervalYearVector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            MinorType.INTERVALYEAR.getType()
          ,null, null),
          IntervalYearVector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.INTERVALYEAR);
      }
    }
    return writer;
  }


  @Override
  public TimeSecWriter timeSec(String name) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      TimeSecVector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            MinorType.TIMESEC.getType()
          ,null, null),
          TimeSecVector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.TIMESEC);
      }
    }
    return writer;
  }


  @Override
  public TimeMilliWriter timeMilli(String name) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      TimeMilliVector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            MinorType.TIMEMILLI.getType()
          ,null, null),
          TimeMilliVector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.TIMEMILLI);
      }
    }
    return writer;
  }


  @Override
  public BigIntWriter bigInt(String name) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      BigIntVector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            MinorType.BIGINT.getType()
          ,null, null),
          BigIntVector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.BIGINT);
      }
    }
    return writer;
  }


  @Override
  public UInt8Writer uInt8(String name) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      UInt8Vector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            MinorType.UINT8.getType()
          ,null, null),
          UInt8Vector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.UINT8);
      }
    }
    return writer;
  }


  @Override
  public Float8Writer float8(String name) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      Float8Vector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            MinorType.FLOAT8.getType()
          ,null, null),
          Float8Vector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.FLOAT8);
      }
    }
    return writer;
  }


  @Override
  public DateMilliWriter dateMilli(String name) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      DateMilliVector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            MinorType.DATEMILLI.getType()
          ,null, null),
          DateMilliVector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.DATEMILLI);
      }
    }
    return writer;
  }


  @Override
  public DurationWriter duration(String name) {
    // returns existing writer
    final FieldWriter writer = fields.get(handleCase(name));
    Preconditions.checkNotNull(writer);
    return writer;
  }

  @Override
  public DurationWriter duration(String name, org.apache.arrow.vector.types.TimeUnit unit) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      DurationVector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            new org.apache.arrow.vector.types.pojo.ArrowType.Duration(unit)
          ,null, null),
          DurationVector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ArrowType arrowType = new org.apache.arrow.vector.types.pojo.ArrowType.Duration(unit);
        ((PromotableWriter)writer).getWriter(MinorType.DURATION, arrowType);
      }
    }
    return writer;
  }


  @Override
  public TimeStampSecWriter timeStampSec(String name) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      TimeStampSecVector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            MinorType.TIMESTAMPSEC.getType()
          ,null, null),
          TimeStampSecVector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.TIMESTAMPSEC);
      }
    }
    return writer;
  }


  @Override
  public TimeStampMilliWriter timeStampMilli(String name) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      TimeStampMilliVector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            MinorType.TIMESTAMPMILLI.getType()
          ,null, null),
          TimeStampMilliVector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.TIMESTAMPMILLI);
      }
    }
    return writer;
  }


  @Override
  public TimeStampMicroWriter timeStampMicro(String name) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      TimeStampMicroVector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            MinorType.TIMESTAMPMICRO.getType()
          ,null, null),
          TimeStampMicroVector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.TIMESTAMPMICRO);
      }
    }
    return writer;
  }


  @Override
  public TimeStampNanoWriter timeStampNano(String name) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      TimeStampNanoVector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            MinorType.TIMESTAMPNANO.getType()
          ,null, null),
          TimeStampNanoVector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.TIMESTAMPNANO);
      }
    }
    return writer;
  }


  @Override
  public TimeStampSecTZWriter timeStampSecTZ(String name) {
    // returns existing writer
    final FieldWriter writer = fields.get(handleCase(name));
    Preconditions.checkNotNull(writer);
    return writer;
  }

  @Override
  public TimeStampSecTZWriter timeStampSecTZ(String name, String timezone) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      TimeStampSecTZVector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            new org.apache.arrow.vector.types.pojo.ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.SECOND, timezone)
          ,null, null),
          TimeStampSecTZVector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ArrowType arrowType = new org.apache.arrow.vector.types.pojo.ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.SECOND, timezone);
        ((PromotableWriter)writer).getWriter(MinorType.TIMESTAMPSECTZ, arrowType);
      }
    }
    return writer;
  }


  @Override
  public TimeStampMilliTZWriter timeStampMilliTZ(String name) {
    // returns existing writer
    final FieldWriter writer = fields.get(handleCase(name));
    Preconditions.checkNotNull(writer);
    return writer;
  }

  @Override
  public TimeStampMilliTZWriter timeStampMilliTZ(String name, String timezone) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      TimeStampMilliTZVector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            new org.apache.arrow.vector.types.pojo.ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, timezone)
          ,null, null),
          TimeStampMilliTZVector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ArrowType arrowType = new org.apache.arrow.vector.types.pojo.ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, timezone);
        ((PromotableWriter)writer).getWriter(MinorType.TIMESTAMPMILLITZ, arrowType);
      }
    }
    return writer;
  }


  @Override
  public TimeStampMicroTZWriter timeStampMicroTZ(String name) {
    // returns existing writer
    final FieldWriter writer = fields.get(handleCase(name));
    Preconditions.checkNotNull(writer);
    return writer;
  }

  @Override
  public TimeStampMicroTZWriter timeStampMicroTZ(String name, String timezone) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      TimeStampMicroTZVector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            new org.apache.arrow.vector.types.pojo.ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, timezone)
          ,null, null),
          TimeStampMicroTZVector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ArrowType arrowType = new org.apache.arrow.vector.types.pojo.ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, timezone);
        ((PromotableWriter)writer).getWriter(MinorType.TIMESTAMPMICROTZ, arrowType);
      }
    }
    return writer;
  }


  @Override
  public TimeStampNanoTZWriter timeStampNanoTZ(String name) {
    // returns existing writer
    final FieldWriter writer = fields.get(handleCase(name));
    Preconditions.checkNotNull(writer);
    return writer;
  }

  @Override
  public TimeStampNanoTZWriter timeStampNanoTZ(String name, String timezone) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      TimeStampNanoTZVector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            new org.apache.arrow.vector.types.pojo.ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.NANOSECOND, timezone)
          ,null, null),
          TimeStampNanoTZVector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ArrowType arrowType = new org.apache.arrow.vector.types.pojo.ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.NANOSECOND, timezone);
        ((PromotableWriter)writer).getWriter(MinorType.TIMESTAMPNANOTZ, arrowType);
      }
    }
    return writer;
  }


  @Override
  public TimeMicroWriter timeMicro(String name) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      TimeMicroVector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            MinorType.TIMEMICRO.getType()
          ,null, null),
          TimeMicroVector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.TIMEMICRO);
      }
    }
    return writer;
  }


  @Override
  public TimeNanoWriter timeNano(String name) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      TimeNanoVector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            MinorType.TIMENANO.getType()
          ,null, null),
          TimeNanoVector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.TIMENANO);
      }
    }
    return writer;
  }


  @Override
  public IntervalDayWriter intervalDay(String name) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      IntervalDayVector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            MinorType.INTERVALDAY.getType()
          ,null, null),
          IntervalDayVector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.INTERVALDAY);
      }
    }
    return writer;
  }


  @Override
  public IntervalMonthDayNanoWriter intervalMonthDayNano(String name) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      IntervalMonthDayNanoVector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            MinorType.INTERVALMONTHDAYNANO.getType()
          ,null, null),
          IntervalMonthDayNanoVector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.INTERVALMONTHDAYNANO);
      }
    }
    return writer;
  }


  @Override
  public Decimal256Writer decimal256(String name) {
    // returns existing writer
    final FieldWriter writer = fields.get(handleCase(name));
    Preconditions.checkNotNull(writer);
    return writer;
  }

  @Override
  public Decimal256Writer decimal256(String name, int scale, int precision) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      Decimal256Vector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            new org.apache.arrow.vector.types.pojo.ArrowType.Decimal(precision, scale, Decimal256Vector.TYPE_WIDTH * 8)
          ,null, null),
          Decimal256Vector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.DECIMAL256, new org.apache.arrow.vector.types.pojo.ArrowType.Decimal(precision, scale, Decimal256Vector.TYPE_WIDTH * 8));
      }
    }
    return writer;
  }


  @Override
  public DecimalWriter decimal(String name) {
    // returns existing writer
    final FieldWriter writer = fields.get(handleCase(name));
    Preconditions.checkNotNull(writer);
    return writer;
  }

  @Override
  public DecimalWriter decimal(String name, int scale, int precision) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      DecimalVector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            new org.apache.arrow.vector.types.pojo.ArrowType.Decimal(precision, scale, DecimalVector.TYPE_WIDTH * 8)
          ,null, null),
          DecimalVector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.DECIMAL, new org.apache.arrow.vector.types.pojo.ArrowType.Decimal(precision, scale, DecimalVector.TYPE_WIDTH * 8));
      }
    }
    return writer;
  }


  @Override
  public FixedSizeBinaryWriter fixedSizeBinary(String name) {
    // returns existing writer
    final FieldWriter writer = fields.get(handleCase(name));
    Preconditions.checkNotNull(writer);
    return writer;
  }

  @Override
  public FixedSizeBinaryWriter fixedSizeBinary(String name, int byteWidth) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      FixedSizeBinaryVector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            new org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeBinary(byteWidth)
          ,null, null),
          FixedSizeBinaryVector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ArrowType arrowType = new org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeBinary(byteWidth);
        ((PromotableWriter)writer).getWriter(MinorType.FIXEDSIZEBINARY, arrowType);
      }
    }
    return writer;
  }


  @Override
  public VarBinaryWriter varBinary(String name) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      VarBinaryVector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            MinorType.VARBINARY.getType()
          ,null, null),
          VarBinaryVector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.VARBINARY);
      }
    }
    return writer;
  }


  @Override
  public VarCharWriter varChar(String name) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      VarCharVector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            MinorType.VARCHAR.getType()
          ,null, null),
          VarCharVector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.VARCHAR);
      }
    }
    return writer;
  }


  @Override
  public ViewVarBinaryWriter viewVarBinary(String name) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      ViewVarBinaryVector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            MinorType.VIEWVARBINARY.getType()
          ,null, null),
          ViewVarBinaryVector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.VIEWVARBINARY);
      }
    }
    return writer;
  }


  @Override
  public ViewVarCharWriter viewVarChar(String name) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      ViewVarCharVector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            MinorType.VIEWVARCHAR.getType()
          ,null, null),
          ViewVarCharVector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.VIEWVARCHAR);
      }
    }
    return writer;
  }


  @Override
  public LargeVarCharWriter largeVarChar(String name) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      LargeVarCharVector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            MinorType.LARGEVARCHAR.getType()
          ,null, null),
          LargeVarCharVector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.LARGEVARCHAR);
      }
    }
    return writer;
  }


  @Override
  public LargeVarBinaryWriter largeVarBinary(String name) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      LargeVarBinaryVector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            MinorType.LARGEVARBINARY.getType()
          ,null, null),
          LargeVarBinaryVector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.LARGEVARBINARY);
      }
    }
    return writer;
  }


  @Override
  public BitWriter bit(String name) {
    FieldWriter writer = fields.get(handleCase(name));
    if(writer == null) {
      ValueVector vector;
      ValueVector currentVector = container.getChild(name);
      BitVector v = container.addOrGet(name,
          new FieldType(addVectorAsNullable,
            MinorType.BIT.getType()
          ,null, null),
          BitVector.class);
      writer = new PromotableWriter(v, container, getNullableStructWriterFactory());
      vector = v;
      if (currentVector == null || currentVector != vector) {
        if(this.initialCapacity > 0) {
          vector.setInitialCapacity(this.initialCapacity);
        }
        vector.allocateNewSafe();
      }
      writer.setPosition(idx());
      fields.put(handleCase(name), writer);
    } else {
      if (writer instanceof PromotableWriter) {
        // ensure writers are initialized
        ((PromotableWriter)writer).getWriter(MinorType.BIT);
      }
    }
    return writer;
  }


}
