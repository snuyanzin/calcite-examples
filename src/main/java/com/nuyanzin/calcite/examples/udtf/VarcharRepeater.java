package com.nuyanzin.calcite.examples.udtf;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Varchar repeater.
 */
public class VarcharRepeater extends AbstractTable implements ScannableTable {

  private final int size;
  private final String value;

  public VarcharRepeater(int size, String value) {
    this.size = size;
    this.value = value;
  }

  public Enumerable<Object[]> scan(DataContext root) {
    return new AbstractEnumerable<Object[]>() {
      public Enumerator<Object[]> enumerator() {
        return Linq4j.transform(
            new Enumerator<String>() {
              int i = 0;
              public String current() {
                return value;
              }

              public boolean moveNext() {
                if (i < size) {
                  i++;
                  return true;
                }
                return false;
              }

              public void reset() {
                i = 0;
              }

              public void close() {
              }
            },
            s -> new Object[]{s});
      }
    };
  }

  @SuppressWarnings("unused") // called via reflection
  public static ScannableTable repeat(int size, String value) {
    return new VarcharRepeater(size, value);
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        .add("S", SqlTypeName.VARCHAR, size + 1)
        .build();
  }
}

// End VarcharRepeater.java

