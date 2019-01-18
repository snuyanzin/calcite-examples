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
 * SequenceGenerator example.
 */
public class SequenceGenerator extends AbstractTable implements ScannableTable {
  private final int start;
  private final int count;
  private final int increment;

  private SequenceGenerator(int start, int count, int increment) {
    this.start = start;
    this.count = count;
    this.increment = increment;
  }

  private SequenceGenerator(int start, int count) {
    this(start, count, 1);
  }

  @SuppressWarnings("unused") // called via reflection
  public static ScannableTable generate(
      int start, int count, int increment) {
    return new SequenceGenerator(start, count, increment);
  }

  @SuppressWarnings("unused") // called via reflection
  public static ScannableTable generate(int start, int count) {
    return new SequenceGenerator(start, count);
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        .add("S", SqlTypeName.INTEGER)
        .build();
  }

  public Enumerable<Object[]> scan(DataContext root) {
    return new AbstractEnumerable<Object[]>() {
        public Enumerator<Object[]> enumerator() {
          return Linq4j.transform(
              new Enumerator<Integer>() {
                int c = 0;

                public Integer current() {
                  return start + (c - 1) * increment;
                }

                public boolean moveNext() {
                  if (c == count) {
                    return false;
                  }
                  c++;
                  return true;
                }

                public void reset() {
                  c = 0;
                }

                public void close() {
                }
              },
              s -> new Object[]{s}
          );
        }
    };
  }
}
