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
  private final long start;
  private final long count;
  private final long increment;

  private SequenceGenerator(long start, long count, long increment) {
    this.start = start;
    this.count = count;
    this.increment = increment;
  }

  private SequenceGenerator(long start, long count) {
    this(start, count, 1);
  }

  @SuppressWarnings("unused") // called via reflection
  public static ScannableTable generate(
      long start, long count, long increment) {
    return new SequenceGenerator(start, count, increment);
  }

  @SuppressWarnings("unused") // called via reflection
  public static ScannableTable generate(long start, long count) {
    return new SequenceGenerator(start, count);
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return typeFactory.builder()
        .add("S", SqlTypeName.BIGINT, RelDataType.PRECISION_NOT_SPECIFIED)
        .build();
  }

  public Enumerable<Object[]> scan(DataContext root) {
    return new AbstractEnumerable<Object[]>() {
        public Enumerator<Object[]> enumerator() {
          return Linq4j.transform(
              new Enumerator<Long>() {
                long current = start;
                long c = 0;

                public Long current() {
                  return current;
                }

                public boolean moveNext() {
                  if (c == count) {
                    return false;
                  }
                  current += increment;
                  c++;
                  return true;
                }

                public void reset() {
                  c = 0;
                  current = start;
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
