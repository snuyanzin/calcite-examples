package com.nuyanzin.calcite.examples.udtf;

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.BaseQueryable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;

/**
 */
public class IntStringGenerator {

  private IntStringGenerator() {}
  public static QueryableTable generateStrings(final int count) {
    return new AbstractQueryableTable(IntString.class) {
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.createJavaType(IntString.class);
      }

      public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
          SchemaPlus schema, String tableName) {
        BaseQueryable<IntString> queryable =
            new BaseQueryable<IntString>(null, IntString.class, null) {
              public Enumerator<IntString> enumerator() {
                return new Enumerator<IntString>() {
                  static final String Z = "abcdefghijklm";

                  int i = 0;
                  int curI;
                  String curS;

                  public IntString current() {
                    return new IntString(curI, curS);
                  }

                  public boolean moveNext() {
                    if (i < count) {
                      curI = i;
                      curS = Z.substring(0, i % Z.length());
                      ++i;
                      return true;
                    } else {
                      return false;
                    }
                  }

                  public void reset() {
                    i = 0;
                  }

                  public void close() {
                  }
                };
              }
            };
        //noinspection unchecked
        return (Queryable<T>) queryable;
      }
    };
  }

  /** Class with int and String fields. */
  public static class IntString {
    public final int n;
    public final String s;

    public IntString(int n, String s) {
      this.n = n;
      this.s = s;
    }

    public String toString() {
      return "{n=" + n + ", s=" + s + "}";
    }
  }
}
