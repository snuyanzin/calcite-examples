package com.nuyanzin.calcite.examples.test.udtf;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.function.Consumer;

import com.nuyanzin.calcite.examples.udtf.IntStringGenerator;
import com.nuyanzin.calcite.examples.udtf.SequenceGenerator;
import com.nuyanzin.calcite.examples.udtf.VarcharRepeater;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.TableFunctionImpl;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for udtfs.
 */
public class UdtfTest {

  @Test public void testVarcharRepeater() {
    final String expected = ""
        + "test_string\n"
        + "test_string\n"
        + "test_string\n"
        + "test_string\n"
        + "test_string\n";

    final String schemaName = "s";
    final String udtfName = "Repeater";
    final String sql = "select * "
        + "from table(\"" + schemaName + "\".\"" + udtfName + "\"(5, 'test_string')) as t(s)";
    final Method seqGenWithCustomIncrement = Types.lookupMethod(
        VarcharRepeater.class, "repeat", int.class, String.class);

    final Consumer<SchemaPlus> registerSchemaAndUdtf = rootSchema -> {
      SchemaPlus schema = rootSchema.add(schemaName, new AbstractSchema());
      schema.add(udtfName, TableFunctionImpl.create(seqGenWithCustomIncrement));
    };

    checkTableFunction(expected, sql, registerSchemaAndUdtf);
  }

  @Test public void testSequenceGenerationWithCustomIncrement() {
    final String expected = "1\n21\n41\n";
    final String schemaName = "s";
    final String udtfName = "Generate";
    final String sql = "select * "
        + "from table(\"" + schemaName + "\".\"" + udtfName + "\"(1, 3, 20)) as t(s)";
    final Method seqGenWithCustomIncrement = Types.lookupMethod(
        SequenceGenerator.class, "generate", int.class, int.class, int.class);

    final Consumer<SchemaPlus> registerSchemaAndUdtf = rootSchema -> {
      SchemaPlus schema = rootSchema.add(schemaName, new AbstractSchema());
      schema.add(udtfName, TableFunctionImpl.create(seqGenWithCustomIncrement));
    };

    checkTableFunction(expected, sql, registerSchemaAndUdtf);
  }

  @Test public void testSequenceGenerationWithDefaultIncrement() {
    final String expected = "5\n6\n7\n";
    final String schemaName = "s";
    final String udtfName = "Generate";
    final String sql = "select * "
        + "from table(\"" + schemaName + "\".\"" + udtfName + "\"(5, 3)) as t(s)";
    final Method seqGen = Types.lookupMethod(
        SequenceGenerator.class, "generate", int.class, int.class);

    final Consumer<SchemaPlus> registerSchemaAndUdtf = rootSchema -> {
      SchemaPlus schema = rootSchema.add(schemaName, new AbstractSchema());
      schema.add(udtfName, TableFunctionImpl.create(seqGen));
    };

    checkTableFunction(expected, sql, registerSchemaAndUdtf);
  }

  @Test public void testJsonValue() {
    final String expected = ""
        + "0\t\n"
        + "1\ta\n"
        + "2\tab\n"
        + "3\tabc\n"
        + "4\tabcd\n"
        + "5\tabcde\n";
    final String schemaName = "s";
    final String udtfName = "Generate";
    final String sql = "select * "
        + "from table(\"" + schemaName + "\".\"" + udtfName + "\"(6)) as t(s, q)";
    final Method seqGen = Types.lookupMethod(
        IntStringGenerator.class, "generateStrings", int.class);

    final Consumer<SchemaPlus> registerSchemaAndUdtf = rootSchema -> {
      SchemaPlus schema = rootSchema.add(schemaName, new AbstractSchema());
      schema.add(udtfName, TableFunctionImpl.create(seqGen));
    };

    checkTableFunction(expected, sql, registerSchemaAndUdtf);
  }

  private void checkTableFunction(
      String expected, String sql, Consumer<SchemaPlus> schemaAndUdtfRegister) {
    try {
      Connection connection = DriverManager.getConnection("jdbc:calcite:");
      CalciteConnection calciteConnection =
          connection.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      schemaAndUdtfRegister.accept(rootSchema);
      ResultSet resultSet = connection.createStatement().executeQuery(sql);
      final StringBuilder b = new StringBuilder();
      final int columnCount = resultSet.getMetaData().getColumnCount();
      while (resultSet.next()) {
        for (int i = 1; i <= columnCount; i++) {
          b.append(resultSet.getString(i));
          if (i < columnCount) {
            b.append("\t");
          }
        }
        b.append("\n");
      }
      assertEquals(expected, b.toString());
    } catch (Throwable t) {
      // fail
      t.printStackTrace();
      throw new RuntimeException(t);
    }
  }
}
