package com.nuyanzin.calcite.examples.test.udtf;

import com.nuyanzin.calcite.examples.udtf.SequenceGenerator;
import com.nuyanzin.calcite.examples.udtf.VarcharRepeater;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.TableFunctionImpl;
import org.junit.Test;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

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

  private void checkTableFunction(String expected, String sql, Consumer<SchemaPlus> schemaAndUdtfRegister) {
    try {
      Connection connection = DriverManager.getConnection("jdbc:calcite:");
      CalciteConnection calciteConnection =
          connection.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      schemaAndUdtfRegister.accept(rootSchema);
      ResultSet resultSet = connection.createStatement().executeQuery(sql);
      final StringBuilder b = new StringBuilder();
      while (resultSet.next()) {
        b.append(resultSet.getString(1)).append("\n");
      }
      assertThat(b.toString(), is(expected));
    } catch (Throwable t) {
      // fail
      throw new RuntimeException(t);
    }
  }
}
