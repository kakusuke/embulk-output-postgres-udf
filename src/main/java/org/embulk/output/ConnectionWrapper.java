package org.embulk.output;

import org.embulk.spi.Exec;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.Type;
import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by kakusuke on 2015/03/21.
 */
public class ConnectionWrapper implements AutoCloseable {
    private final Logger logger = Exec.getLogger(ConnectionWrapper.class);
    private final Connection con;

    public ConnectionWrapper(Connection con, String schemaName) throws SQLException {
        this.con = con;
        con.setAutoCommit(false);
        if (schemaName != null) {
            Statement stmt = con.createStatement();
            String sql = "SET search_path TO '" + schemaName + "'";
            logger.info("SQL: " + sql);
            stmt.execute(sql);
        }
    }

    @Override
    public void close() throws Exception {
        con.close();
    }

    public void createTempFunction(String functionName, Schema schema, String query, String language) throws SQLException {
        String sql = String.format("CREATE OR REPLACE FUNCTION pg_temp.\"%s\"(%s) RETURNS void AS $$\n%s\n$$ LANGUAGE %s",
            functionName,
            getArgumentsString(schema),
            query,
            language
        );
        logger.info("SQL: " + sql);
        con.createStatement().execute(sql);
    }

    private String getArgumentsString(Schema schema) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0, len = schema.getColumnCount(); i < len; i++) {
            if (i > 0) {
                builder.append(", ");
            }
            builder.append("\"" + schema.getColumnName(i) + "\"");
            builder.append(" ");
            builder.append(getSqlTypeName(schema.getColumnType(i)));
        }
        return builder.toString();
    }

    public PreparedStatement prepareCall(String functionName, Schema schema) throws SQLException {
        StringBuilder builder = new StringBuilder();
        for (int i = 0, len = schema.getColumnCount(); i < len; i++) {
            if (i > 0) {
                builder.append(", ");
            }
            builder.append("?::" + getSqlTypeName(schema.getColumnType(i)));
        }
        String sql = String.format("{ call pg_temp.\"%s\"(%s) }", functionName, builder.toString());
        logger.info("SQL: " + sql);
        return con.prepareCall(sql);
    }

    public void commit() throws SQLException {
        con.commit();
    }

    public void rollback() throws SQLException {
        con.rollback();
    }

    private String getSqlTypeName(Type columnType) {
        if (columnType.getJavaType().equals(long.class)) {
            return "bigint";
        }
        else if (columnType.getJavaType().equals(double.class)) {
            return "float8";
        }
        else if (columnType.getJavaType().equals(boolean.class)) {
            return "boolean";
        }
        else if (columnType.getJavaType().equals(String.class)) {
            return "text";
        }
        else if (columnType.getJavaType().equals(Timestamp.class)) {
            return "timestamp";
        }
        return "unknown";
    }
}
