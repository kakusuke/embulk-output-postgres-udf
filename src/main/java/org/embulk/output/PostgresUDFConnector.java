package org.embulk.output;

import com.google.common.base.Throwables;

import java.sql.*;
import java.util.Properties;

/**
 * Created by kakusuke on 2015/03/21.
 */
public class PostgresUDFConnector {
    private static final Driver driver = new org.postgresql.Driver();

    private final String url;
    private final Properties properties;
    private final String schemaName;

    public PostgresUDFConnector(String url, Properties properties, String schemaName) {
        this.url = url;
        this.properties = properties;
        this.schemaName = schemaName;
    }

    public ConnectionWrapper connect() throws SQLException {
        Connection c = createConnection();
        try {
            ConnectionWrapper con = new ConnectionWrapper(c, schemaName);
            c = null;
            return con;
        } finally {
            if (c != null) {
                c.close();
            }
        }
    }

    private Connection createConnection() throws SQLException {
        SQLException firstException = null;
        int count = 0;
        while(count < 10) {
            count++;

            try {
                return driver.connect(url, properties);
            }
            catch (SQLRecoverableException | SQLTimeoutException e) {
                if (firstException == null) {
                    firstException = e;
                }
                try {
                    Thread.sleep(300);
                } catch (InterruptedException e2) {
                    throw Throwables.propagate(e2);
                }
            }
        }
        throw firstException;
    }
}
