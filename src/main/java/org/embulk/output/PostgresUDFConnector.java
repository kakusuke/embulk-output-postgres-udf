package org.embulk.output;

import com.google.common.base.Throwables;
import org.embulk.spi.Exec;
import org.embulk.spi.util.RetryExecutor;
import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by kakusuke on 2015/03/21.
 */
public class PostgresUDFConnector {
    private static final Driver driver = new org.postgresql.Driver();
    private final Logger log = Exec.getLogger(PostgresUDFOutputPlugin.class);


    private final String url;
    private final Properties properties;
    private final String schemaName;

    public PostgresUDFConnector(String url, Properties properties, String schemaName) {
        this.url = url;
        this.properties = properties;
        this.schemaName = schemaName;
    }

    public ConnectionWrapper connect(boolean autoCommit) throws SQLException {
        Connection c = createConnection();
        try {
            ConnectionWrapper con = new ConnectionWrapper(c, schemaName, autoCommit);
            c = null;
            return con;
        } finally {
            if (c != null) {
                c.close();
            }
        }
    }

    private Connection createConnection() throws SQLException {
        try {
            return RetryExecutor.retryExecutor()
                    .withRetryLimit(12)
                    .withInitialRetryWait(1000)
                    .withMaxRetryWait(30 * 60 * 1000)
                    .runInterruptible(new RetryExecutor.Retryable<Connection>() {
                        @Override
                        public Connection call() throws Exception {
                            return driver.connect(url, properties);
                        }

                        @Override
                        public boolean isRetryableException(Exception exception) {
                            /*
                            if (exception instanceof SQLRecoverableException) return true;
                            if (exception instanceof SQLTimeoutException) return true;
                            if (exception instanceof ConnectException) return true;
                            */
                            return true;
                        }

                        @Override
                        public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait) throws RetryExecutor.RetryGiveupException {
                            String message = String.format("connection failed. Retrying %d/%d after %d seconds. Message: %s",
                                    retryCount, retryLimit, retryWait/1000, exception.getMessage());
                            if (retryCount % 3 == 0) {
                                log.warn(message, exception);
                            } else {
                                log.warn(message);
                            }
                        }

                        @Override
                        public void onGiveup(Exception firstException, Exception lastException) throws RetryExecutor.RetryGiveupException {
                        }
                    });
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (RetryExecutor.RetryGiveupException e) {
            Throwables.propagateIfInstanceOf(e.getCause(), SQLException.class);
            throw Throwables.propagate(e.getCause());
        }
    }
}
