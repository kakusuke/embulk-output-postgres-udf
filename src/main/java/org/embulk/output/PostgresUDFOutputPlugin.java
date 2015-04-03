package org.embulk.output;

import com.google.common.base.Throwables;
import org.embulk.config.*;
import org.embulk.spi.*;
import org.embulk.spi.time.Timestamp;
import org.slf4j.Logger;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

public class PostgresUDFOutputPlugin implements OutputPlugin {
    public interface PluginTask extends Task {
        @Config("host")
        public String getHost();

        @Config("port")
        @ConfigDefault("5432")
        public int getPort();

        @Config("user")
        public String getUser();

        @Config("password")
        @ConfigDefault("\"\"")
        public String getPassword();

        @Config("database")
        public String getDatabase();

        @Config("schema")
        @ConfigDefault("\"public\"")
        public String getSchema();

        @Config("function")
        public String getFunction();

        @Config("language")
        @ConfigDefault("\"plpgsql\"")
        public String getLanguage();

        /*
        @Config("batch_size")
        public int getBatchSize();

        @Config("options")
        @ConfigDefault("{}")
        public Properties getOptions();
*/
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, Schema schema, int taskCount, OutputPlugin.Control control) {
        PluginTask task = config.loadConfig(PluginTask.class);

        // retryable (idempotent) output:
        // return resume(task.dump(), schema, taskCount, control);

        // non-retryable (non-idempotent) output:
        try {
            ConnectionWrapper con = getConnector(task).connect(true);
            con.createFunction(getFunctionName(), schema, task.getFunction(), task.getLanguage());
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
        control.run(task.dump());
        return Exec.newConfigDiff();
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource, Schema schema, int taskCount, OutputPlugin.Control control) {
        throw new UnsupportedOperationException("postgres-udf output plugin does not support resuming");
    }

    @Override
    public void cleanup(TaskSource taskSource, Schema schema, int taskCount, List<CommitReport> successCommitReports) {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        try {
            ConnectionWrapper con = getConnector(task).connect(true);
            con.dropFunction(getFunctionName(), schema);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public TransactionalPageOutput open(TaskSource taskSource, Schema schema, int taskIndex) {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        return new PluginPageOutput(task, schema);
    }

    private static PostgresUDFConnector getConnector(PluginTask task) {
        String url = String.format("jdbc:postgresql://%s:%d/%s",
                task.getHost(), task.getPort(), task.getDatabase());

        Properties props = new Properties();
        props.setProperty("user", task.getUser());
        props.setProperty("password", task.getPassword());
        props.setProperty("loginTimeout", "300");  // seconds
        props.setProperty("socketTimeout", "28800");  // seconds
        props.setProperty("tcpKeepAlive", "true");
        return new PostgresUDFConnector(url, props, task.getSchema());
    }

    private static String getFunctionName() {
        Timestamp t = Exec.session().getTransactionTime();
        return String.format("fn_%016x%08x", t.getEpochSecond(), t.getNano());
    }

    static class PluginPageOutput implements TransactionalPageOutput {
        private final Logger logger;
        private final PluginTask task;
        private final Schema schema;
        private final PageReader pageReader;
        private final ConnectionWrapper connection;

        PluginPageOutput(PluginTask task, Schema schema) {
            this.logger = Exec.getLogger(PluginPageOutput.class);
            this.task = task;
            this.schema = schema;
            this.pageReader = new PageReader(schema);
            try {
                this.connection = getConnector(task).connect(false);
            }
            catch (SQLException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void add(Page page) {
            long startTime = System.currentTimeMillis();
            pageReader.setPage(page);

            try (PreparedStatement stmt = this.connection.prepareCall(getFunctionName(), schema)) {
                while (pageReader.nextRecord()) {
                    for (int i = 0; i < schema.getColumnCount(); i++) {
                        Class<?> type = schema.getColumnType(i).getJavaType();
                        if (pageReader.isNull(i)) {
                            stmt.setObject(i + 1, null);
                        } else if (type.equals(boolean.class)) {
                            stmt.setBoolean(i + 1, pageReader.getBoolean(i));
                        } else if (type.equals(double.class)) {
                            stmt.setDouble(i + 1, pageReader.getDouble(i));
                        } else if (type.equals(long.class)) {
                            stmt.setLong(i + 1, pageReader.getLong(i));
                        } else if (type.equals(String.class)) {
                            stmt.setString(i + 1, pageReader.getString(i));
                        } else if (type.equals(Timestamp.class)) {
                            stmt.setTimestamp(i + 1, new java.sql.Timestamp(pageReader.getTimestamp(i).toEpochMilli()));
                        } else {
                            stmt.setObject(i + 1, null);
                        }
                    }
                    stmt.addBatch();
                }
                stmt.executeBatch();
                long endTime = System.currentTimeMillis();
                logger.info("> {} seconds.", (endTime - startTime) / 1000.0);
            }
            catch (SQLException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void finish() {
            try {
                connection.commit();
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void close() {
            try {
                connection.close();
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void abort() {
            try {
                connection.rollback();
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public CommitReport commit() {
            try {
                connection.commit();
            }
            catch (Exception e) {
                throw Throwables.propagate(e);
            }
            return Exec.newCommitReport();
        }

    }
}
