Embulk::JavaPlugin.register_output(
  "postgres_udf", "org.embulk.output.PostgresUDFOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
