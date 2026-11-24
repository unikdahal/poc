package poc;

import java.util.*;
import java.util.concurrent.*;
import org.apache.arrow.adbc.core.*;
import org.apache.arrow.adbc.driver.flightsql.FlightSqlDriver;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/** Java ADBC Flight SQL probe: which client calls work against a Flight SQL server. */
public class Probe {
  static final Map<String, String> out = new LinkedHashMap<>();

  interface Body { String run() throws Exception; }

  static void probe(String name, Body b) {
    long t0 = System.nanoTime();
    try {
      String r = b.run();
      out.put(name, "{\"ok\":true,\"ms\":" + (System.nanoTime() - t0) / 1e6 + ",\"result\":" + q(r) + "}");
    } catch (Throwable e) {
      String m = e.getClass().getSimpleName() + ": " + String.valueOf(e.getMessage()).split("\n")[0];
      out.put(name, "{\"ok\":false,\"error\":" + q(m.length() > 400 ? m.substring(0, 400) : m) + "}");
    }
    System.out.println(name + " -> " + out.get(name));
  }

  static String q(String s) {
    return "\"" + String.valueOf(s).replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", " ") + "\"";
  }

  static long drain(ArrowReader r) throws Exception {
    long rows = 0;
    while (r.loadNextBatch()) rows += r.getVectorSchemaRoot().getRowCount();
    r.close();
    return rows;
  }

  static String query(AdbcConnection c, String sql) throws Exception {
    try (AdbcStatement s = c.createStatement()) {
      s.setSqlQuery(sql);
      try (AdbcStatement.QueryResult r = s.executeQuery()) {
        ArrowReader rd = r.getReader();
        StringBuilder sb = new StringBuilder();
        long rows = 0;
        while (rd.loadNextBatch()) {
          VectorSchemaRoot root = rd.getVectorSchemaRoot();
          rows += root.getRowCount();
          if (sb.length() < 200) sb.append(root.contentToTSVString().trim());
        }
        return "rows=" + rows + " " + sb;
      }
    }
  }

  public static void main(String[] args) throws Exception {
    String uri = args.length > 0 ? args[0] : "grpc+tcp://localhost:32010";
    BufferAllocator alloc = new RootAllocator();
    Map<String, Object> params = new HashMap<>();
    params.put("uri", uri);
    AdbcDatabase db = new FlightSqlDriver(alloc).open(params);
    AdbcConnection c = db.connect();

    probe("query/select-1", () -> query(c, "SELECT 1 AS x"));
    probe("query/iceberg-count", () -> query(c, "SELECT count(*) FROM polaris.tpch_sf1.orders"));
    probe("ddl/create-table-executeQuery", () -> query(c,
        "CREATE TABLE IF NOT EXISTS polaris.flight.tj (id bigint) USING iceberg"));
    probe("dml/executeUpdate-insert", () -> {
      try (AdbcStatement s = c.createStatement()) {
        s.setSqlQuery("INSERT INTO polaris.flight.tj VALUES (1)");
        return "affected=" + s.executeUpdate().getAffectedRows();
      }
    });
    probe("prepared/prepare+bind+execute", () -> {
      try (AdbcStatement s = c.createStatement()) {
        s.setSqlQuery("SELECT ? AS p");
        s.prepare();
        Schema sch = new Schema(List.of(Field.nullable("p", new ArrowType.Int(64, true))));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(sch, alloc)) {
          ((BigIntVector) root.getVector(0)).setSafe(0, 41);
          root.setRowCount(1);
          s.bind(root);
          try (AdbcStatement.QueryResult r = s.executeQuery()) { return "rows=" + drain(r.getReader()); }
        }
      }
    });
    probe("metadata/getInfo", () -> "rows=" + drain(c.getInfo()));
    probe("metadata/getTableTypes", () -> "rows=" + drain(c.getTableTypes()));
    probe("metadata/getObjects(TABLES)", () ->
        "rows=" + drain(c.getObjects(AdbcConnection.GetObjectsDepth.TABLES, null, null, null, null, null)));
    probe("metadata/getTableSchema", () -> String.valueOf(c.getTableSchema(null, "tpch_sf1", "nation")));
    probe("statement/cancel", () -> {
      AdbcStatement s = c.createStatement();
      s.setSqlQuery("SELECT count(*) FROM polaris.tpch_sf1.lineitem a JOIN polaris.tpch_sf1.lineitem b ON a.l_quantity = b.l_quantity");
      ExecutorService ex = Executors.newSingleThreadExecutor();
      Future<String> f = ex.submit(() -> { try (AdbcStatement.QueryResult r = s.executeQuery()) { return "finished rows=" + drain(r.getReader()); } });
      Thread.sleep(3000);
      String cancel;
      try { s.cancel(); cancel = "cancel() ok"; } catch (Exception e) { cancel = "cancel() failed: " + e.getMessage(); }
      String res;
      try { res = f.get(60, TimeUnit.SECONDS); } catch (TimeoutException e) { res = "still running after 60s"; } catch (ExecutionException e) { res = "query ended with: " + e.getCause().getMessage(); }
      ex.shutdownNow();
      return cancel + "; " + res;
    });
    probe("latency/select-1-x200", () -> {
      double[] xs = new double[200];
      for (int i = 0; i < xs.length; i++) {
        long t = System.nanoTime();
        query(c, "SELECT 1");
        xs[i] = (System.nanoTime() - t) / 1e6;
      }
      Arrays.sort(xs);
      return String.format("p50_ms=%.2f p95_ms=%.2f p99_ms=%.2f", xs[100], xs[189], xs[197]);
    });
    StringBuilder sb = new StringBuilder("{");
    int i = 0;
    for (var e : out.entrySet()) sb.append(i++ > 0 ? "," : "").append(q(e.getKey())).append(":").append(e.getValue());
    sb.append("}");
    java.nio.file.Files.writeString(java.nio.file.Path.of(args.length > 1 ? args[1] : "adbc-java.json"), sb.toString());
    System.exit(0);
  }
}
