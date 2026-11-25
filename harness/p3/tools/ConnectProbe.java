import java.util.List;
import java.util.function.Supplier;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.row_number;

public class ConnectProbe {
    private static void probe(String name, Supplier<Dataset<Row>> plan) {
        try {
            List<String> rows = plan.get().collectAsList().stream().map(Row::toString).sorted().toList();
            System.out.printf("P3\t%s\tok\t%s%n", name, rows);
        } catch (Throwable failure) {
            String diagnostic = failure.toString().replaceAll("[\\r\\n\\t]+", " ");
            System.out.printf("P3\t%s\terror\t%s%n", name, diagnostic);
        }
    }

    public static void main(String[] args) {
        try (SparkSession session = SparkSession.builder().remote(args[0]).create()) {
            probe("select_literal", () -> session.sql("SELECT 1"));
            probe("range_filter", () -> session.range(8).where("id % 2 = 0").select("id"));
            probe("groupby_sum", () -> session.range(8).selectExpr("id", "id % 2 AS g").groupBy("g").sum("id"));
            probe("window", () -> session.range(8).selectExpr("id", "id % 2 AS g")
                .select(col("id"), row_number().over(Window.partitionBy("g").orderBy("id"))));
            probe("explode", () -> session.range(8).selectExpr("id", "posexplode(array(id,id+1))"));
            probe("higher_order", () -> session.range(8).selectExpr("id", "transform(array(id,id+1), x -> x*2)"));
            probe("approx", () -> session.range(8).selectExpr("approx_count_distinct(id)", "percentile_approx(id, 0.5)"));
            probe("timestamp_ntz", () -> session.sql("SELECT TIMESTAMP_NTZ '2024-03-10 02:10:00' + INTERVAL 1 HOUR"));
            probe("json_regex", () -> session.range(3).selectExpr(
                "get_json_object(concat('{\042x\042:',id,'}'),'$.x')", "regexp_replace(concat('x',id),'[0-9]','_')"));
        }
    }
}
