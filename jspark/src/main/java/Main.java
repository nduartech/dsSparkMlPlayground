import org.apache.spark.ml.stat.Correlation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("MVC").getOrCreate();
        Dataset<Row> sqlDF = spark.sql("SELECT * FROM parquet.`mvc/crashes/` A, parquet.`mvc/vehicles/` B, parquet.`mvc/person/` C WHERE A.CRASH_DATE = B.CRASH_DATE AND B.CRASH_DATE = C.CRASH_DATE AND to_date(A.CRASH_DATE,'mm/dd/yyyy')>=to_date('04/01/2019','mm/dd/yyyy')");
        Row r1 = Correlation.corr(sqlDF, "features").head();
        System.out.println("Pearson correlation matrix:\n" + r1.get(0).toString());

        Row r2 = Correlation.corr(sqlDF, "features", "spearman").head();
        System.out.println("Spearman correlation matrix:\n" + r2.get(0).toString());
        spark.stop();
    }
}
