import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("MVC").getOrCreate();
        Dataset<Row> sqlDF = spark.sql("SELECT * FROM parquet.`mvc/crashes/` A, parquet.`mvc/vehicles/` B, parquet.`mvc/person/` C WHERE A.CRASH_DATE = B.CRASH_DATE AND B.CRASH_DATE = C.CRASH_DATE AND to_date(A.CRASH_DATE,'mm/dd/yyyy')==to_date('06/23/2019','mm/dd/yyyy')");
        sqlDF.printSchema();
        spark.stop();
    }
}
