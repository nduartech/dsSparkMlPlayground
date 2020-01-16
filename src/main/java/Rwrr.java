package st;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import scala.Tuple2;

public class Rwrr {

	public static void main(String[] args) {
		// Prepare the spark configuration by setting application name and master node "local" i.e. embedded mode
		SparkSession spark = SparkSession
		  .builder()
		  .appName("Jaark")
		  .getOrCreate();
		
		Dataset<Row> sqlDF = spark.sql("SELECT * FROM parquet.`crashes/` A, parquet.`vehicles/` B, parquet.`person/` C WHERE A.CRASH_DATE = B.CRASH_DATE AND B.CRASH_DATE = C.CRASH_DATE AND to_date(A.CRASH_DATE,'mm/dd/yyyy')>=to_date('04/01/2019','mm/dd/yyyy')");
		
		sqlDF.show();

	}

}
