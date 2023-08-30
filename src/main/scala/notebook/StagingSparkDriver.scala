package notebook
import notebook.StagingData
import org.apache.spark.sql.SparkSession

object StagingSparkDriver {

  val spark = SparkSession.builder()
    .appName("Customer Data Project")
    .master("local")
    .getOrCreate()
  spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

  StagingData.get_result_table(spark)

}
