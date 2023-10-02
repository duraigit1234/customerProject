package notebook

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructType,TimestampType}
import utills.CustomerUtills
import org.apache.spark.sql.functions._
import com.typesafe.config.ConfigFactory
import notebook.GetHiveTable
object ReviewsData {

  val conf = ConfigFactory.load()


  val path = "src/main/resources/OrderReviews.csv"
  val SourcePath = conf.getString("path.ReviewsSourcePath")
  val Outputpath = conf.getString("path.ReviewsOutputPath")
  val tableName = conf.getString("tablename.review")
  val customerSchema = new StructType()
    .add("review_id",StringType)
    .add("order_id", StringType)
    .add("review_score", IntegerType)
    .add("review_comment_title", StringType)
    .add("review_comment_message", StringType)
    .add("review_creation_date", StringType)
    .add("review_answer_timestamp", StringType)

  def readReviewData(spark: SparkSession, formate: String, header: Boolean): Unit = {
    GetHiveTable.get_review_table(spark, tableName, Outputpath)
    val df = CustomerUtills.readFile(spark, SourcePath, formate, customerSchema, header)
    val df1 = df.withColumn("review_creation_date",split(col("review_creation_date")," ").getItem(0))
      .withColumn("review_creation_date",to_date(col("review_creation_date"),"M/dd/yyyy"))
//    CustomerUtills.writeFile(df1,Outputpath,"parquet")

  }
}
