package scala.notebook

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructType,TimestampType}
import scala.utills.CustomerUtills
import org.apache.spark.sql.functions._

object ReviewsData {

  val path = "src/main/resources/OrderReviews.csv"
  val customerSchema = new StructType()
    .add("review_id",StringType)
    .add("order_id", StringType)
    .add("review_score", IntegerType)
    .add("review_comment_title", StringType)
    .add("review_comment_message", StringType)
    .add("review_creation_date", StringType)
    .add("review_answer_timestamp", StringType)

  def readReviewData(spark: SparkSession, formate: String, header: Boolean): DataFrame = {
    val df = CustomerUtills.readFile(spark, path, formate, customerSchema, header)
    val df1 = df.withColumn("review_creation_date",split(col("review_creation_date")," ").getItem(0))
      .withColumn("review_creation_date",to_date(col("review_creation_date"),"M/dd/yyyy"))
    df1
  }
}
