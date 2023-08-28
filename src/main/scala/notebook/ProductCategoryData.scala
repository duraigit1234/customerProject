package notebook

import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import utills.CustomerUtills

object ProductCategoryData {

  val path = "src/main/resources/ProductCategory.csv"
  val SourcePath = "/user/cloudera/data/source_data/product_category_name_translation"
  val Outputpath = "/user/cloudera/data/staging/product_category_name_translation"
  val customerSchema = new StructType()
    .add("product_category_name", StringType)
    .add("product_category_name_english", StringType)


  def readCategoryData(spark: SparkSession, formate: String, header: Boolean): DataFrame = {
    val df = CustomerUtills.readFile(spark, SourcePath, formate, customerSchema, header)
    CustomerUtills.writeFile(df,Outputpath)
    df
  }

}
