package notebook

import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import utills.CustomerUtills
object ProductsData {

  val path = "src/main/resources/Products.csv"
  val SourcePath = "/user/cloudera/data/source_data/products"
  val Outputpath = "/user/cloudera/data/staging/products"
  val customerSchema = new StructType()
    .add("product_id", StringType)
    .add("product_category_name", StringType)
    .add("product_name_length", IntegerType)
    .add("product_description_length", IntegerType)
    .add("product_photos_qty", IntegerType)
    .add("product_weight_g", IntegerType)
    .add("product_length_cm", IntegerType)
    .add("product_height_cm", IntegerType)
    .add("product_width_cm", IntegerType)


  def readProductsData(spark: SparkSession, formate: String, header: Boolean): Unit = {
    val df = CustomerUtills.readFile(spark, SourcePath, formate, customerSchema, header)
    CustomerUtills.writeFile(df,Outputpath)

  }

}
