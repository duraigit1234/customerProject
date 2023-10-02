package notebook

import com.typesafe.config.ConfigFactory
import notebook.GetHiveTable
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import utills.CustomerUtills
object ProductsData {

  val conf = ConfigFactory.load()



  val path = "src/main/resources/Products.csv"
  val SourcePath = conf.getString("path.ProductSourcePath")
  val Outputpath = conf.getString("path.ProductOutputPath")
  val tableName = conf.getString("tablename.customer")
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
    GetHiveTable.get_product_table(spark,tableName, Outputpath)
    val df = CustomerUtills.readFile(spark, SourcePath, formate, customerSchema, header)
//    CustomerUtills.writeFile(df,Outputpath,"parquet")

  }

}
