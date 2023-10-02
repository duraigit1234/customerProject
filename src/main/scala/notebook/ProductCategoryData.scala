package notebook

import com.typesafe.config.ConfigFactory
import notebook.GetHiveTable
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import utills.CustomerUtills

object ProductCategoryData {

  val conf = ConfigFactory.load()


  val path = "src/main/resources/ProductCategory.csv"
  val SourcePath = conf.getString("path.CategorySourcePath")
  val Outputpath = conf.getString("path.CategoryOutputPath")
  val tableName = conf.getString("tablename.category")
  val customerSchema = new StructType()
    .add("product_category_name", StringType)
    .add("product_category_name_english", StringType)


  def readCategoryData(spark: SparkSession, formate: String, header: Boolean): Unit = {
    GetHiveTable.get_category_table(spark, tableName, Outputpath)
    val df = CustomerUtills.readFile(spark, SourcePath, formate, customerSchema, header)
//    CustomerUtills.writeFile(df,Outputpath,"parquet")

  }

}
