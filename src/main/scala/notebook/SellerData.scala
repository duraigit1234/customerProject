package notebook

import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.typesafe.config.ConfigFactory
import utills.CustomerUtills
import notebook.GetHiveTable
object SellerData {

  val conf = ConfigFactory.load()

  val path = "src/main/resources/Sellers.csv"
  val SourcePath = conf.getString("path.SellerSourcePath")
  val Outputpath = conf.getString("path.SellerOutputPath")
  val tableName = conf.getString("tablename.seller")
  val customerSchema = new StructType()
    .add("seller_id", StringType)
    .add("seller_zip_code_prefix", IntegerType)
    .add("seller_city", StringType)
    .add("seller_state", StringType)

  def readSellerData(spark: SparkSession, formate: String, header: Boolean): Unit = {
    GetHiveTable.get_seller_table(spark, tableName, Outputpath)
    val df = CustomerUtills.readFile(spark, SourcePath, formate, customerSchema, header)
//    CustomerUtills.writeFile(df,Outputpath,"parquet")

  }

}
