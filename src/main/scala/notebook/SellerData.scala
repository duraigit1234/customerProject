package notebook

import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import utills.CustomerUtills
object SellerData {

  val path = "src/main/resources/Sellers.csv"
  val SourcePath = "/user/cloudera/data/source_data/sellers"
  val Outputpath = "/user/cloudera/data/staging/sellers"
  val customerSchema = new StructType()
    .add("seller_id", StringType)
    .add("seller_zip_code_prefix", IntegerType)
    .add("seller_city", StringType)
    .add("seller_state", StringType)


  def readSellerData(spark: SparkSession, formate: String, header: Boolean): Unit = {
    val df = CustomerUtills.readFile(spark, SourcePath, formate, customerSchema, header)
    CustomerUtills.writeFile(df,Outputpath)

  }

}
