package notebook

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}
import utills.CustomerUtills
import com.typesafe.config.ConfigFactory
import notebook.GetHiveTable
object GeoLocationData {


  val conf = ConfigFactory.load()


  val path = "src/main/resources/GeoLocation.csv"
  val SourcePath = conf.getString("path.GeoLocationSourcePath")
  val Outputpath = conf.getString("path.GeoLocationOutputPath")
  val tableName = conf.getString("tablename.geolocation")
  val customerSchema = new StructType()
    .add("geolocation_zip_code_prefix", IntegerType)
    .add("geolocation_lat", DoubleType)
    .add("geolocation_lang", DoubleType)
    .add("geolocation_city", StringType)
    .add("geolocation_state", StringType)

  def readLocatinData(spark: SparkSession, formate: String, header: Boolean): Unit = {
    GetHiveTable.get_geoLocation_table(spark,tableName,Outputpath)
    val df = CustomerUtills.readFile(spark, SourcePath, formate, customerSchema, header)
//    CustomerUtills.writeFile(df,Outputpath,"parquet")

  }


}
