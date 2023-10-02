package notebook
import com.typesafe.config.ConfigFactory
import notebook.GetHiveTable
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import utills.CustomerUtills

object PaymentsData {

  val conf = ConfigFactory.load()


  val path = "src/main/resources/OrderPayments.csv"
  val SourcePath = conf.getString("path.PaymentsSourcePath")
  val Outputpath = conf.getString("path.PaymentsOutputPath")
  val tableName = conf.getString("tablename.payment")
  val customerSchema = new StructType()
    .add("order_id", StringType)
    .add("payment_sequential", IntegerType)
    .add("payment_type", StringType)
    .add("payment_installments", IntegerType)
    .add("payment_value", DoubleType)

  def readPaymentData(spark: SparkSession, formate: String, header: Boolean): Unit = {
    GetHiveTable.get_payment_table(spark, tableName, Outputpath)
    val df = CustomerUtills.readFile(spark, SourcePath, formate, customerSchema, header)
//    CustomerUtills.writeFile(df,Outputpath,"parquet")

  }

}
