package notebook

import notebook.OrdersData.Outputpath
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import utills.CustomerUtills
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.File

object OrdersData {

//  val audit_table_path = "C://Users//SSLTP11358//Desktop//audit//audit_table.csv"
//  val newfile = new File(audit_table_path)
//  val exist_or_not = newfile.exists()
//  val path = "src/main/resources/Orders.csv"
//  val table_path = "C://Users//SSLTP11358//Desktop//target//orders.csv"
  val SourcePath = "/user/cloudera/data/source_data/orders"
  val Outputpath = "/user/cloudera/data/staging/orders"
  val audit_table_path = "/user/cloudera/data/audit"

  val audit_table_schema = new StructType()
    .add("Application_ID", StringType)
    .add("Job_Run_Date", StringType)
    .add("Source_Path", StringType)
    .add("Target_Path", StringType)
    .add("Table_Name", StringType)
    .add("Source_Count", LongType)
    .add("Target_Count", LongType)
    .add("Application_Status", StringType)
    .add("Load_Type", StringType)
    .add("Snap_Shot_Date", StringType)

  val customerSchema = new StructType()
    .add("order_id", StringType)
    .add("customer_id", StringType)
    .add("order_status", StringType)
    .add("order_purchase_timestamp", StringType)
    .add("order_approved_at", StringType)
    .add("order_delivered_carrier_date", StringType)
    .add("order_deliver_customer_date", StringType)
    .add("order_estimated_delivery_date", StringType)

  def readOrderData(spark: SparkSession, formate: String, header: Boolean): Unit = {

    val exist_or_not = testDirExist(spark,audit_table_path)
    val appid = spark.sparkContext.applicationId
        if (exist_or_not){
         val existing_audit = CustomerUtills.readFile(spark,audit_table_path,"csv",audit_table_schema,true)
          val table_exist_or_not = existing_audit.filter(col("Table_Name") === "orders").count() > 0

          if (table_exist_or_not){
            val windspec = Window.partitionBy(existing_audit("Table_Name")).orderBy(existing_audit("Snap_Shot_Date").desc)
            val rownum = existing_audit.withColumn("row",row_number().over(windspec))
              .where(col("Table_Name") === "orders" and col("row") === 1)

            val latest_load_date = rownum.select("Snap_Shot_Date").collect()(0)(0).toString
            println(s"latest load Date $latest_load_date")
            val filterdf = CustomerUtills.readFile(spark, SourcePath, formate, customerSchema, header,filtercolumn = Some("order_purchase_timestamp"),filtercond = Some(latest_load_date))
            val trans_fiterdata = transform_data(filterdf)

            try {
              val newmaxDateDF = filterdf.agg(max("order_purchase_timestamp").alias("maxDate"))
              val newmaxDateAsString = newmaxDateDF.collect()(0)(0).toString
              println(s"Maximum Date as String: $newmaxDateAsString")
              val newdatetimedf = spark.sql("SELECT CAST(current_timestamp() AS STRING) AS current_datetime")
              val newcurrentDateTimeAsString = newdatetimedf.collect()(0)(0).toString

              CustomerUtills.writeFile(trans_fiterdata, Outputpath, "append", "csv")
              val newcnt = filterdf.count()
              val seqdata = Seq(
                (appid, newcurrentDateTimeAsString, SourcePath, Outputpath, "orders", newcnt, newcnt, "success", "incremental load", newmaxDateAsString)
              )
              val newauditDF = spark.createDataFrame(seqdata).toDF(audit_table_schema.fieldNames: _*)

              CustomerUtills.writeFile(newauditDF, audit_table_path, "append", "csv")
            } catch {
              case _: Throwable =>
                println("No data available")
            }

          }
          else {
            val df = CustomerUtills.readFile(spark, SourcePath, formate, customerSchema, header)
            val df1 = transform_data(df)

            val maxDateDF = df1.agg(max("order_purchase_timestamp").alias("maxDate"))
            val maxDateAsString = maxDateDF.collect()(0)(0).toString

            val datetimedf = spark.sql("SELECT CAST(current_timestamp() AS STRING) AS current_datetime")
            val currentDateTimeAsString = datetimedf.collect()(0)(0).toString


            CustomerUtills.writeFile(df1, Outputpath, "overwrite", "csv")
            val cnt = df1.count()
            val seqdata = Seq(
              (appid, currentDateTimeAsString, SourcePath, Outputpath, "orders", cnt, cnt, "success", "full load", maxDateAsString)
            )
            val emptyDF = spark.createDataFrame(seqdata).toDF(audit_table_schema.fieldNames: _*)

            CustomerUtills.writeFile(emptyDF, audit_table_path, "append", "csv")
          }

        }
        else{

          val df = CustomerUtills.readFile(spark, SourcePath, formate, customerSchema, header)
          val df1 = transform_data(df)

          val maxDateDF = df1.agg(max("order_purchase_timestamp").alias("maxDate"))
          val maxDateAsString = maxDateDF.collect()(0)(0).toString

            val datetimedf = spark.sql("SELECT CAST(current_timestamp() AS STRING) AS current_datetime")
            val currentDateTimeAsString =datetimedf.collect()(0)(0).toString


          CustomerUtills.writeFile(df1, Outputpath, "overwrite", "csv")
          val cnt = df1.count()
          val seqdata = Seq(
            (appid,currentDateTimeAsString,SourcePath,Outputpath,"orders",cnt,cnt,"success","full load",maxDateAsString)
          )
        val emptyDF = spark.createDataFrame(seqdata).toDF(audit_table_schema.fieldNames:_*)

          CustomerUtills.writeFile(emptyDF,audit_table_path,"append","csv")
        }


  }

  def testDirExist(spark:SparkSession,path: String): Boolean = {
    val hadoopfs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val p = new Path(path)
    hadoopfs.exists(p) && hadoopfs.getFileStatus(p).isDirectory
  }

  def transform_data(data:DataFrame):DataFrame={
    val df1 = data.withColumn("order_purchase_timestamp", to_date(split(col("order_purchase_timestamp"), " ").getItem(0), "M/dd/yyyy"))
      .withColumn("order_approved_at", to_date(split(col("order_approved_at"), " ").getItem(0), "M/dd/yyyy"))
      .withColumn("order_delivered_carrier_date", to_date(split(col("order_delivered_carrier_date"), " ").getItem(0), "M/dd/yyyy"))
      .withColumn("order_deliver_customer_date", to_date(split(col("order_deliver_customer_date"), " ").getItem(0), "M/dd/yyyy"))
      .withColumn("order_estimated_delivery_date", to_date(split(col("order_estimated_delivery_date"), " ").getItem(0), "M/dd/yyyy"))
      .withColumn("load_dtm", current_timestamp())
    df1
  }

}
