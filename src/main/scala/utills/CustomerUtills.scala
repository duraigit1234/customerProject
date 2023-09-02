package utills

import org.apache.spark.sql.types.{DateType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}

import java.text.SimpleDateFormat

object CustomerUtills {

//  val stringColumn = org.apache.spark.sql.types.StringType


  def readFile(spark: SparkSession, path: String, formate: String, schema: StructType, header: Boolean, filtercolumn:Option[String] = None,filtercond:Option[String] = None): DataFrame = {

    try {

      val filteredDF = (filtercolumn, filtercond) match {
        case (Some(filtercolumn), Some(filtercond)) =>
          val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
          val dateParam = dateFormat.parse(filtercond)
            println(s"dateparam is $dateParam")
          val df = spark.read.format(formate).schema(schema)
            .option("header", header)
            .load(path)
            .withColumn("order_purchase_timestamp", to_date(split(col("order_purchase_timestamp"), " ").getItem(0), "M/dd/yyyy"))
            .filter(col("order_purchase_timestamp") > to_date(lit(filtercond)))
          df
        case (None, None) =>
          val df = spark.read.format(formate).schema(schema)
            .option("header", header)
            .load(path)
          df
      }

      if (filteredDF.isEmpty) {
        println("The DataFrame is empty.")
        spark.emptyDataFrame
      } else {
        filteredDF
      }
    } catch {
      case ex: java.io.FileNotFoundException =>
        println(s"FileNotFoundException: ${ex.getMessage}")
        spark.emptyDataFrame
      case ex: Exception =>
        println(s"General Exception: ${ex.getMessage}")
        spark.emptyDataFrame
    }


  }

  def readFile(spark: SparkSession, path: String,formate:String): DataFrame = {

    try {

      val df = spark.read.format(formate).load(path)

      if (df.isEmpty) {
        println("The DataFrame is empty.")
        spark.emptyDataFrame
      } else {
        df
      }
    } catch {
      case ex: java.io.FileNotFoundException =>
        println(s"FileNotFoundException: ${ex.getMessage}")
        spark.emptyDataFrame
      case ex: Exception =>
        println(s"General Exception: ${ex.getMessage}")
        spark.emptyDataFrame
    }


  }

  def writeFile(data:DataFrame,path:String,mode:String,formate:String)={

    try {
      if (data.isEmpty) {
        println("The DataFrame is empty.")
      } else {
        //data.write.format("parquet").save(path)
        if (formate == "csv"){
          data.write.mode(mode).option("header",true).format(formate).save(path)
          println("Data written successfully.")
        }
        if (formate == "parquet") {
          data.write.mode("overwrite").format(formate).save(path)
          println("Data written successfully.")
        }

      }

    } catch {
      case ex: java.io.IOException =>
        println(s"IOException: ${ex.getMessage}")
      case ex: Exception =>
        println(s"General Exception: ${ex.getMessage}")
    }
  }

  def replaceStringNull(data:DataFrame):DataFrame={
    val stringColumns = data.columns.filter(
      (colm)=>
        data.schema(colm).dataType == org.apache.spark.sql.types.StringType
    )

    println("String columns : "+stringColumns.mkString(","))

    val df = stringColumns.foldLeft(data)((df, colName) =>
//      df.withColumn(colName,when(col(colName) === null,"hello").otherwise(col(colName)))
      df.na.replace(colName,Map("null" ->"Hello"))
    )
    df
  }

  def trimColumns(data:DataFrame):DataFrame={
    val df = data.columns.foldLeft(data){
      (newdf,colmn)=>
          newdf.withColumn(colmn,trim(col(colmn)))
    }
    df
  }

}


