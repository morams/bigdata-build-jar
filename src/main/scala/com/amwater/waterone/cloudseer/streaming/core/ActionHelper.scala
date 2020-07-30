package com.amwater.waterone.cloudseer.streaming.core
import ObjectHelper.AppConfig
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}

object ActionHelper {
  def getCount(df:DataFrame) : Long = {
    val count = df.count()
    count
  }

  def writeJDBC(df:DataFrame,tName:String,appConfig:AppConfig) = {
    //dbUrl:String,dbProperties:Properties,writeMode:String
    try {
      val dbProperties = new java.util.Properties
      dbProperties.setProperty("user", appConfig.pSql.dbUser)
      dbProperties.setProperty("password", appConfig.pSql.dbPassword)
      dbProperties.setProperty("driver", appConfig.pSql.dbDriver)
      df.write.mode(appConfig.pSql.dbWriteMode).jdbc(appConfig.pSql.dbUrl, tName, dbProperties)
    }catch{
      case e: Exception => e.printStackTrace()
    }
  }

  def readFromJDBC(sparkSession: SparkSession,qryText:String,appConfig: AppConfig):DataFrame = {

    val dbProperties = new java.util.Properties
    dbProperties.setProperty("user", appConfig.pSql.dbUser)
    dbProperties.setProperty("password", appConfig.pSql.dbPassword)
    dbProperties.setProperty("driver", appConfig.pSql.dbDriver)
    val df = sparkSession.sqlContext.read.jdbc(appConfig.pSql.dbUrl,table=qryText,dbProperties)
    df
  }

  def dedupDataframe(sparkSession: SparkSession,dfFrom: DataFrame, filterCols:List[Column], parCols:List[Column],newColName:String):DataFrame = {
    import sparkSession.implicits._
    val updated_df = dfFrom.select("*").withColumn(newColName,filterCols.reduce(_ + _))
    val windowFunc = Window.partitionBy(parCols:_*).orderBy(col(newColName).desc)
    val deduped_df = updated_df.select("*").withColumn("rn",(row_number.over(windowFunc))).where($"rn" === 1)
    val unique_df = deduped_df.drop("rn")
    unique_df
  }
}
