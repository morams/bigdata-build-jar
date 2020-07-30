package com.amwater.waterone.cloudseer.streaming.poc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.{DateType, StringType, StructType}

object BusinessLockStream {

  def main(args:Array[String]): Unit ={
    println("Business lock example ... Kafka reading example ...")

    if (args.length < 1) {
      System.err.println(s"""
                            |Usage: SparkKafkaTest <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |
        """.stripMargin)
      System.exit(1)
    }
    val brokers = args(0)
    //Spark Session
    val sparkSession = SparkSession.builder().appName("Cloudseer-Streaming").getOrCreate()
    val df = sparkSession.read
      .format("kafka")
      .option("kafka.bootstrap.servers",brokers)
      .option("subscribe","businessLocks_v1")
      .option("startingOffsets", """{"businessLocks_v1":{"0":1225,"1":1225,"2":1225}}""")
      .option("endingOffsets", """{"businessLocks_v1":{"0":-1,"1":-1,"2":-1}}""")
      .option("failOnDataLoss",false)
      .load()
    //{"SAPClient":"100","LockObject":"2450032561510000002000","LockObjectCategory":"02","ProcessCode":"10","LockReason":"W","ValidFrom":"2020-02-03","ValidTo":"2020-02-17","BusinessPartner":1100035766,"ContractAccount":220010466753,"LockObjectCategoryName":"Line Item",
    // "ProcessCodeName":"Payments","LockReasonName":"Outgoing Payment lock (refund approved)","ChangedBy":"","ChangedOn":"1900-01-01","ChangedAt":"000000","DateID":"00000000","Identification":"","Count":"1",
    // "DFKKLOCKS_DF_TS":"20200203175314","DFKKLOCKS_DF_DI":"D"}

    val schema_bl = new StructType()
      .add("SAPClient", StringType)
      .add("LockObject", StringType)
      .add("LockObjectCategory", StringType)
      .add("ProcessCode",StringType)
      .add("LockReason",StringType)
      .add("ValidFrom",DateType)
      .add("ValidTo",DateType).add("BusinessPartner",StringType).add("ContractAccount",StringType).add("LockObjectCategoryName",StringType)
      .add("ProcessCodeName",StringType).add("LockReasonName",StringType).add("ChangedBy",StringType)
      .add("ChangedOn",DateType).add("ChangedAt",StringType).add("DateID",StringType).add("Identification",StringType).add("Count",StringType)
      .add("DFKKLOCKS_DF_TS",StringType).add("DFKKLOCKS_DF_DI",StringType)

    import sparkSession.implicits._
    val bl_raw_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    bl_raw_df.printSchema()
    bl_raw_df.show(100)
    val bl_formatted_df = bl_raw_df.select(from_json($"value",schema_bl).as("businessLock_v1"))
    bl_formatted_df.printSchema()
    bl_formatted_df.show(20)
    val partial_data = bl_formatted_df.selectExpr("businessLock_v1.SAPClient", "businessLock_v1.BusinessPartner", "businessLock_v1.ContractAccount")
    partial_data.show(20)

    //from_json($"value, schema_bl).as("businessLock_v1"))

    sparkSession.stop()
  }

}
