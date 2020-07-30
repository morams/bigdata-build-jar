package com.amwater.waterone.cloudseer.streaming.pipeline

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.{DateType, StringType, StructType}

class BusinessLocks(sparkSession:SparkSession) {
    def ingestData(brokers:String,dbUrl:String,dbProps:Properties): Unit ={


      //read offsets from db
      val offset_qry = s"""(SELECT fast_load.get_kafka_topic_offset('businessLocks_v1'))as kafkaOffsets""".stripMargin
      val df = sparkSession.read.jdbc(dbUrl,table=offset_qry,dbProps)
      df.show()
      val offset_json = df.first().get(0).toString
      println(offset_json)

      //read kafka topic
      val raw_businessLock = sparkSession.read
        .format("kafka")
        .option("kafka.bootstrap.servers",brokers)
        .option("subscribe","businessLocks_v1")
        //.option("startingOffsets", """{"businessLocks_v1":{"0":1225,"1":1225,"2":1225}}""")
        .option("startingOffsets", offset_json)
        .option("endingOffsets", """{"businessLocks_v1":{"0":-1,"1":-1,"2":-1}}""")
        .option("maxOffsetsPerTrigger",100)
        .option("failOnDataLoss",false)
        .load()

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

      println(schema_bl)

      import sparkSession.implicits._
      val bl_raw_df = raw_businessLock.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)","topic","partition","offset","timestamp")
        .as[(String, String,String,Int,Long,Long)]
      bl_raw_df.printSchema()
      bl_raw_df.show(10)
      val bl_formatted_df = bl_raw_df.select(from_json($"value",schema_bl).as("businessLock"),$"topic",$"partition",$"offset",$"timestamp")
      bl_formatted_df.printSchema()
      bl_formatted_df.show(20)
      bl_formatted_df.createOrReplaceTempView("businessLock_v1")
      val result_bl = sparkSession.sql(
        """SELECT
          |businessLock.SAPClient as sap_client
          |,businessLock.LockObject as lock_object
          |,businessLock.LockObjectCategory as lock_object_category
          |,businessLock.ProcessCode as process_code
          |,businessLock.LockReason as lock_reason
          |,businessLock.ValidFrom as valid_from
          |,businessLock.ValidTo as valid_to
          |,businessLock.BusinessPartner as business_partner_number
          |,businessLock.ContractAccount as connection_contract_number
          |,businessLock.LockObjectCategoryName as lock_object_category_name
          |,businessLock.ProcessCodeName as process_code_name
          |,businessLock.LockReasonName as lock_reason_name
          |,businessLock.ChangedBy as changed_by
          |,businessLock.ChangedOn as changed_on
          |,businessLock.ChangedAt as changed_at
          |,businessLock.DateID as date_id
          |,businessLock.Identification as identification
          |,businessLock.Count as record_count
          |,businessLock.DFKKLOCKS_DF_TS as DFKKLOCKS_DF_TS
          |,businessLock.DFKKLOCKS_DF_DI as DFKKLOCKS_DF_DI
          |,topic as kafka_topic
          |,partition as kafka_partition
          |,offset as kafka_offset
          |,timestamp as kafka_record_timestamp
          |,current_timestamp as last_updated
          |FROM businessLock_v1""".stripMargin)
      result_bl.write.mode("append").jdbc(dbUrl,"fast_load.business_locks",dbProps)
      /*val partial_data = bl_formatted_df.selectExpr("businessLock_v1.SAPClient", "businessLock_v1.BusinessPartner", "businessLock_v1.ContractAccount")
      partial_data.show(20)*/
      val update_offset_qry = s"""(SELECT fast_load.update_kafka_topic_offset('businessLocks_v1')) as kafkaOffsets""".stripMargin
      val offset_df = sparkSession.read.jdbc(dbUrl,table=update_offset_qry,dbProps)
      offset_df.show()
      val merge_data_query = s"""(SELECT fast_load.upsert_business_locks_stream())as businessLocksUpsert""".stripMargin
      val merge_data_result = sparkSession.read.jdbc(dbUrl,table=merge_data_query,dbProps)
      merge_data_result.show()
      sparkSession.stop()
    }
}
