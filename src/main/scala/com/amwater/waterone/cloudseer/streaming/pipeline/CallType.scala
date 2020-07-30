package com.amwater.waterone.cloudseer.streaming.pipeline
import com.amwater.waterone.cloudseer.streaming.core.ActionHelper
import java.util.Properties

import org.apache.spark.sql.catalyst.ScalaReflection
import com.amwater.waterone.cloudseer.streaming.core.ObjectHelper.AppConfig
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.{DateType, StringType, StructType}



class CallType(sparkSession:SparkSession) {

  case class CallTypeSchema(DateTime: String,
                            CallTypeID: String,
                            TimeZone: String,
                            RecoveryKey: String,
                            RouterQueueWaitTime: String,
                            RouterQueueCalls: String,
                            AvgRouterDelayQ: String,
                            RouterCallsAbandQ: String,
                            RouterQueueCallTypeLimit: String,
                            RouterQueueGlobalLimit: String,
                            CallsRouted: String,
                            ErrorCount: String,
                            ICRDefaultRouted: String,
                            NetworkDefaultRouted: String,
                            ReturnBusy: String,
                            ReturnRing: String,
                            NetworkAnnouncement: String,
                            AnswerWaitTime: String,
                            CallsHandled: String,
                            CallsOffered: String,
                            HandleTime: String,
                            ServiceLevelAband: String,
                            ServiceLevelCalls: String,
                            ServiceLevelCallsOffered: String,
                            ServiceLevel: String,
                            TalkTime: String,
                            OverflowOut: String,
                            HoldTime: String,
                            IncompleteCalls: String,
                            ShortCalls: String,
                            DelayQAbandTime: String,
                            CallsAnswered: String,
                            CallsRoutedNonAgent: String,
                            CallsRONA: String,
                            ReturnRelease: String,
                            CallsQHandled: String,
                            VruUnhandledCalls: String,
                            VruHandledCalls: String,
                            VruAssistedCalls: String,
                            VruOptOutUnhandledCalls: String,
                            VruScriptedXferredCalls: String,
                            VruForcedXferredCalls: String,
                            VruOtherCalls: String,
                            ServiceLevelType: String,
                            BucketIntervalID: String,
                            AnsInterval1: String,
                            AnsInterval2: String,
                            AnsInterval3: String,
                            AnsInterval4: String,
                            AnsInterval5: String,
                            AnsInterval6: String,
                            AnsInterval7: String,
                            AnsInterval8: String,
                            AnsInterval9: String,
                            AnsInterval10: String,
                            AbandInterval1: String,
                            AbandInterval2: String,
                            AbandInterval3: String,
                            AbandInterval4: String,
                            AbandInterval5: String,
                            AbandInterval6: String,
                            AbandInterval7: String,
                            AbandInterval8: String,
                            AbandInterval9: String,
                            AbandInterval10: String,
                            CallsRequeried: String,
                            DbDateTime: String,
                            RouterCallsAbandToAgent: String,
                            TotalCallsAband: String,
                            DelayAgentAbandTime: String,
                            CallDelayAbandTime: String,
                            CTDelayAbandTime: String,
                            ServiceLevelError: String,
                            ServiceLevelRONA: String,
                            AgentErrorCount: String,
                            VRUTime: String,
                            CTVRUTime: String,
                            Reserved1: String,
                            Reserved2: String,
                            Reserved3: String,
                            Reserved4: String,
                            Reserved5: String,
                            CallsOnHold: String,
                            MaxHoldTime: String,
                            ReportingHalfHour: String,
                            ReportingInterval: String,
                            MaxCallWaitTime: String,
                            MaxCallsQueued: String,
                            ReservationCalls: String,
                            rnum: String
                           )

  def ingestData(brokers: String, dbUrl: String, dbProps: Properties): Unit = {

    import java.time.Instant
    val unixTimestamp = Instant.now.getEpochSecond
    val load_name = "callTypeIntervalDataIVRRefresh"
    val load_type = "Kafka"
    val load_key = s"""${unixTimestamp}_${load_name}""".stripMargin

    print("CallType Started!!!!!")

    //read offsets from db
    val offset_qry = s"""(SELECT fast_load.get_kafka_topic_offset('callTypeInterval'))as kafkaOffsets""".stripMargin
    val df = sparkSession.read.jdbc(dbUrl, table = offset_qry, dbProps)
    df.show()
    val offset_json = df.first().get(0).toString
    println(offset_json)

    try {

      //read kafka topic
      val raw_callTypeInterval = sparkSession.read
        .format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", "callTypeInterval")
        //.option("startingOffsets", """{"callTypeInterval":{"0":1,"1":1,"2":1}}""")
        .option("startingOffsets", offset_json)
        .option("endingOffsets", """{"callTypeInterval":{"0":-1,"1":-1,"2":-1}}""")
        .option("maxOffsetsPerTrigger", 100)
        .option("failOnDataLoss", false)
        .load()


      val schema_ct = ScalaReflection.schemaFor[CallTypeSchema].dataType.asInstanceOf[StructType]
      println(schema_ct)


      import sparkSession.implicits._
      val bl_raw_df = raw_callTypeInterval.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition", "offset", "timestamp")
        .as[(String, String, String, Int, Long, Long)]

      val bl_formatted_df = bl_raw_df.select(from_json($"value",schema_ct).as("cti"),$"topic",$"partition",$"offset",$"timestamp")
      bl_formatted_df.printSchema()
      bl_formatted_df.show(10)

      //TO_DATE(CAST(UNIX_TIMESTAMP(CAST(cti.DateTime AS String), 'yyyy-MM-dd') AS TIMESTAMP)) AS datetime,
      bl_formatted_df.createOrReplaceTempView("callTypeInterval")


      val callTypeIntervalObj = sparkSession.sql(
        """SELECT
          |CAST(cti.DateTime AS TIMESTAMP) AS datetime,
          |CAST(cti.CallTypeID AS INT) AS call_type_id,
          |CAST(cti.RecoveryKey AS DOUBLE) AS recovery_key,
          |CAST(cti.OverflowOut AS INT) AS over_flow_out,
          |CAST(cti.RouterCallsAbandQ AS INT) AS router_calls_abandq,
          |CAST(cti.CallsRouted AS INT) AS calls_routed,
          |CAST(cti.CallsHandled AS INT) + CAST(cti.TotalCallsAband AS INT) as outflow_to_queue,
          |CAST(cti.CallsOffered AS INT) AS callsoffered,
          |CAST(cti.CallsAnswered AS INT) AS callsanswered,
          |CAST(cti.AnswerWaitTime AS INT) AS answerwaittime,
          |CAST(cti.HandleTime AS INT) AS handletime,
          |CAST(cti.CallsHandled AS INT) AS callshandled,
          |CAST(cti.MaxCallWaitTime AS INT) AS maxcallwaittime,
          |CAST(cti.TotalCallsAband AS INT) AS total_calls_abandoned,
          |'bluesky' as interval_type,
          |CAST(cti.ServiceLevelCalls AS INT) as service_level_calls,
          |topic as kafka_topic,
          |CAST(partition as INT) as kafka_partition,
          |CAST(offset as LONG) as kafka_offset,
          |CAST(timestamp as TIMESTAMP) as kafka_record_timestamp,
          |CAST(current_timestamp as TIMESTAMP) as last_updated
          |FROM callTypeInterval""".stripMargin)


      println("#########Kafka-RECORD-COUNT############")
      val sourceCount = ActionHelper.getCount(callTypeIntervalObj)

      callTypeIntervalObj.printSchema()
      callTypeIntervalObj.show(10)

      println(s"""Source count $sourceCount""")

      //Truncating the table before loading
      val ctdrCleanup_table = """(select delta_load.cloudseer_mso_cleanup('fast_load.call_type_interval')) as ctdrCleanup_table""".stripMargin

      val cleaned_df = sparkSession.read.jdbc(dbUrl, table = ctdrCleanup_table, dbProps)
      cleaned_df.show()

      //Appending it to Temp Table
      callTypeIntervalObj.write.mode("append").jdbc(dbUrl, "fast_load.call_type_interval", dbProps)



      //Stored Proc for Merge
      val merge_data_query = s"""(SELECT fast_load.call_type_interval_data_ivr_delta_refresh_stream())as callTypeIntervalUpsert""".stripMargin
      val merge_data_query1 = s"""(SELECT fast_load.call_type_interval_data_delta_refresh_stream())as callTypeIntervalUpsert1""".stripMargin
      val merge_data_result = sparkSession.read.jdbc(dbUrl, table = merge_data_query, dbProps)
      val merge_data_result1 = sparkSession.read.jdbc(dbUrl, table = merge_data_query1, dbProps)
      merge_data_result.show()
      merge_data_result1.show()

      //Update kafka Offset
      val update_offset_qry = s"""(SELECT fast_load.update_kafka_topic_offset_calltype('callTypeInterval'))as kafkaOffsets""".stripMargin
      val offset_df = sparkSession.read.jdbc(dbUrl, table = update_offset_qry, dbProps)
      offset_df.show()


    }catch
      {
        case ie@(_: RuntimeException | _: java.io.IOException | _: Exception) => {
          val errorDesc = ExceptionUtils.getStackTrace(ie)
          val logging_error_query_payments = s"""(SELECT delta_load.cloudseer_data_load_log_update('$load_key','$load_type','$load_name','ErrorProcess','error','$errorDesc')) as ctidrError_logging_query""".stripMargin

          val logging_error_status_payments = sparkSession.read.jdbc(dbUrl, table = logging_error_query_payments, dbProps)

          logging_error_status_payments.show()
        }



          sparkSession.stop()


      }

    }
}

