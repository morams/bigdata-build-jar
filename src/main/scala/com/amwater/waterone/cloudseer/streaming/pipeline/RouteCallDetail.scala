package com.amwater.waterone.cloudseer.streaming.pipeline

import java.util.Properties

import com.amwater.waterone.cloudseer.streaming.core.ActionHelper
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.StructType

class RouteCallDetail(sparkSession:SparkSession) {
  case class RouteCallSchema(RecoveryKey: String,
                             DialedNumberID: String,
                             RouterCallKeyDay: String,
                             RouterCallKey: String,
                             RouteID: String,
                             DateTime: String,
                             RequestType: String,
  RoutingClientID: String,
  OriginatorType: String,
  Unused: String ,
  RoutingClientCallKey: String,
  Priority: String,
  MsgOrigin: String,
  Variable1: String ,
  Variable2: String ,
  Variable3: String ,
  Variable4: String ,
  Variable5: String ,
  UserToUser: String ,
  ANI: String ,
  CDPD: String ,
  CED: String ,
  ScriptID: String,
  FinalObjectID: String,
  CallSegmentTime: String,
  NetQTime: String,
  CallTypeID: String,
  RouterErrorCode: String,
  RecoveryDay: String,
  TimeZone: String,
  NetworkTargetID: String,
  LabelID: String,
  Originator: String ,
  Variable6: String ,
  Variable7: String ,
  Variable8: String ,
  Variable9: String ,
  Variable10: String ,
  TargetLabelID: String,
  RouterCallKeySequenceNumber: String,
  RouterQueueTime: String,
  VruScripts: String,
  Label: String ,
  TargetLabel: String ,
  DialedNumberString: String ,
  BeganRoutingDateTime: String ,
  BeganCallTypeDateTime: String ,
  TargetType: String,
  MRDomainID: String,
  RequeryResult: String,
  VruProgress: String,
  DbDateTime: String,
  ECCPayloadID: String,
  ContactShareRuleID: String,
  ContactShareQueueID: String,
  ContactShareGroupID: String,
  ApplicationGatewayID: String,
  ContactShareErrorCode: String,
  ContactShareResult: String)

  def ingestData(brokers: String, dbUrl: String, dbProps: Properties): Unit = {

    import java.time.Instant
    val unixTimestamp = Instant.now.getEpochSecond
    val load_name = "RouteCallDetail"
    val load_type = "Kafka"
    val load_key = s"""${unixTimestamp}_${load_name}""".stripMargin

    print("RouteCallDetail Started!!!!!")

    //read offsets from db
    val offset_qry = s"""(SELECT fast_load.get_kafka_topic_offset('routeCallDetail'))as kafkaOffsets""".stripMargin
    val df = sparkSession.read.jdbc(dbUrl, table = offset_qry, dbProps)
    df.show()
    val offset_json = df.first().get(0).toString
    println(offset_json)

    try {

      //read kafka topic
      val raw_routeCallDetail = sparkSession.read
        .format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", "routeCallDetail")
        //.option("startingOffsets", """{"routeCallDetail":{"0":1,"1":1,"2":1}}""")
        .option("startingOffsets", offset_json)
        .option("endingOffsets", """{"routeCallDetail":{"0":-1,"1":-1,"2":-1}}""")
        .option("maxOffsetsPerTrigger", 100)
        .option("failOnDataLoss", false)
        .load()


      println(raw_routeCallDetail)


      val schema_ct = ScalaReflection.schemaFor[RouteCallSchema].dataType.asInstanceOf[StructType]


      import sparkSession.implicits._
      val bl_raw_df = raw_routeCallDetail.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition", "offset", "timestamp")
        .as[(String, String, String, Int, Long, Long)]

      val bl_formatted_df = bl_raw_df.select(from_json($"value",schema_ct).as("rcd"),$"topic",$"partition",$"offset",$"timestamp")
      bl_formatted_df.printSchema()
      bl_formatted_df.show(20)
      //TO_DATE(CAST(UNIX_TIMESTAMP(CAST(cti.DateTime AS String), 'yyyy-MM-dd') AS TIMESTAMP)) AS datetime,
      bl_formatted_df.createOrReplaceTempView("routeCallDetail")
      val routeCallDetailObj = sparkSession.sql(
        """select CAST(rcd.recoverykey AS DOUBLE) as recovery_key
          |      ,CAST(rcd.routercallkeyday AS INT) as router_call_keyday
          |      ,CAST(rcd.routercallkey AS INT) as router_call_key
          |      ,CAST(rcd.DateTime AS TIMESTAMP) AS date_time
          |      ,CAST(rcd.requesttype AS INT) as request_type
          |      ,rcd.ani AS ani
          |      ,rcd.ced as ced
          |      ,CAST(rcd.calltypeid AS INT) as call_type_id
          |      ,rcd.variable6 as variable_6
          |      ,rcd.variable7 as variable_7
          |      ,rcd.variable8 as variable_8
          |      ,substring(rcd.variable8,5,2) as state
          |      ,case rcd.variable9 when 'MAKEPYMENT' then 'MAKEPAYMENT' else rcd.variable9 end as call_intent
          |      ,CAST(rcd.RouterCallKeySequenceNumber AS INT) as router_call_key_sequence_number
          |      ,rcd.dialednumberstring as dialed_number_string
          |      ,CAST(rcd.dbdatetime AS TIMESTAMP) AS db_date_time
          |      ,'bluesky' as interval_type,
          |      topic as kafka_topic,
          |CAST(partition as INT) as kafka_partition,
          |CAST(offset as LONG) as kafka_offset,
          |CAST(timestamp as TIMESTAMP) as kafka_record_timestamp,
          |CAST(current_timestamp as TIMESTAMP) as last_updated
          |  from routeCallDetail""".stripMargin)


      println("#########Kafka-RECORD-COUNT############")
      val sourceCount = ActionHelper.getCount(routeCallDetailObj)

      println(s"""Source count $sourceCount""")
      routeCallDetailObj.printSchema()
      routeCallDetailObj.show(5)



      //Truncating the table before loading
      val ctdrCleanup_table = """(select delta_load.cloudseer_mso_cleanup('fast_load.route_call_detail')) as ctdrCleanup_table""".stripMargin

      val cleaned_df = sparkSession.read.jdbc(dbUrl, table = ctdrCleanup_table, dbProps)
      cleaned_df.show()

      //Appending it to Temp Table
      routeCallDetailObj.write.mode("append").jdbc(dbUrl, "fast_load.route_call_detail", dbProps)



      //Stored Proc for Merge

      val merge_data_query = s"""(SELECT fast_load.cso_batch_individual_csc_call_log_delta_reload_stream())as routeCallDetailUpsert""".stripMargin
      val merge_data_query1 = s"""(SELECT fast_load.customer_drill_down_insert_data_stream())as routeCallDetailUpsert1""".stripMargin
      val merge_data_result = sparkSession.read.jdbc(dbUrl, table = merge_data_query, dbProps)
      val merge_data_result1 = sparkSession.read.jdbc(dbUrl, table = merge_data_query1, dbProps)
      merge_data_result.show()
      merge_data_result1.show()

      //updating kafka offset
      val update_offset_qry = s"""(SELECT fast_load.update_kafka_topic_offset_routeCallDetail('routeCallDetail'))as kafkaOffsets""".stripMargin
      val offset_df = sparkSession.read.jdbc(dbUrl, table = update_offset_qry, dbProps)
      offset_df.show()


    } catch {
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
