package com.amwater.waterone.cloudseer.streaming.controller

import java.util.Properties

import com.amwater.waterone.cloudseer.streaming.core.{CommandLineHelper, ConfigFileReader}
import com.amwater.waterone.cloudseer.streaming.core.ObjectHelper.AppConfig
import com.amwater.waterone.cloudseer.streaming.pipeline.{BusinessLocks, CallType, RouteCallDetail}
//import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.sql.SparkSession
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import scala.annotation.switch



object PipelineController {



  def main(args:Array[String]): Unit ={
    println("Streaming application started ...")
    try {
      val usage = "Usage: [--runEnv] [--pipelineName] [--configFilePath]"
      val optsMap = CommandLineHelper.getOpts(args, usage)
      val runEnv = optsMap("runEnv")
      val pipelineName = optsMap("pipelineName")
      //val brokersList = optsMap("brokers")
      val configFilePath = optsMap("configFilePath")
      println(pipelineName)
      //println(brokersList)
      println(runEnv)
      println(configFilePath)
      //(fileType,filePath)
      val fileType = configFilePath match {
        case f if(!f.isEmpty && f.startsWith("s3")) => {
          "s3"
        }
        case _ => {
          "local"
        }
      }



      println(fileType)
      println("case!!")
      val configFileContents = fileType match {
        case "s3" => ConfigFileReader.readS3ObjectAsString(configFilePath)
        case "local" => ConfigFileReader.readFileObjectAsString(configFilePath)
      }

      println("reading spark!!")

      implicit val formats = DefaultFormats
      val jsonConfig = parse(configFileContents)
      println(jsonConfig)
      val cObject = jsonConfig.extract[AppConfig]
      println(cObject)
      println("reading spark!!!!!!!!")


      //reading spark session
      val sparkSession = if (runEnv.equalsIgnoreCase("local")) {
        SparkSession.builder().master("local").appName(pipelineName).getOrCreate()
      } else {
        SparkSession.builder().appName(pipelineName).getOrCreate()
      }
      println(sparkSession)
      val props = new Properties()
      props.setProperty("user", cObject.pSql.dbUser)
      props.setProperty("password", cObject.pSql.dbPassword)
      props.setProperty("driver", cObject.pSql.dbDriver)
      val dbUrl = cObject.pSql.dbUrl
      //props.setProperty("user", "postgres")
      //props.setProperty("password", "vjedqz9D6kd")
      //props.setProperty("driver", "org.postgresql.Driver")
      //val dbUrl = "jdbc:postgresql://datards-databasecluster-1adhht6s9gxrh.cluster-cmnvuxhrrcdg.us-east-1.rds.amazonaws.com:3306/ao_aw_cloudseer3"
      println(props)

      (pipelineName: @switch) match {
        case "businessLocks" => {
          val businessLock = new BusinessLocks(sparkSession)
          businessLock.ingestData(cObject.kafkaBrokers,dbUrl,props)
        }
        case "callType" => {
          val callType = new CallType(sparkSession)
          callType.ingestData(cObject.kafkaBrokers,dbUrl,props)
        }
        case "routeCallDetail" => {
          val routeCallDetail = new RouteCallDetail(sparkSession)
          routeCallDetail.ingestData(cObject.kafkaBrokers,dbUrl,props)
        }

      }
    }catch {
      case ie @ (_ : RuntimeException | _ : java.io.IOException | _ : Exception ) => {
        System.err.println(ie)
        System.exit(1)

      }
    }
  }
}
