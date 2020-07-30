package com.amwater.waterone.cloudseer.streaming.core

object ObjectHelper {

  case class PSql(dbUrl:String,dbUser:String,dbPassword:String,dbDriver:String,dbWriteMode:String)
  case class AppConfig(name:String,pSql: PSql,kafkaBrokers:String)

}
