package com.amwater.waterone.cloudseer.streaming.core

import org.apache.log4j.LogManager

object CommandLineHelper {
  val log = LogManager.getRootLogger

  def getOpts(args:Array[String],usage:String): collection.mutable.Map[String,String] ={
     if(args.length == 0){
       log.warn(usage)
       System.exit(1)
     }
     val (opts,vals) = args.partition{
       _.startsWith("-")
     }
    val optsMap = collection.mutable.Map[String,String]()
    opts.map { x =>
      val pair = x.split("=")
      if (pair.length == 2) {
        optsMap += (pair(0).split("-{1,2}")(1) -> pair(1))
      } else {
        log.warn(usage)
        System.exit(1)
      }
    }
      optsMap
    }

  def getS3PathBucketKey(filePath:String): (String,String) ={
      val splitPart1 = filePath.split("//",2)
      println(splitPart1(1))
      val splitPart2 = splitPart1(1).split("/",2)
      val (bucket,key) = (splitPart2(0),splitPart2(1))
      println(bucket)
      println(key)
      (bucket,key)
  }
}
