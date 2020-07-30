package com.amwater.waterone.cloudseer.streaming.poc

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SparkSession

object AppLog {
      def main(args: Array[String]) {
        val log = LogManager.getRootLogger
        log.setLevel(Level.WARN)

        val spark = SparkSession.builder().master("local").appName("demo-app").getOrCreate()

        log.warn("Hello demo")

        val data = spark.sparkContext.parallelize(1 to 100000)
        println("something to print in console")
        val mapper = MyMapper(1)
        val result = mapper.mapWithLogging(data)
        /*data.map { value =>
          log.info(value)
          value.toString
        }*/
        result.collect()
        log.warn("I am done")
      }
}
