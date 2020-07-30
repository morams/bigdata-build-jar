package com.amwater.waterone.cloudseer.streaming.poc

import org.apache.spark.rdd.RDD

class MyMapper(n:Int) extends Serializable {
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("myLogger")

  def mapWithLogging(rdd:RDD[Int]): RDD[String] ={
      rdd.map{ i =>
          log.warn("mapping: " + i)
          (i + n).toString
      }

    }
}

object MyMapper {
  def apply(n: Int): MyMapper = new MyMapper(n)
}
