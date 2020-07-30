package com.amwater.waterone.cloudseer.streaming.poc

import com.amwater.waterone.cloudseer.streaming.core.CommandLineHelper

object ArgsTest {
      def main(args:Array[String]): Unit ={
        println("Command line args parser ...")
        val usage = "Usage: [--citys] [--num]"
        val optsMap = CommandLineHelper.getOpts(args,usage)
        val citysValue = optsMap("citys")
        val numValue = optsMap.getOrElse("num","5")
        println(citysValue)
        println(numValue)
      }
}
