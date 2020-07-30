package com.amwater.waterone.cloudseer.streaming.core

  import java.io.{File, FileNotFoundException}

  import com.amazonaws.{AmazonClientException, AmazonServiceException}
  import com.amazonaws.services.s3.AmazonS3Client
  import com.typesafe.config.{Config, ConfigFactory}

  import scala.io.{BufferedSource, Source}


  object ConfigFileReader {

   def readS3ObjectAsString(filePath: String):String = {
      val  s3Client = new AmazonS3Client
      try {
        val (bucketName,keyName) = CommandLineHelper.getS3PathBucketKey(filePath)
        val s3Obj = s3Client.getObject(bucketName, keyName)
        val source: BufferedSource = Source.fromInputStream(s3Obj.getObjectContent)
        source.mkString
      }catch {
        case ase: AmazonServiceException => {
          System.err.println("Exception: " + ase.toString)
          null
        }
        case ace: AmazonClientException => {
          System.err.println("Exception: " + ace.toString)
          null
        }
      }
    }


    def readFileObjectAsString(filePath:String) : String = {
      try {
        if (!(new File(filePath).exists)){
          throw new FileNotFoundException()
        }else {
          // val config = ConfigFactory.parseFile(new File(fileName))
          val fileContents = Source.fromFile(filePath).mkString
          fileContents
        }
      } catch {
        case fne: FileNotFoundException => {
          System.err.println("Exception: " + fne.toString)
          null
        }
      }
    }

}