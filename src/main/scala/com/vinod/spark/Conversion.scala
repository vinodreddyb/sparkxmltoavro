package com.vinod.spark

import java.io.StringReader

import scala.Left
import scala.Right
import scala.reflect.ClassTag
import scala.reflect.classTag
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import org.apache.avro.file.DataFileConstants
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroJob
import org.apache.avro.mapreduce.{ AvroKeyOutputFormat => AvroKeyOutputFormat }
import org.apache.avro.reflect.ReflectData
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.SQLContext

import com.vinod.spark.xml.XmlInputFormat

import javax.xml.bind.JAXBContext
import javax.xml.bind.JAXBElement
import javax.xml.bind.JAXBException
import org.apache.avro.generic.GenericRecord

/**
 * This object parse the xml and gives RDD of avro models
 */
object Conversion {
    
  def getSpakContext(appName: String) = {
    val conf = new SparkConf().setAppName(appName)
  
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    sc.hadoopConfiguration.set("mapreduce.output.fileoutputformat.compress", "true")
    sc.hadoopConfiguration.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec")
    sc.hadoopConfiguration.set(AvroJob.CONF_OUTPUT_CODEC, DataFileConstants.SNAPPY_CODEC)
    sc
  }


  
 /**
   * This method  convert xml file to avro file and saves the avro file to output path
   * @param inputPath  hdfs input of the xml data
   * @param outputPath hdfs outpath to store avro files
   * @param rowTag xml start tag
   * 
   */
 def convertAndSaveAvro[T: ClassTag](inputPath: String, outputPath: String,  rowTag: String) {
    @transient val conf = new Configuration()
    conf.set(XmlInputFormat.START_TAG_KEY, s"<$rowTag")
    conf.set(XmlInputFormat.END_TAG_KEY, s"</$rowTag>")
    
    val sc = getSpakContext("XmlConverion")
    val xmlRDD = sc.newAPIHadoopFile(inputPath,classOf[XmlInputFormat], classOf[org.apache.hadoop.io.LongWritable], classOf[org.apache.hadoop.io.Text],conf)
    val rowRDD = xmlRDD.mapPartitions(partiton => {
    val jaxbContext = JAXBContext.newInstance(classTag[T].runtimeClass)
    val jaxbUnmarshaller = jaxbContext.createUnmarshaller()
     jaxbUnmarshaller.setEventHandler(new javax.xml.bind.helpers.DefaultValidationEventHandler());
      partiton.flatMap {
        xml =>
          try { 
            Option(jaxbUnmarshaller.unmarshal(new StringReader(xml._2.toString())).asInstanceOf[T] )
          } catch {
            case e : JAXBException => 
              LogHolder.log.error("Exception occured for : " + xml)
              None
          }
         
      }
    }).cache()
    val reflectData = ReflectData.AllowNull.get();
    val schema = reflectData.getSchema(classTag[T].runtimeClass);
    val job = Job.getInstance(sc.hadoopConfiguration)
    AvroJob.setOutputKeySchema(job,  schema)
    val avrordd = rowRDD.filter(_ != None).map(x => (new AvroKey(x),NullWritable.get()))
    avrordd.saveAsNewAPIHadoopFile(outputPath, classTag[T].runtimeClass,
        classOf[org.apache.hadoop.io.NullWritable],
        classOf[AvroKeyOutputFormat[T]],
        job.getConfiguration)
   
  }
  
  /**
   * This method  convert xml file to avro file and saves the avro file to output path
   * @param inputPath  hdfs input of the xml data
   * @param outputPath hdfs outpath to store avro files
   * @param rowTag xml start tag
   * @param className jaxb xml class name which is generated from xsd to java bindings
   * 
   */
  def convertAndSaveAvro(inputPath: String, outputPath: String,  rowTag: String, className : String) {
    
    val sc = getSpakContext("XmlConverion")
   
    @transient val conf = new Configuration()
    conf.set(XmlInputFormat.START_TAG_KEY, s"<$rowTag")
    conf.set(XmlInputFormat.END_TAG_KEY, s"</$rowTag>")
 
    val klaz = Class.forName(className)
    val xmlRDD = sc.newAPIHadoopFile(inputPath,classOf[XmlInputFormat], classOf[org.apache.hadoop.io.LongWritable], classOf[org.apache.hadoop.io.Text],conf)
    val rowRDD = xmlRDD.mapPartitions(partiton => {
      val jaxbContext = JAXBContext.newInstance(klaz)
      val jaxbUnmarshaller = jaxbContext.createUnmarshaller()
     // jaxbUnmarshaller.setEventHandler(new javax.xml.bind.helpers.DefaultValidationEventHandler());
      partiton.map {
        xml =>
          try {
             Option(jaxbUnmarshaller.unmarshal(new StringReader(xml._2.toString())))
          } catch {
            case e : JAXBException =>
              LogHolder.log.error("Exception occured for : " + xml)
              None
          }

      }
    }).cache()
    val reflectData = ReflectData.AllowNull.get();
    val schema = reflectData.getSchema(klaz);
   
    val job = Job.getInstance(sc.hadoopConfiguration)
    AvroJob.setOutputKeySchema(job,  schema)
    
    val avrordd = rowRDD.filter(_ != None).map(x => (new AvroKey(x.getOrElse(klaz.newInstance())),NullWritable.get()))
    avrordd.saveAsNewAPIHadoopFile(outputPath, klaz,
      classOf[org.apache.hadoop.io.NullWritable],
      classOf[AvroKeyOutputFormat[GenericRecord]],
      job.getConfiguration)

  }
  

  
}

