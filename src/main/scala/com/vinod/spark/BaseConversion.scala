package com.vinod.spark

import java.sql.Timestamp
import java.text.SimpleDateFormat
import scala.util.Try

trait BaseConversion {
   val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
   
   implicit class StringImprovements(val s: String) {
         import scala.util.control.Exception._
         def toLongOpt = catching(classOf[NumberFormatException]) opt s.toLong
   }
   
   /**
    * This method get the long value from JAXB generated lists
    * XSDs have complexTypes instead of simpleType and with empty option elements. So simple types like long,int and String are e generated like list[String] 
    */
   def extractLongValue(list: java.util.List[String]): Long = {
    if (list.size() > 0) {
      Try(list.get(0).toLong).getOrElse(0L)
    } else {
      0L
    }
  }
   
    /**
    * This method get the String value from JAXB generated lists
    * XSDs have complexTypes instead of simpleType and with empty option elements. So simple types like long,int and String are e generated like list[String] 
    */
   def extractStringValue(list: java.util.List[String]): String = {
    if (list.size() > 0) {
      list.get(0)
    } else {
      null
    }
  }
 /**
    * This method get the java.sql.Timestamp value from JAXB generated lists of String
    * XSDs have complexTypes instead of simpleType and with empty option elements. So simple types like long,int and String are e generated like list[String] 
    */
  def extractTimeStamp(list: java.util.List[String]): Timestamp = {
   try {
      if (list.size() > 0) {
      val parsedDate = dateFormat.parse(list.get(0))
      new Timestamp(parsedDate.getTime)
    } else {
      null
    }
    } catch {
      case excep: Exception => 
        //TODO: Log exeception
        null
    }
    
  }
}