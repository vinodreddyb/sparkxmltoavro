package com.vinod.spark.jobs

import com.vinod.spark.Conversion
import com.vinod.spark.LogHolder

object XmlToAvroDriver {
  
  def main(args: Array[String]): Unit = {
      
    if(args.length < 4) {
      LogHolder.log.error("Please supply required arguments <input_path> <output_path> <xml_row_tag> <jaxb generated classname>")
      return
    }
    Conversion.convertAndSaveAvro(args(0), args(1),args(2), args(3))
    
    /*
    Conversion.convertAndSaveAvro1("input ",
      "test","MobileSubscriptionServicesAndCustomer", "MobileSubscriptionServicesAndCustomer")
*/   
  }
}