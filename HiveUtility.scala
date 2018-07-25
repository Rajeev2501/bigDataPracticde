package com.att.contentintelligence

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
//import com.att.contentintelligence.util.genericUtilities
object HiveUtility {

  /*
   * Method to read data from a hive table
   */

  def readHiveTable(spark: SparkSession, schd_date: String, query: String): DataFrame = {

    println("The query to fetch data from the input table -> " + query)
    import spark.sql
    val inputTbl = sql(query)
    println("The schema of the input dataframe ")
    //prints the schema of the dataframe created after reading the table's data into dataframe
    inputTbl.printSchema()
    println("The data -> ")
    //prints the dataframe content
    inputTbl.show()
    inputTbl
  }
  
  /*
   * Method to write data directly to hive table
   */

  def writeIntoHive(spark: SparkSession, res1: DataFrame, targetDB: String, targetTbl: String, schd_date: String): Unit = {
    
    import spark.sql
    
    /*
     * for setting the dynamic partition mode=nonstrict
     * Only required when data is written to the partitioned table 
     */
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    
    //udf created to be used in the query 
    spark.udf.register("DateDiff", (AIR_START_TM: String, AIR_END_TM: String) => ((AIR_START_TM.toLong - AIR_END_TM.toLong).abs.toString()))
    
    res1.createOrReplaceTempView("inputView")

    println("Data written successfully to the hive table")
    val outPutView = {
      val extracted = "select chnl_obj_id,tms_id as src_tms_id,chnl_program_id,air_start_tm,air_end_tm,DateDiff(air_end_tm,air_start_tm) as runtime_dur_in_sec,is_live_program,is_repeat_program,is_season_final,is_season_premier,is_series_final,is_series_premier,'" + schd_date + "' as sched_dy_key,'20180717' as data_dt from inputView"
      //val extracted = s"""select chnl_obj_id,tms_id as src_tms_id,chnl_program_id,air_start_tm,air_end_tm,DateDiff(air_end_tm,air_start_tm) as runtime_dur_in_sec,is_live_program,is_repeat_program,is_season_final,is_season_premier,is_series_final,is_series_premier,'$schd_date' as sched_dy_key,'20180717' as data_dt from inputView"""
      println("The extracted query -> " + extracted)
      /* 
       * creating the outputView by querying the input table and 
       * Same will be written to the hive table
       */
      
      spark.sql(extracted).createOrReplaceTempView("outPutview")
    }
    sql("insert into " + targetDB + "." + targetTbl + " select * from outPutview")
    println("Data written to hive successfully")

  }
 }