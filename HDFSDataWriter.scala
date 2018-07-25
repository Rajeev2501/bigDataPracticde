package com.att.contentintelligence
import org.apache.spark.sql.DataFrame

object HDFSDataWriter {
  
  def writeToHDFS(df:DataFrame,partition_column:String,hdfsPath:String):Unit={
    
    /*
     * To write the dataframe to hdfs partition wise.
     */
    df.write.mode("append").partitionBy(partition_column).format("csv").save(hdfsPath)
    /*
     * if you have partition data on multiple columns 
     * pass the columns after giving comma(,)
     * ex:  df.write.mode("append").partitionBy(partition_column,partition_column1).format("csv").save(hdfsPath)
     */
  }
}