package com.att.contentintelligence


import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Locale

object DateFormatter {
  //expected input is in format yyyyMMdd
  
  def schdDateFormatter(scheduled_date: String): String = {
    val dateFormat = new SimpleDateFormat("yyyyMMdd", Locale.ENGLISH);
    val schdDayFormat = dateFormat.parse(scheduled_date);
    //instantiating Calendar object to be used further
    var cal = Calendar.getInstance()
    
    //setting the supplied time to the cal object 
    cal.setTime(schdDayFormat)
    
    //Reducing 1 day from the scheduled date
    cal.add(Calendar.DATE, -1);
    var todate1 = cal.getTime();
    dateFormat.format(todate1).toString()
    
    /*
    *input = 20160101 output = 20151231
    */
  }
}
