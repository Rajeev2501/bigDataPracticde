package com.att.contentintelligence


import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Locale

object DateFormatter {
  
  def schdDateFormatter(scheduled_date: String): String = {
    val dateFormat = new SimpleDateFormat("yyyyMMdd", Locale.ENGLISH);
    val schdDayFormat = dateFormat.parse(scheduled_date);
    var cal = Calendar.getInstance()
    cal.setTime(schdDayFormat)
    cal.add(Calendar.DATE, -1);
    var todate1 = cal.getTime();
    dateFormat.format(todate1).toString()
  }
}