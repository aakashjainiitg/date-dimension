// Databricks notebook source
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ListBuffer

object date_time_utils {
  val default_format = "yyyy-MM-dd"

  def check(start_date: String, end_date: String): Boolean = {
    val start = LocalDate.parse(start_date, DateTimeFormatter.ofPattern(default_format))
    val end = LocalDate.parse(end_date, DateTimeFormatter.ofPattern(default_format))
    end.isAfter(start)
  }


  def convert_string_to_date(current_date: String, input_format: String, output_format: String) = {
    val input_formatter = DateTimeFormatter.ofPattern(input_format)
    val output_formatter = DateTimeFormatter.ofPattern(output_format)
    output_formatter.format(input_formatter.parse(current_date))
  }

  def get_day_suffix(current_date: String) = {
    val day = convert_string_to_date(current_date, default_format, "d").toInt
    day match {
      case 1 => "st"
      case 21 => "st"
      case 31 => "st"
      case 2 => "nd"
      case 22 => "nd"
      case 3 => "rd"
      case 23 => "rd"
      case _ => "th"
    }
  }

  def get_quater_name(current_date: String) = {
    val quater = convert_string_to_date(current_date, default_format, "Q").toInt
    quater match {
      case 1 => "Q1"
      case 2 => "Q2"
      case 3 => "Q3"
      case 4 => "Q4"
    }
  }

  def is_weekend(current_date: String) = {
    val week_day_name = convert_string_to_date(current_date, default_format, "EEEE")
    week_day_name match {
      case "Saturday" => 1
      case "Sunday" => 1
      case _ => 0
    }
  }

  def get_date_of_year(current_date: String, position: String) = {
    val year = convert_string_to_date(current_date, default_format, "u")
    position match {
      case "first" => year + "-01-01"
      case "last" => year + "-12-31"
    }
  }

  def get_first_date_of_quarter(current_date: String) = {
    val quater = convert_string_to_date(current_date, default_format, "QQ")
    val year = convert_string_to_date(current_date, default_format, "u")
    quater match {
      case "01" => year + "-01-01"
      case "02" => year + "-04-01"
      case "03" => year + "-07-01"
      case "04" => year + "-10-01"
    }
  }

  def get_last_date_of_quarter(current_date: String) = {
    val quater = convert_string_to_date(current_date, default_format, "QQ")
    val year = convert_string_to_date(current_date, default_format, "u")
    quater match {
      case "01" => year + "-03-31"
      case "02" => year + "-06-30"
      case "03" => year + "-09-30"
      case "04" => year + "-12-31"
    }
  }

  def get_first_date_of_month(current_date: String) = {
    val month = convert_string_to_date(current_date, default_format, "MM")
    val year = convert_string_to_date(current_date, default_format, "u")
    year + "-" + month + "-01"
  }

  def get_last_date_of_month(current_date: String) = {
    val converted_date = LocalDate.parse(current_date, DateTimeFormatter.ofPattern(default_format))
    val last_day_of_month = converted_date.withDayOfMonth(converted_date.getMonth.length(converted_date.isLeapYear))
    last_day_of_month.toString
  }

  def get_first_date_of_week(current_date: String) = {
    val converted_date = LocalDate.parse(current_date, DateTimeFormatter.ofPattern(default_format))
    val day_backward = convert_string_to_date(current_date, default_format, "e").toInt - 1
    converted_date.minusDays(day_backward).toString
  }

  def get_last_date_of_week(current_date: String) = {
    val converted_date = LocalDate.parse(current_date, DateTimeFormatter.ofPattern(default_format))
    val day_forward = 7 - convert_string_to_date(current_date, default_format, "e").toInt
    converted_date.plusDays(day_forward).toString
  }
  
  def get_last_12_month_list() = {
    var last_12_month_list = ListBuffer[String]()
    var i = 0
    for( i <- 1 to 12){
      last_12_month_list += DateTimeFormatter.ofPattern("yyyyMM").format(LocalDate.now.minusMonths(i))
    }
    last_12_month_list
  }
  
  
  def get_last_12_month_flag(yyyyMM: String) = {
    if (get_last_12_month_list().contains(yyyyMM)) 1 else 0
  }
  
  def get_last_6_month_flag(yyyyMM: String) = {    
    if (get_last_12_month_list().slice(0,6).contains(yyyyMM)) 1 else 0
  }
  
  def get_last_month_flag(yyyyMM: String) = {
    if (get_last_12_month_list()(0).equals(yyyyMM)) 1 else 0
  }
  
  def get_calendar_end_date(plus_month:Int) = {
    get_last_date_of_month(DateTimeFormatter.ofPattern("yyyy-MM-dd").format(LocalDate.now.plusMonths(plus_month)))
  }
}