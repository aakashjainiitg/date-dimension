// Databricks notebook source
// MAGIC %run ./dim_date_schema

// COMMAND ----------

// MAGIC %run ./date_time_utils

// COMMAND ----------

def dim_date_generator(current_date: String): dim_date_schema ={
  val default_format = "yyyy-MM-dd"
  val date_key = date_time_utils.convert_string_to_date(current_date, default_format, "yyyyMMdd").toInt
  val date = current_date
  val day = date_time_utils.convert_string_to_date(current_date, default_format, "d").toInt
  val day_suffix = date_time_utils.get_day_suffix(current_date)
  val week_day = date_time_utils.convert_string_to_date(current_date, default_format, "e").toInt
  val week_day_name = date_time_utils.convert_string_to_date(current_date, default_format, "EEEE")
  val week_day_name_short = date_time_utils.convert_string_to_date(current_date, default_format, "E").toUpperCase
  val week_day_name_first_letter = date_time_utils.convert_string_to_date(current_date, default_format, "E").substring(0, 1)
  val day_of_year = date_time_utils.convert_string_to_date(current_date, default_format, "D").toInt
  val week_of_month = date_time_utils.convert_string_to_date(current_date, default_format, "W").toInt
  val week_of_year = date_time_utils.convert_string_to_date(current_date, default_format, "w").toInt
  val month = date_time_utils.convert_string_to_date(current_date, default_format, "M").toInt
  val month_name = date_time_utils.convert_string_to_date(current_date, default_format, "MMMM")
  val month_name_short = date_time_utils.convert_string_to_date(current_date, default_format, "MMM").toUpperCase
  val month_name_first_letter = date_time_utils.convert_string_to_date(current_date, default_format, "MMM").substring(0, 1)
  val quarter = date_time_utils.convert_string_to_date(current_date, default_format, "Q").toInt
  val quarter_name = date_time_utils.get_quater_name(current_date)
  val year = date_time_utils.convert_string_to_date(current_date, default_format, "u").toInt
  val yyyyMM = date_time_utils.convert_string_to_date(current_date, default_format, "yyyyMM")
  val month_year = date_time_utils.convert_string_to_date(current_date, default_format, "yyyy MMM").toUpperCase
  val is_weekend = date_time_utils.is_weekend(current_date)
  val is_holiday = 0
  val first_date_of_year = date_time_utils.get_date_of_year(current_date, "first")
  val last_date_of_year = date_time_utils.get_date_of_year(current_date, "last")
  val first_date_of_quarter = date_time_utils.get_first_date_of_quarter(current_date)
  val last_date_of_quarter = date_time_utils.get_last_date_of_quarter(current_date)
  val first_date_of_month = date_time_utils.get_first_date_of_month(current_date)
  val last_date_of_month = date_time_utils.get_last_date_of_month(current_date)
  val first_date_of_week = date_time_utils.get_first_date_of_week(current_date)
  val last_date_of_week = date_time_utils.get_last_date_of_week(current_date)
  val last_12_month_flag = date_time_utils.get_last_12_month_flag(yyyyMM)
  val last_6_month_flag = date_time_utils.get_last_6_month_flag(yyyyMM)
  val last_month_flag = date_time_utils.get_last_month_flag(yyyyMM)
  dim_date_schema(date_key, date, day, day_suffix, week_day, week_day_name, week_day_name_short, week_day_name_first_letter,
    day_of_year, week_of_month, week_of_year, month, month_name, month_name_short, month_name_first_letter, quarter,
    quarter_name, year, yyyyMM, month_year, is_weekend, is_holiday, first_date_of_year, last_date_of_year, first_date_of_quarter,
    last_date_of_quarter, first_date_of_month, last_date_of_month, first_date_of_week, last_date_of_week, last_12_month_flag, last_6_month_flag, last_month_flag)
}