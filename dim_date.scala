// Databricks notebook source
// MAGIC %md
// MAGIC # DIM DATE
// MAGIC 
// MAGIC ## Overview
// MAGIC 
// MAGIC | Detail Tag | Information |
// MAGIC |------------|-------------|
// MAGIC |Originally Created By | Aakash Jain ([aakashjainiitg@gmail.com](mailto:aakashjainiitg@gmail.com)) |
// MAGIC 
// MAGIC ## History
// MAGIC 
// MAGIC | Date | Developed By | Reason |
// MAGIC |:----:|--------------|--------|
// MAGIC |26th December 2022 | Aakash Jain | Implemented the logic for DIM DATE|
// MAGIC 
// MAGIC ## Other Details
// MAGIC This Notebook contains many cells with lots of titles and markdown to give details and context for future developers. For more details visit https://medium.com/@aakashjainiitg/how-to-create-a-date-dimension-using-databricks-4867858eb20f

// COMMAND ----------

// MAGIC %md
// MAGIC ## Load Common Libraries

// COMMAND ----------

// MAGIC %run ./dim_date_generator

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

// COMMAND ----------

def create_final_df(): DataFrame = {

  //Start Date
  var start_date = "2017-01-01"

  //End Date
  val end_date = date_time_utils.get_calendar_end_date(60)

  //Mutable list to store dim date
  var dim_date_mutable_list = new ListBuffer[dim_date_schema]()

  while (date_time_utils.check(start_date, end_date)) {
    val dim_date_schema_object = dim_date_generator(start_date)
    dim_date_mutable_list += dim_date_schema_object
    start_date = LocalDate.parse(start_date, DateTimeFormatter.ofPattern("yyyy-MM-dd")).plusDays(1).toString
  }

  val dim_date_list = dim_date_mutable_list.toList
  val dim_date_df = spark.createDataset(dim_date_list)
  dim_date_df.select(col("date_key")
    , col("date").cast("date")
    , col("day")
    , col("day_suffix")
    , col("week_day")
    , col("week_day_name")
    , col("week_day_name_short")
    , col("week_day_name_first_letter")
    , col("day_of_year")
    , col("week_of_month")
    , col("week_of_year")
    , col("month")
    , col("month_name")
    , col("month_name_short")
    , col("month_name_first_letter")
    , col("quarter")
    , col("quarter_name")
    , col("year")
    , col("yyyymm")
    , col("month_year")
    , col("is_weekend")
    , col("is_holiday")
    , col("first_date_of_year").cast("date")
    , col("last_date_of_year").cast("date")
    , col("first_date_of_quarter").cast("date")
    , col("last_date_of_quarter").cast("date")
    , col("first_date_of_month").cast("date")
    , col("last_date_of_month").cast("date")
    , col("first_date_of_week").cast("date")
    , col("last_date_of_week").cast("date")
    , col("last_12_month_flag")
    , col("last_6_month_flag")
    , col("last_month_flag")
    , current_timestamp().as("load_date"))
}

// COMMAND ----------

display(create_final_df())