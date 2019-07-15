package com.Spark_DW

import com.Constants.Constan
import com.SparkUtils.JDBCUtils
import com.config.ConfigManager
import org.apache.commons.logging.LogFactory
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * dwd导入dws
  */
object DWD_DWS {
  def main(args: Array[String]): Unit = {
    val ssc: SparkSession = SparkSession.builder().enableHiveSupport().appName(Constan.SPARK_APP_NAME_USER).master(Constan.SPARK_LOACL).getOrCreate()
    val sql = ConfigManager.getProper(args(0))
    if (sql==null) {
      LogFactory.getLog("SparkLogger").debug("提交的表名参数有问题")
    } else {
      val finalSql = sql.replace("?",args(1))
      val df = ssc.sql(finalSql)
      val mysqlTableName = args(0).split("\\.")(1)
      val hiveTableName = args(0)
      val jdbcProp = JDBCUtils.getJdbcProp()._1
      val jdbcUrl = JDBCUtils.getJdbcProp()._2
      //      df.write.mode("append").jdbc(jdbcUrl,mysqlTableName,jdbcProp)
      df.write.mode(SaveMode.Overwrite).insertInto(hiveTableName)

    }
  }


}
