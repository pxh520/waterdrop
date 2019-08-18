package io.github.interestinglab.waterdrop.input.batch

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStaticInput
import javax.swing.text.html.Option
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import java.util.Properties
class Jdbc extends BaseStaticInput {

  var config: Config = ConfigFactory.empty()

  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = {

    val requiredOptions = List("url", "table", "user", "password");

    val nonExistsOptions = requiredOptions.map(optionName => (optionName, config.hasPath(optionName))).filter { p =>
      val (optionName, exists) = p
      !exists
    }

    if (nonExistsOptions.isEmpty) {
      (true, "")
    } else {
      (
        false,
        "please specify " + nonExistsOptions
          .map { case (field, _) => "[" + field + "]" }
          .mkString(", ") + " as non-empty string")
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)
  }

  override def getDataset(spark: SparkSession): Dataset[Row] = {
    if(config.hasPath("predicates")){
      val strPredicates = config.getString("predicates")
      val arrPredicates = strPredicates.split(",")
      val readProperties = new Properties()
      readProperties.put("driver",config.getString("driver"))
      readProperties.put("user", config.getString("user"))
      readProperties.put("password", config.getString("password"))
      spark.read.jdbc(config.getString("url"),config.getString("table"),arrPredicates,readProperties)
    }
    else{
      spark.read
        .format("jdbc")
        .option("driver", config.getString("driver"))
        .option("url", config.getString("url"))
        .option("dbtable", config.getString("table"))
        .option("user", config.getString("user"))
        .option("password", config.getString("password"))
        .load()
    }
  }
}
