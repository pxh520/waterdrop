package org.interestinglab.waterdrop.filter

import java.util

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.functions.col

import scala.collection.JavaConversions._

class Dict(var config: Config) extends BaseFilter(config) {

  var isJoinFieldNested = false

  var dictDF: Option[DataFrame] = None

  def this() = {
    this(ConfigFactory.empty())
  }

  // TODO: 要求source_field 在fields中出现的话，必须为primary_field，否则配置出错
  override def checkConfig(): (Boolean, String) = (true, "")
  // parameters:
  // file_url, support protocol: hdfs, file, http
  // delimiter, column delimiter in row
  // schema
  // primary_field[required] // source_field join primary_field
  // source_field[required]
  // table_name // register as table for SQL use
//  override def checkConfig(): (Boolean, String) = {
//    conf.hasPath("file_url") && conf.hasPath("headers") && conf.hasPath("source_field") && conf.hasPath("dict_field") match {
//      case true => {
//        val fileURL = conf.getString("file_url")
//        val allowedProtocol = List("hdfs://", "file://", "http://")
//        var unsupportedProtocol = true
//        allowedProtocol.foreach(p => {
//          if (fileURL.startsWith(p)) {
//            unsupportedProtocol = false
//          }
//        })
//
//        if (unsupportedProtocol) {
//          (false, "unsupported protocol in [file_url], please choose one of " + allowedProtocol.mkString(", "))
//        } else {
//          (true, "")
//        }
//      }
//      case false => (false, "please specify [file_url], [headers], [source_field], [dict_field]")
//    }
//  }

  override def prepare(spark: SparkSession, ssc: StreamingContext): Unit = {
    super.prepare(spark, ssc)
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "delimiter" -> ",",
        "exclude_fields" -> util.Arrays.asList(), // fields in the list will be excluded
        "target_field" -> Json.ROOT
      )
    )

    config = config.withFallback(defaultConfig)

    isJoinFieldNested = config.getString("source_field").contains(".")

    // TODO: 字典数据中如果有需要排除的字段，则创建dataframe时，直接排除
    import spark.sqlContext.implicits._
    val df = config.getString("target_field") match {
      case Json.ROOT => {
        // TODO: 创建dictDF,非map型Schema, primary_field rename 为TEMPORARY_JOIN_FIELD_NAME
        Seq(("35", "IOS"), ("12", "Android")).toDF(Dict.TEMPORARY_JOIN_FIELD_NAME, "os")
      }
      case targetField: String => {
        // TODO: 创建dictDF,map型Schema, primary_field rename 为TEMPORARY_JOIN_FIELD_NAME
        Seq(("35", Map("os" -> "IOS")), ("12", Map("os" -> "Android")))
          .toDF(Dict.TEMPORARY_JOIN_FIELD_NAME, targetField)
      }
    }

    // TODO: 问题是dict数据的Schema已经被改掉了，怎么办？
    if (config.hasPath("table_name")) {
      df.createOrReplaceTempView(config.getString("table_name"))
    }

    dictDF = Some(df)

    dictDF.foreach { df =>
      // As the data set is getting materialized and send over the network it does only bring significant performance improvement,
      // if it considerable small.
      // Another constraint is that it also needs to fit completely into memory of each executor.
      // Not to forget it also needs to fit into the memory of the Driver!
      df.cache()

      df.count() // logging
      val firstN = 10
      df.show(firstN) // debug logging
      df.schema.treeString // debug logging
    }
  }

  override def process(spark: SparkSession, df: DataFrame): DataFrame = {

    dictDF match {
      case Some(dict) => {}
      case None => df
    }

    dictDF match {
      case Some(dict) => {
        // TODO :解决字段重名冲突
        val processedDF = config.getString("target_field") match {
          case Json.ROOT => {
            // TODO: 一个是嵌套方式时，没删除成功，一个是字段嵌套情况下，应该不需要删除！！！

            // drop existing conflicting fields to avoid duplicate fields in joined dataframe
            val fields =
              config
                .getStringList("fields")
                .toSet
                .diff(config.getStringList("exclude_fields").toSet)
                .diff(Set(config.getString("source_field")))
            fields.foldRight(df)((field, df1) => {
              df1.drop(field)
            })
          }
          case targetField: String => df.drop(targetField)
        }

        // handled join field name conflict on 2 dataframe
        processedDF
          .withColumn(Dict.TEMPORARY_JOIN_FIELD_NAME, col(config.getString("source_field")))
          .join(dict, Seq(Dict.TEMPORARY_JOIN_FIELD_NAME), "left_outer")
          .drop(Dict.TEMPORARY_JOIN_FIELD_NAME)
      }
      case None => df
    }
  }
}

object Dict {
  // This is use for handing join on nested field, because joining on nested field is not supported,
  // we can use this field as a bridge
  private val TEMPORARY_JOIN_FIELD_NAME = "_waterdrop_filter_dict_f1"
}
