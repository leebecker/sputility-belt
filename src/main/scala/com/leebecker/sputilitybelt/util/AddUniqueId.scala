package com.leebecker.sputilitybelt.util

import java.util.UUID

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.functions._

/**
  * Created by leebecker on 4/11/16.
  */
class AddUniqueId(override val uid: String) extends Transformer {

  def getId = UUID.randomUUID.toString
  val addUUID = udf(getId)

  def this() = this("AddUniqueId" + UUID.randomUUID().toString.takeRight(12))

  val outputCol: Param[String] = new Param(this, "outputCol", "input column name")

  def getOutputCol: String = $(outputCol)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(df: DataFrame): DataFrame = df.withColumn(getOutputCol, addUUID())

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = schema.add(getOutputCol, StringType)

}
