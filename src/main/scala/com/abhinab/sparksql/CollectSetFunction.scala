package com.abhinab.sparksql

import org.apache.spark.sql.catalyst.expressions.aggregate.{Collect, ImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription}

import scala.collection.mutable

/**
  * Created by abhin on 3/22/2018.
  */
/*@ExpressionDescription(
  usage = "_FUNC_(expr) - Collects and returns a list of non-unique elements.")
case class CollectList(
                        child: Expression,
                        mutableAggBufferOffset: Int = 0,
                        inputAggBufferOffset: Int = 0) extends Collect {

  def this(child: Expression) = this(child, 0, 0)

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def prettyName: String = "collect_list"

  override protected[this] val buffer: mutable.ArrayBuffer[Any] = mutable.ArrayBuffer.empty
}*/

case class CollectList(
                        child: Expression,
                        mutableAggBufferOffset: Int = 0,
                        inputAggBufferOffset: Int = 0)