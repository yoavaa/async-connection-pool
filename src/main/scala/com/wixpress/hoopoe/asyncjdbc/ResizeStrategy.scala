package com.wixpress.hoopoe.asyncjdbc

import impl.ConnectionWorkerStatistics

/**
 *
 * @author yoav
 * @since 4/25/13
 */
trait ResizeStrategy {

  def addSnapshot(stats: Seq[ConnectionWorkerStatistics])
  def calculateNewSize(): Int
  val minPoolSize: Int
  val maxPoolSize: Int
}
