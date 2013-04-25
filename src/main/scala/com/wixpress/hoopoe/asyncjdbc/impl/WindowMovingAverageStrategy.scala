package com.wixpress.hoopoe.asyncjdbc.impl

import com.wixpress.hoopoe.asyncjdbc._
import collection.mutable

/**
 *
 * @author yoav
 * @since 4/25/13
 */
class WindowMovingAverageStrategy(val minPoolSize: Int,
                                  val maxPoolSize: Int,
                                  val expandWindowSize: Int,
                                  val contractWindowSize: Int,
                                  val expandThreshold: Double,
                                  val contractThreshold: Double ) extends ResizeStrategy {

  var currentSize: Int = _
  val expandWindow: mutable.Queue[PoolSnapshot] = new mutable.Queue
  val contractWindow: mutable.Queue[PoolSnapshot] = new mutable.Queue


  def addSnapshot(stats: Seq[ConnectionWorkerStatistics]) {
    val currentSnapshot: PoolSnapshot = stats.foldLeft(PoolSnapshot())((poolSnapshot, workerSnapshot) => poolSnapshot.plus(workerSnapshot))
    currentSize = currentSnapshot.workers
    if (expandWindow.length >= expandWindowSize)
      expandWindow.dequeue()
    expandWindow.enqueue(currentSnapshot)
    if (contractWindow.length >= contractWindowSize)
      contractWindow.dequeue()
    contractWindow.enqueue(currentSnapshot)
  }

  def calculateNewSize(): Int = {
    currentSize =
      if (expandWindow.size == 0 && contractWindow.size == 0)
        minPoolSize
      else if (currentSize < maxPoolSize && expandWindowLoad > expandThreshold)
        currentSize + 1
      else if (currentSize > minPoolSize && contractWindowLoad < contractThreshold)
        currentSize - 1
      else
        currentSize
    currentSize
  }


  def contractWindowLoad: Double = {
    val load = contractWindow.map(_.load)
    waitedAverage(load)
  }

  def expandWindowLoad: Double = {
    val load = expandWindow.map(_.load)
    waitedAverage(load)
  }

  def waitedAverage(seq: Seq[Load]): Double = {
    val length = seq.length
    var sum: Double = 0
    seq.foreach(load => {
      sum = sum + load.percent * (load.workers) / currentSize
    })
    sum / length
  }

  private[WindowMovingAverageStrategy] case class PoolSnapshot(workTime: Millis,
                          overheadTime: Millis,
                          sleepTime: Millis,
                          errorCount: Int,
                          workers: Int) {
    def plus(that: ConnectionWorkerStatistics): PoolSnapshot = {
      PoolSnapshot(workTime + that.workTime,
        overheadTime + that.overheadTime,
        sleepTime + that.sleepTime,
        errorCount + that.errorCount,
        workers + 1)
    }

    lazy val load: Load = Load(1.0 * (workTime + overheadTime) / (workTime + overheadTime + sleepTime), workers)
  }

  private[WindowMovingAverageStrategy] object PoolSnapshot {
    def apply(): PoolSnapshot = this(0,0,0,0,0)
  }

  private[WindowMovingAverageStrategy] case class Load(percent: Double, workers: Int)

}


