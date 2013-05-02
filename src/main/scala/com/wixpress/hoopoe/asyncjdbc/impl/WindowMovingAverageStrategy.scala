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
    var count = 0
    var sum: Double = 0
    seq foreach {load =>
      load match {
        case valueLoad: ValueLoad => {
          sum = sum + valueLoad.percent * (valueLoad.workers) / currentSize
          count = count + 1
        }
        case NaNLoad => {}
      }
    }
    if (count > 0)
      sum / count
    else
      Double.NaN
  }

  private[WindowMovingAverageStrategy] case class PoolSnapshot(workTime: Millis,
                          overheadTime: Millis,
                          sleepTime: Millis,
                          errorCount: Int,
                          workers: Int,
                          operationals: Int) {
    def plus(that: ConnectionWorkerStatistics): PoolSnapshot = {
      that match {
        case stat: OperationalStatistics => {
          PoolSnapshot(workTime + stat.workTime,
            overheadTime + stat.overheadTime,
            sleepTime + stat.sleepTime,
            errorCount + stat.errorCount,
            workers + 1,
            operationals + 1)
        }
        case NonOperationalStatistics => {
          PoolSnapshot(workTime,
            overheadTime,
            sleepTime,
            errorCount,
            workers + 1,
            operationals)
        }
      }
    }

    // todo - load function should take into account startup, current thread is sleeping and such
    lazy val load: Load = operationals match {
      case 0 => NaNLoad
      case _ => ValueLoad(1.0 * (workTime + overheadTime) / (workTime + overheadTime + sleepTime), workers)
    }
  }

  private[WindowMovingAverageStrategy] object PoolSnapshot {
    def apply(): PoolSnapshot = this(0,0,0,0,0,0)
  }

  private[WindowMovingAverageStrategy] trait Load
  private[WindowMovingAverageStrategy] case class ValueLoad(percent: Double, workers: Int) extends Load
  private[WindowMovingAverageStrategy] case object NaNLoad extends Load

}


