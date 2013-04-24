package com.wixpress.hoopoe.asyncjdbc.impl

import collection.{mutable, immutable}


class AsyncDataSourceManager(val strategy: ResizeStrategy,
                             val statsSource: () => immutable.Seq[ConnectionWorkerStatistics],
                             val resizeTo: (Int) => Unit) extends Thread {
  var stopped = false

  override def run() {
    while (!stopped) {
      checkResize()
      waitForNextIteration()
    }
  }

  def checkResize() {
    val stats: Seq[ConnectionWorkerStatistics] = statsSource()
    strategy.addSnapshot(stats)
    resizeTo(strategy.calculateNewSize())
  }

  def waitForNextIteration() {
    try {
      Thread.sleep(100)
    }
    catch {
      case e: InterruptedException => {}
    }
  }

  def shutdown() {
    stopped = true
    this.interrupt()
  }

}

trait ResizeStrategy {

  def addSnapshot(stats: Seq[ConnectionWorkerStatistics])
  def calculateNewSize(): Int
  abstract val minPoolSize: Int
  abstract val maxPoolSize: Int
}

class WindowMovingAverageStrategy(val minPoolSize: Int,
                                  val maxPoolSize: Int,
                                  val expandWindowSize: Int,
                                  val contractWindowSize: Int,
                                  val expandThreshold: Double,
                                  val contractThreshold: Double ) extends ResizeStrategy {

  var currentSize: Int = minPoolSize
  val expandWindow: mutable.Queue[ConnectionWorkerStatistics] = new mutable.Queue
  val contractWindow: mutable.Queue[ConnectionWorkerStatistics] = new mutable.Queue


  def addSnapshot(stats: Seq[ConnectionWorkerStatistics]) {
    val currentSnapshot: ConnectionWorkerStatistics = stats.reduce(_ plus _)
    if (expandWindow.length > expandWindowSize)
    expandWindow.dequeue()
    expandWindow.enqueue(currentSnapshot)
    if (contractWindow.length > contractWindowSize)
      contractWindow.dequeue()
    contractWindow.enqueue(currentSnapshot)
  }

  def calculateNewSize(): Int = {
    currentSize =
      if (currentSize < maxPoolSize && expandWindowLoad > expandThreshold)
        currentSize + 1
      else if (currentSize > minPoolSize && contractWindowLoad > contractThreshold)
        currentSize - 1
      else
        currentSize
    currentSize
  }


  def contractWindowLoad: Double = {
    contractWindow.reduce(_ plus _).loadPercent
  }

  def expandWindowLoad: Double = {
    expandWindow.reduce(_ plus _).loadPercent
  }
}
