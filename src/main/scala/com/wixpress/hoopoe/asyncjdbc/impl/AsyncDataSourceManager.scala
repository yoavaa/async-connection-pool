package com.wixpress.hoopoe.asyncjdbc.impl

import collection.immutable
import com.wixpress.hoopoe.asyncjdbc.ResizeStrategy


class AsyncDataSourceManager(val strategy: ResizeStrategy,
                             val statsSource: () => immutable.Seq[ConnectionWorkerStatistics],
                             val resizeTo: (Int) => Unit,
                             val datasourceIndex: Int) extends Thread("QueuedDataSource %d manager".format(datasourceIndex)) {
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


