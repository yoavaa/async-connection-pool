package com.wixpress.hoopoe.asyncjdbc

import impl._
import impl.ConnectionWorkerStatistics
import java.sql.Connection
import java.io.Closeable
import java.util.concurrent.{RejectedExecutionException, BlockingQueue}
import concurrent.Future
import collection.immutable.Queue
import collection.immutable

/**
 *
 * @author Yoav
 * @since 4/9/13
 */
// todo expand and contract
// todo expose statistics
class QueuedDataSource(val driverClass: String,
                       val jdbcUrl: String,
                       val username: String,
                       val password: String,
                       val resizeStrategy: ResizeStrategy,
                       val queue: BlockingQueue[ConnectionTask[_]],
                       val connectionTester: ConnectionTester,
                       val checkResizeInterval: Millis) extends AsyncDataSource with Closeable {
  var stopped = false
  var workers: Queue[ConnectionWorker] = Queue()
  val manager = new AsyncDataSourceManager(resizeStrategy, workerStatistics _, resizeTo)
  init()

  def doWithConnection[T](task: (Connection) => T): Future[T] = {
    if (stopped)
      throw new RejectedExecutionException
    val connectionTask = new ConnectionTask[T](task)
    queue.put(connectionTask)
    connectionTask.future
  }

  def shutdown() {
    manager.shutdown()
    synchronized({
      workers.foreach(_.shutdown())
    })
  }

  def shutdownNew() {
    manager.shutdown()
    synchronized({
      workers.foreach(_.shutdownNow())
    })
  }

  def close() {
    shutdown()
  }

  private def workerStatistics: immutable.Seq[ConnectionWorkerStatistics] = {
    workers.map(_.meter.snapshot).toSeq
  }

  private def resizeTo(toNum: Int) {
    synchronized({
      val currentSize = workers.size
      if (toNum > currentSize) {
        for (index <- toNum +1 to currentSize) {
          val worker = workers.head
          workers = workers.tail
          worker.shutdown()
        }
      }
      else if (toNum < currentSize) {
        for (index <- currentSize + 1 to toNum) {
          val worker: ConnectionWorker = new ConnectionWorker(queue, jdbcUrl, username, password, connectionTester)
          worker.start()
          workers = workers.enqueue(worker)
        }
      }
    })
  }

  private def init() {
    getClass.getClassLoader.loadClass(driverClass)
    resizeTo(resizeStrategy.minPoolSize)
    manager.start()
  }

}


object QueuedDataSource {

}
