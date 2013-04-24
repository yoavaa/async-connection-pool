package com.wixpress.hoopoe.asyncjdbc

import impl.{ConnectionWorker, ConnectionTask}
import java.sql.Connection
import java.io.Closeable
import java.util.concurrent.{RejectedExecutionException, BlockingQueue}
import concurrent.Future
import collection.immutable.Queue

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
                       val minPoolSize: Int,
                       val maxPoolSize: Int,
                       val queue: BlockingQueue[ConnectionTask[_]],
                       val connectionTester: ConnectionTester,
                       val checkResizeInterval: Millis) extends AsyncDataSource with Closeable {
  var stopped = false
  var workers: Queue[ConnectionWorker] = Queue()
  val manager = new ManagerThread()
  init()

  def doWithConnection[T](task: (Connection) => T): Future[T] = {
    if (stopped)
      throw new RejectedExecutionException
    val connectionTask = new ConnectionTask[T](task)
    queue.put(connectionTask)
    connectionTask.future
  }

  def shutdown() {
    manager.shutdown();
    synchronized({
      workers.foreach(_.shutdown())
    })
  }

  def shutdownNew() {
    manager.shutdown();
    synchronized({
      workers.foreach(_.shutdownNow())
    })
  }

  def close() {
    shutdown()
  }

  private def expand(num: Int) {
    synchronized({
      for (index <- 1 to num) {
        val worker: ConnectionWorker = new ConnectionWorker(queue, jdbcUrl, username, password, connectionTester)
        worker.start()
        workers = workers.enqueue(worker)
      }
    })
  }

  private def contract() {
    synchronized({
      // todo optimize
      if (workers.size > 0) {
        val worker = workers.head
        workers = workers.tail
        worker.shutdown()
      }
    })
  }

  private def init() {
    getClass.getClassLoader.loadClass(driverClass)
    expand(minPoolSize)
    manager.start()
  }

  private class ManagerThread extends Thread {
    var stopped = false

    override def run() {
      while (!stopped) {
        checkExpandContract()
        waitForNextIteration()
      }
    }

    def checkExpandContract() {
      val works = workers
      val stats = works.map(_.meter.snapshot)
    }

    def shutdown() {
      stopped = true
      this.interrupt()
    }

  }


  def waitForNextIteration() {
    try {
      Thread.sleep(100)
    }
    catch {
      case e: InterruptedException => {}
    }
  }
}

object QueuedDataSource {

}
