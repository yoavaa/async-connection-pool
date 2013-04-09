package com.wixpress.hoopoe.asyncjdbc

import impl.ConnectionTask
import java.sql.Connection
import java.io.Closeable
import java.util.concurrent.BlockingQueue
import concurrent.Future

/**
 * 
 * @author Yoav
 * @since 4/9/13
 */
class MydaSource(val driverClass: String,
                 val jdbcUrl: String,
                 val username: String,
                 val password: String,
                 val minPoolSize: Int,
                 val maxPoolSize: Int,
                 val queue: BlockingQueue[ConnectionTask[_]],
                 val checkResizeInterval: Millis) extends AsyncDataSource with Closeable {

  init()


  def doWithConnection[T](task: (Connection) => T): Future[T] = {
    val connectionTask = new ConnectionTask[T](task)
    queue.put(connectionTask)
    connectionTask.future
  }

  def close() {}

  private def init() {

  }
}

object MydaSource {

}
