package com.wixpress.hoopoe.asyncjdbc.impl

import java.sql.{SQLException, DriverManager, Connection}
import java.util.concurrent.BlockingQueue
import com.wixpress.hoopoe.asyncjdbc._

/**
 *
 * @author Yoav
 * @since 3/27/13
 */
class ConnectionWorker(val queue: BlockingQueue[ConnectionTask[_]],
                       val jdbcUrl: String,
                       val user: String,
                       val password: String,
                       val connectionTester: ConnectionTester,
                       val meter: ConnectionWorkerMeter = new ConnectionWorkerMeter) extends Thread {

  var stopped = false
  var runningTask = false


  // todo cancel support
  override def run() {
    var connStatus: ConnectionTry = aquireConnection()
    while (!stopped) {
      val task = aquireTask()
      task.map({connTask =>
          connStatus = preTaskConnectionHook(connStatus)
          connStatus match {
            case FailedToConnect(e) => connectionError(e, connTask)
            case Connected(conn, _) => connStatus = handleTask(conn, connTask)
          }
          connStatus = postTaskConnectionHook(connStatus)
        })
    }
    clearConnection(connStatus)

  }

  def connectionError(exception: Exception, task: ConnectionTask[_]) {
    task.failed(exception)
  }

  def handleTask(connection: Connection, task: ConnectionTask[_]): ConnectionTry = {
    runningTask = true
    meter.startTask()
    val lastTaskError = task.run(connection)
    runningTask = false
    meter.taskCompleted(lastTaskError)
    Connected(connection, lastTaskError)
  }

  def postTaskConnectionHook(connStatus: ConnectionTry): ConnectionTry = {
    connStatus match {
      case FailedToConnect(e) => aquireConnection()
      case Connected(conn, lastTaskError) => {
        connectionTester.postTaskTest(conn, lastTaskError) match {
          case ConnectionTestResult.ok => connStatus
          case _ => {
            clearConnection(conn)
            aquireConnection()
          }
        }
      }
    }
  }

  def preTaskConnectionHook(connStatus: ConnectionTry): ConnectionTry = {
    connStatus match {
      case FailedToConnect(e) => aquireConnection()
      case Connected(conn, lastTaskError) => {
        connectionTester.preTaskTest(conn, lastTaskError, meter.getLastWaitOnQueueTime) match {
          case ConnectionTestResult.ok => connStatus
          case _ => {
            clearConnection(conn)
            aquireConnection()
          }
        }
      }
    }
  }

  def clearConnection(connTry: ConnectionTry) {
    connTry match {
      case Connected(connection, _) => clearConnection(connection)
      case _ =>
    }
  }


  def clearConnection(connection: Connection) {
    try {
      connection.close()
    }
    catch {
      case e: SQLException =>
    }
  }

  def aquireConnection(): ConnectionTry = {
    try {
      Connected(DriverManager.getConnection(jdbcUrl, user, password))
    }
    catch {
      case e: SQLException => FailedToConnect(e)
      case e: InterruptedException => {
        stopped = true
        FailedToConnect(e)
      }
    }
  }

  def aquireTask(): Option[ConnectionTask[_]] = {
    try {
      meter.startWaitingOnQueue()
      val task = queue.take()
      meter.completedWaitingOnQueue()
      Some(task)
    }
    catch {
      case e: InterruptedException => {
        meter.completedWaitingOnQueue()
        None
      }
    }
  }

  def shutdown() {
    stopped = true
    if (!runningTask)
      this.interrupt()
  }

  def shutdownNow() {
    stopped = true
    this.interrupt()
  }
}

trait ConnectionTry
case class Connected(conn: Connection, lastTaskError: OptionalError) extends ConnectionTry
object Connected {
  def apply(conn: Connection): Connected = this(conn, ok)
}
case class FailedToConnect(error: Exception) extends ConnectionTry
