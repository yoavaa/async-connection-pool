package com.wixpress.hoopoe.asyncjdbc.impl

import java.sql.{SQLException, DriverManager, Connection}
import java.util.concurrent.BlockingQueue
import com.wixpress.hoopoe.asyncjdbc._

/**
 * 
 * @author Yoav
 * @since 3/27/13
 */
class ConnectionWorker(val queue: BlockingQueue[AsyncTask],
                       val jdbcUrl: String,
                       val user: String,
                       val password: String,
                       val connectionTester: ConnectionTester,
                       val meter: ConnectionWorkerMeter) extends Thread {

  var stopped = false

  // todo cancel support
  override def run() {
    var connStatus: ConnectionStatus = aquireConnection()
    while (!stopped) {
      val task = aquireTask()
      task match {
        case connTask: ConnectionTask[_] => {
          connStatus = preTaskConnectionHook(connStatus)
          connStatus match {
            case FailedToConnect(e) => connectionError(e, connTask)
            case Connected(conn, _) => connStatus = handleTask(conn, connTask)
          }
          connStatus = postTaskConnectionHook(connStatus)
        }
        case stopTask: StopTask => stopped = true
      }
    }
    clearConnection(connStatus)

  }

  def connectionError(exception: Exception, task: ConnectionTask[_]) {
    task.failed(exception)
  }

  def handleTask(connection: Connection, task: ConnectionTask[_]): ConnectionStatus = {
    meter.startTask()
    val lastTaskError = task.run(connection)
    meter.taskCompleted(lastTaskError)
    Connected(connection, lastTaskError)
  }

  def postTaskConnectionHook(connStatus: ConnectionStatus): ConnectionStatus = {
    connStatus match {
      case FailedToConnect(e) => aquireConnection()
      case Connected(conn, lastTaskError) => {
        if (connectionTester.postTaskTest(conn, lastTaskError))
          connStatus
        else {
          clearConnection(conn)
          aquireConnection()
        }
      }
    }
  }

  def preTaskConnectionHook(connStatus: ConnectionStatus): ConnectionStatus = {
    connStatus match {
      case FailedToConnect(e) => aquireConnection()
      case Connected(conn, lastTaskError) => {
        if (connectionTester.preTaskTest(conn, lastTaskError, meter.getLastWaitOnQueueTime))
          connStatus
        else {
          clearConnection(conn)
          aquireConnection()
        }
      }
    }
  }

  def clearConnection(connTry: ConnectionStatus) {
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

  def aquireConnection(): ConnectionStatus = {
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

  def aquireTask(): AsyncTask = {
    try {
      meter.startWaitingOnQueue()
      val task = queue.take()
      meter.completedWaitingOnQueue()
      task
    }
    catch {
      case e: InterruptedException => {
        val task = new StopTask()
        meter.completedWaitingOnQueue()
        task
      }
    }
  }

  def shutdown() {
    queue.add(new StopTask)
  }

  def shutdownNow() {
    this.interrupt()
    queue.add(new StopTask)
    stopped = true
  }
}

trait ConnectionStatus
case class Connected(conn: Connection, lastTaskError: OptionalError) extends ConnectionStatus
object Connected {
  def apply(conn: Connection): Connected = this(conn, ok)
}
case class FailedToConnect(error: Exception) extends ConnectionStatus
