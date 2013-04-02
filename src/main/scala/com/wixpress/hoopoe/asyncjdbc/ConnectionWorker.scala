package com.wixpress.hoopoe.asyncjdbc

import java.sql.{SQLException, DriverManager, Connection}
import java.util.concurrent.BlockingQueue
import util.{Failure, Success, Try}

/**
 * 
 * @author Yoav
 * @since 3/27/13
 */
class ConnectionWorker(val queue: BlockingQueue[AsyncTask],
                       val jdbcUrl: String,
                       val user: String,
                       val password: String,
                       val preTaskConnectionTester: ((Connection, OptionalError) => Boolean),
                       val postTaskConnectionTester: ((Connection, OptionalError) => Boolean)) extends Thread {

  var stopped = false

  // todo cancel support
  // todo metrics support
  override def run {
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
    val lastTaskError = task.run(connection)
    Connected(connection, lastTaskError)
  }

  def postTaskConnectionHook(connStatus: ConnectionStatus): ConnectionStatus = {
    connStatus match {
      case FailedToConnect(e) => aquireConnection()
      case Connected(conn, lastTaskError) => {
        if (postTaskConnectionTester(conn, lastTaskError))
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
        if (preTaskConnectionTester(conn, lastTaskError))
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
      queue.take()
    }
    catch {
      case e: InterruptedException => new StopTask()
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

//case class ConnectionStatus(conn: Try[Connection], lastTaskError: OptionalError)

trait ConnectionStatus
case class Connected(conn: Connection, lastTaskError: OptionalError) extends ConnectionStatus
object Connected {
  def apply(conn: Connection): Connected = this(conn, ok)
}
case class FailedToConnect(error: Exception) extends ConnectionStatus
