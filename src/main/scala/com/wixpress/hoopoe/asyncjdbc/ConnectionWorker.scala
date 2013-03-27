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
                       val validateConnection: (Connection => Boolean) ) extends Thread {

  var stopped = false

  // todo cancel support
  // todo metrics support
  override def run {
    var connection: Try[Connection] = aquireConnection
    while (!stopped) {
      val task = aquireTask()
      task match {
        case connTask: ConnectionTask[_] => {
          connection = ensureConnected(connection)
          connection match {
            case Failure(e) => connectionError(e, connTask)
            case Success(conn) => handleTask(conn, connTask)
          }
        }
        case stopTask: StopTask => stopped = true
      }
    }
    clearConnection(connection)

  }

  def connectionError(exception: Throwable, task: ConnectionTask[_]) {
    task.failed(exception)
  }

  def handleTask(connection: Connection, task: ConnectionTask[_]) {
    task.run(connection)
  }

  def ensureConnected(connTry: Try[Connection]): Try[Connection] = {
    connTry match {
      case Failure(e) => aquireConnection
      case Success(conn) => {
        if (validateConnection(conn))
          Success(conn)
        else {
          clearConnection(connTry)
          aquireConnection
        }
      }
    }
  }

  def clearConnection(connTry: Try[Connection]) {
    connTry match {
      case Success(connection) => {
        try {
          connection.close()
        }
        catch {
          case e: SQLException =>
        }
      }
      case _ =>
    }
  }

  def aquireConnection: Try[Connection] = {
    try {
      Success(DriverManager.getConnection(jdbcUrl, user, password))
    }
    catch {
      case e: SQLException => Failure(e)
      case e: InterruptedException => {
        stopped = true
        Failure(e)
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

  def shutdown {
    queue.add(new StopTask)
  }

  def shutdownNow {
    this.interrupt()
    queue.add(new StopTask)
    stopped = true
  }
}
