package com.wixpress.hoopoe.asyncjdbc

import java.sql.Connection
import concurrent.Promise

/**
 * 
 * @author Yoav
 * @since 3/27/13
 */
class ConnectionTask[T](val doWithConnection: (Connection => T)) extends AsyncTask {

  val promise: Promise[T] = Promise[T]()

  def failed(exception: Throwable) {
    promise.failure(exception)
  }

  def run(connection: Connection) {
    try {
      promise.success(doWithConnection(connection))
    }
    catch {
      case e: Throwable => promise.failure(e)
    }
  }

}
