package com.wixpress.hoopoe.asyncjdbc.impl

import java.sql.Connection
import concurrent.Promise

/**
 * 
 * @author Yoav
 * @since 3/27/13
 */
class ConnectionTask[T](val doWithConnection: (Connection => T)) extends AsyncTask {

  val promise: Promise[T] = Promise[T]()

  def failed(exception: Exception) {
    promise.failure(exception)
  }

  def run(connection: Connection): OptionalError = {
    try {
      promise.success(doWithConnection(connection))
      ok
    }
    catch {
      case e: Exception => {
        promise.failure(e)
        Error(e)
      }
    }
  }

}
