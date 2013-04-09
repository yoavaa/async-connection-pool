package com.wixpress.hoopoe.asyncjdbc

import java.sql.Connection
import concurrent.Future

/**
 * 
 * @author Yoav
 * @since 4/9/13
 */
trait AsyncDataSource {
  def doWithConnection[T](task: (Connection => T)): Future[T]
}


