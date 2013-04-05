package com.wixpress.hoopoe.asyncjdbc

import impl.OptionalError
import java.sql.Connection

/**
 * 
 * @author Yoav
 * @since 4/3/13
 */
trait ConnectionTester {
  def preTaskTest(conn: Connection, lastError: OptionalError, idleTime: Millis): Boolean
  def postTaskTest(conn: Connection, lastError: OptionalError): Boolean
}
