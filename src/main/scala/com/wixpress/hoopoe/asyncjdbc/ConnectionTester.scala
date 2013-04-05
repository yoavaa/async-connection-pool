package com.wixpress.hoopoe.asyncjdbc

import java.sql.Connection

/**
 * 
 * @author Yoav
 * @since 4/3/13
 */
trait ConnectionTester {
  def preTaskTest(conn: Connection, lastError: OptionalError, idleTime: Millis): ConnectionTestResult.Value
  def postTaskTest(conn: Connection, lastError: OptionalError): ConnectionTestResult.Value
}

object ConnectionTestResult extends Enumeration {
  val ok, invalid = Value

}
