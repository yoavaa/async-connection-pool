package com.wixpress.hoopoe.asyncjdbc.impl

import com.wixpress.hoopoe.asyncjdbc.{OptionalError, ConnectionTestResult, ConnectionTester, Millis}
import java.sql.{ResultSet, Connection}

/**
 * 
 * @author Yoav
 * @since 4/5/13
 */
class DefaultConnectionTester extends ConnectionTester {

  val testAfterIdlePeriod: Int = 1000
  def preTaskTest(conn: Connection, lastError: OptionalError, idleTime: Millis): ConnectionTestResult.Value = {
    if (idleTime > testAfterIdlePeriod || lastError.isError)
      doTestConnection(conn)
    else
      ConnectionTestResult.ok
  }

  def postTaskTest(conn: Connection, lastError: OptionalError):ConnectionTestResult.Value = {
    ConnectionTestResult.ok
  }

  def doTestConnection(conn: Connection): ConnectionTestResult.Value = {
    var rs: ResultSet = null
    try {
      rs = conn.getMetaData.getTables(null, null, "PROBABLYNOT", Array[String]("TABLE"))
      ConnectionTestResult.ok
    }
    catch {
      case e: Exception => {
        ConnectionTestResult.invalid
      }
    }
    finally {
      try {
        if (rs != null)
          rs.close()
      }
      catch {
        case _:Exception => {}
      }
    }
  }
}
