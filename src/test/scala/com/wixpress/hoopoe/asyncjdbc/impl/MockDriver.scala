package com.wixpress.hoopoe.asyncjdbc.impl

import java.sql._
import java.util.Properties
import org.mockito.Mockito._
import java.util.logging.Logger
import scala.Array

/**
 * 
 * @author Yoav
 * @since 3/27/13
 */
class MockDriver extends Driver {

  val connectException = mock(classOf[SQLException])
  val connection = mock(classOf[Connection])
  var enabled = true

  def connect(url: String, info: Properties) = {
    if (enabled)
      connection
    else
      throw connectException
  }

  def acceptsURL(url: String) = {
    url.startsWith("jdbc:mock")
  }

  def getPropertyInfo(url: String, info: Properties) = {
    Array[DriverPropertyInfo]()
  }

  def getMajorVersion = 1

  def getMinorVersion = 1

  def jdbcCompliant() = true

  def getParentLogger: Logger = {
    throw new SQLFeatureNotSupportedException
  }

  def mockUrl = "jdbc:mock:mock"

  def enable() {enabled = true}

  def disable() {enabled = false}

}

object MockDriver {

  val driver: MockDriver = new MockDriver
  DriverManager.registerDriver(driver)

  def apply() = driver
}
