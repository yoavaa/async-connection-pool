package com.wixpress.hoopoe.asyncjdbc.impl

/**
 * 
 * @author Yoav
 * @since 4/2/13
 */
trait OptionalError {
  def isError: Boolean
}

case object ok extends OptionalError{
  def isError = false
}
case class Error(exception: Exception) extends OptionalError{
  def isError = true
}

