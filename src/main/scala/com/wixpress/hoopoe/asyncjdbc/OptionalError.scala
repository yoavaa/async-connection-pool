package com.wixpress.hoopoe.asyncjdbc

/**
 * 
 * @author Yoav
 * @since 4/2/13
 */
class OptionalError
case object ok extends OptionalError
case class Error(exception: Exception) extends OptionalError
