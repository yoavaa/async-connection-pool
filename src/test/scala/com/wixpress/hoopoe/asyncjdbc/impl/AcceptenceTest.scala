package com.wixpress.hoopoe.asyncjdbc.impl

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import com.wixpress.hoopoe.asyncjdbc.QueuedDataSource
import concurrent.{Await, Future}
import concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import util.Success

/**
 * 
 * @author yoav
 * @since 4/25/13
 */
class AcceptenceTest extends FlatSpec with ShouldMatchers {

  val datasource = QueuedDataSource("org.h2.Driver", "jdbc:h2:mem:db", "sa", "", 3, 10)

  "QueuedDataSource" should "execute sql" in {
    val res: Future[Boolean] = datasource.doWithConnection(conn => {
      val statement = conn.prepareStatement("create table test (a int, b varchar(10))")
      statement.execute()
      true
    })
    Await.ready(res, Duration(100, TimeUnit.SECONDS))
    res.value should equal(Some(Success(true)))
  }


}
