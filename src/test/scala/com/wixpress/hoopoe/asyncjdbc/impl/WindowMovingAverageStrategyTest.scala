package com.wixpress.hoopoe.asyncjdbc.impl

import org.scalatest.{BeforeAndAfter, FlatSpec}
import org.scalatest.matchers.ShouldMatchers
import java.util.concurrent.{LinkedBlockingQueue, BlockingQueue}
import java.sql.{SQLException, Connection}
import org.mockito.Mockito._
import concurrent.duration.Duration
import concurrent.Await
import org.mockito.Matchers
import com.wixpress.hoopoe.asyncjdbc._
import util.Failure
import scala.Some
import util.Success
import com.wixpress.hoopoe.asyncjdbc.Error

/**
 * 
 * @author Yoav
 * @since 3/27/13
 */
class WindowMovingAverageStrategyTest extends FlatSpec with ShouldMatchers with BeforeAndAfter {

  before {
  }

  "WindowMovingAverageStrategy" should "report 5 when current size is 5 and load is 50%" in {
    val strategy = new WindowMovingAverageStrategy(2, 8, 3, 3, 0.8, 0.2)
    strategy.addSnapshot(Seq(load50, load50, load50, load50, load50))
    strategy.addSnapshot(Seq(load50, load50, load50, load50, load50))
    strategy.addSnapshot(Seq(load50, load50, load50, load50, load50))
    strategy.addSnapshot(Seq(load50, load50, load50, load50, load50))
    strategy.addSnapshot(Seq(load50, load50, load50, load50, load50))
    strategy.calculateNewSize() should equal(5)
  }

//  it should "return the value from the connection callback" in {
//  }

  def load50 = new ConnectionWorkerStatistics(50, 0, 50, 0)
}
