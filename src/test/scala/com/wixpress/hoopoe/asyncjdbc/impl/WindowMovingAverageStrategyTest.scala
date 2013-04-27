package com.wixpress.hoopoe.asyncjdbc.impl

import org.scalatest.{BeforeAndAfter, FlatSpec}
import org.scalatest.matchers.ShouldMatchers

/**
 * 
 * @author Yoav
 * @since 3/27/13
 */
class WindowMovingAverageStrategyTest extends FlatSpec with ShouldMatchers {

  "WindowMovingAverageStrategy" should "report 5 when current size is 5 and load is steady 50%" in {
    val strategy = new WindowMovingAverageStrategy(2, 8, 3, 3, 0.8, 0.2)
    strategy.addSnapshot(Seq(load(1), load(1), load(1), load(1), load(1)))
    strategy.addSnapshot(Seq(load(50), load(50), load(50), load(50), load(50)))
    strategy.addSnapshot(Seq(load(50), load(50), load(50), load(50), load(50)))
    strategy.addSnapshot(Seq(load(50), load(50), load(50), load(50), load(50)))
    strategy.calculateNewSize() should equal(5)
  }

  it should "report 5 when current size is 5 and load is average 50%" in {
    val strategy = new WindowMovingAverageStrategy(2, 8, 3, 3, 0.8, 0.2)
    strategy.addSnapshot(Seq(load(1), load(1), load(1), load(1), load(1)))
    strategy.addSnapshot(Seq(load(10), load(30), load(50), load(70), load(90)))
    strategy.addSnapshot(Seq(load(90), load(90), load(90), load(90), load(90)))
    strategy.addSnapshot(Seq(load(10), load(10), load(10), load(10), load(10)))
    strategy.calculateNewSize() should equal(5)
  }

  it should "report 5 when current size is 5 and load is average 75%" in {
    val strategy = new WindowMovingAverageStrategy(2, 8, 3, 3, 0.8, 0.2)
    strategy.addSnapshot(Seq(load(1), load(1), load(1), load(1), load(1)))
    strategy.addSnapshot(Seq(load(70), load(70), load(70), load(70), load(70)))
    strategy.addSnapshot(Seq(load(80), load(80), load(80), load(80), load(80)))
    strategy.addSnapshot(Seq(load(75), load(75), load(75), load(75), load(75)))
    strategy.calculateNewSize() should equal(5)
  }

  it should "report 5 when current size is 5 and load is average 25%" in {
    val strategy = new WindowMovingAverageStrategy(2, 8, 3, 3, 0.8, 0.2)
    strategy.addSnapshot(Seq(load(1), load(1), load(1), load(1), load(1)))
    strategy.addSnapshot(Seq(load(20), load(20), load(20), load(20), load(20)))
    strategy.addSnapshot(Seq(load(25), load(25), load(25), load(25), load(25)))
    strategy.addSnapshot(Seq(load(30), load(30), load(30), load(30), load(30)))
    strategy.calculateNewSize() should equal(5)
  }

  it should "report 4 when current size is 5 and load is average 10%" in {
    val strategy = new WindowMovingAverageStrategy(2, 8, 3, 3, 0.8, 0.2)
    strategy.addSnapshot(Seq(load(1), load(1), load(1), load(1), load(1)))
    strategy.addSnapshot(Seq(load(20), load(20), load(20), load(20), load(20)))
    strategy.addSnapshot(Seq(load(05), load(05), load(05), load(05), load(05)))
    strategy.addSnapshot(Seq(load(05), load(05), load(05), load(05), load(05)))
    strategy.calculateNewSize() should equal(4)
  }


  it should "report 5 when current size is 5 and load is average 90%" in {
    val strategy = new WindowMovingAverageStrategy(2, 8, 3, 3, 0.8, 0.2)
    strategy.addSnapshot(Seq(load(1), load(1), load(1), load(1), load(1)))
    strategy.addSnapshot(Seq(load(80), load(80), load(80), load(80), load(80)))
    strategy.addSnapshot(Seq(load(95), load(95), load(95), load(95), load(95)))
    strategy.addSnapshot(Seq(load(95), load(95), load(95), load(95), load(95)))
    strategy.calculateNewSize() should equal(6)
  }

  it should "report 5 after growing from 4 at 90% load to 5 at 70% load" in {
    val strategy = new WindowMovingAverageStrategy(2, 8, 3, 3, 0.8, 0.2)
    strategy.addSnapshot(Seq(load(90), load(90), load(90), load(90)))
    strategy.addSnapshot(Seq(load(90), load(90), load(90), load(90)))
    strategy.addSnapshot(Seq(load(90), load(90), load(90), load(90)))
    strategy.calculateNewSize() should equal(5)
    strategy.addSnapshot(Seq(load(70), load(70), load(70), load(70), load(70)))
    strategy.calculateNewSize() should equal(5)
  }

  it should "report 5 after shrinking from 5 at 10% load to 4 at 30% load" in {
    val strategy = new WindowMovingAverageStrategy(2, 8, 3, 3, 0.8, 0.2)
    strategy.addSnapshot(Seq(load(10), load(10), load(10), load(10), load(10)))
    strategy.addSnapshot(Seq(load(10), load(10), load(10), load(10), load(10)))
    strategy.addSnapshot(Seq(load(10), load(10), load(10), load(10), load(10)))
    strategy.calculateNewSize() should equal(4)
    strategy.addSnapshot(Seq(load(50), load(50), load(50), load(50)))
    strategy.calculateNewSize() should equal(4)
  }

  it should "handle empty state" in {
    val strategy = new WindowMovingAverageStrategy(2, 8, 3, 3, 0.8, 0.2)
    strategy.calculateNewSize() should equal(2)
  }

  it should "handle just one snapshot" in {
    val strategy = new WindowMovingAverageStrategy(2, 8, 3, 3, 0.8, 0.2)
    strategy.addSnapshot(Seq(load(50), load(50), load(50), load(50), load(50)))
    strategy.calculateNewSize() should equal(5)
  }

  it should "handle just two snapshots" in {
    val strategy = new WindowMovingAverageStrategy(2, 8, 3, 3, 0.8, 0.2)
    strategy.addSnapshot(Seq(load(50), load(50), load(50), load(50), load(50)))
    strategy.addSnapshot(Seq(load(50), load(50), load(50), load(50), load(50)))
    strategy.calculateNewSize() should equal(5)
  }

  def load(v: Int) = new ConnectionWorkerStatistics(v, 0, 100-v, 0)
}
