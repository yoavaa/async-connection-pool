package com.wixpress.hoopoe.asyncjdbc.impl

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers

/**
 * 
 * @author Yoav
 * @since 4/3/13
 */
class ConnectionWorkerMeterTest extends FlatSpec with ShouldMatchers {

  "ConnectionWorkerMeter" should "start as counting overhead time" in withMeter {meter =>
    incTime(1000)
    meter.snapshot should equal(ConnectionWorkerStatistics(0, 1000, 0, 0))
  }

  it should "count work time with no errors" in withMeter {meter =>
    meter.startTask()
    incTime(1000)
    meter.taskCompleted(ok)
    meter.snapshot should equal(ConnectionWorkerStatistics(1000, 0, 0, 0))
  }

  it should "count work time if still processing task" in withMeter {meter =>
    meter.startTask()
    incTime(1000)
    meter.snapshot should equal(ConnectionWorkerStatistics(1000, 0, 0, 0))
  }

  it should "count work time with errors" in withMeter {meter =>
    meter.startTask()
    incTime(1000)
    meter.taskCompleted(Error(new RuntimeException))
    meter.snapshot should equal(ConnectionWorkerStatistics(1000, 0, 0, 1))
  }

  it should "count sleep time" in withMeter {meter =>
    meter.startWaitingOnQueue()
    incTime(1000)
    meter.completedWaitingOnQueue()
    meter.snapshot should equal(ConnectionWorkerStatistics(0, 0, 1000, 0))
  }

  it should "count sleep time if still sleeping" in withMeter {meter =>
    meter.startWaitingOnQueue()
    incTime(1000)
    meter.snapshot should equal(ConnectionWorkerStatistics(0, 0, 1000, 0))
  }

  it should "count all three counters" in withMeter {meter =>
    meter.startTask()
    incTime(1000)
    meter.taskCompleted(ok)
    incTime(1000)
    meter.startWaitingOnQueue()
    incTime(1000)
    meter.completedWaitingOnQueue()
    meter.snapshot should equal(ConnectionWorkerStatistics(1000, 1000, 1000, 0))
  }

  it should "reset after snapshot" in withMeter {meter =>
    meter.startTask()
    incTime(1000)
    meter.taskCompleted(Error(new RuntimeException))
    incTime(1000)
    meter.startWaitingOnQueue()
    incTime(1000)
    meter.completedWaitingOnQueue()
    meter.snapshot should equal(ConnectionWorkerStatistics(1000, 1000, 1000, 1))
    meter.snapshot should equal(ConnectionWorkerStatistics(0, 0, 0, 0))
  }

  var currentTime = System.currentTimeMillis()

  def incTime(offset: Long) {
    currentTime = currentTime + offset
  }

  def withMeter(testCode: (ConnectionWorkerMeter) => Any) {
    val meter = new ConnectionWorkerMeter(() => currentTime)
    testCode(meter)
  }

}
