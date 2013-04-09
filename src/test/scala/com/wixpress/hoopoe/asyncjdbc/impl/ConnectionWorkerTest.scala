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
class ConnectionWorkerTest extends FlatSpec with ShouldMatchers with BeforeAndAfter {

  val mockDriver = MockDriver()
  val connectionTester = mock(classOf[ConnectionTester])
  val connectionWorkerMeter = mock(classOf[ConnectionWorkerMeter])
//  val futureWaitDuration: Duration = Duration("10 ms")
  val futureWaitDuration: Duration = Duration("10 s")

  before {
    reset(connectionTester, connectionWorkerMeter)
    when(connectionTester.preTaskTest(Matchers.any[Connection], Matchers.any[OptionalError], Matchers.any[Long]))
      .thenReturn(ConnectionTestResult.ok)
    when(connectionTester.postTaskTest(Matchers.any[Connection], Matchers.any[OptionalError]))
      .thenReturn(ConnectionTestResult.ok)
    when(connectionWorkerMeter.getLastWaitOnQueueTime).thenReturn(0)
  }

  "ConnectionWorker" should "run task if connection available" in withWorker { (worker, queue) =>
    val callback = mock(classOf[(Connection) => Unit])
    val task = new ConnectionTask(callback)

    queue.add(task)

    val future = task.promise.future
    Await.ready(future, futureWaitDuration)

    verify(callback).apply(mockDriver.connection)
  }

  it should "return the value from the connection callback" in withWorker { (worker, queue) =>
    val callback: (Connection) => Int = {Connection => 1}
    val task = new ConnectionTask(callback)

    queue.add(task)

    val future = task.promise.future
    Await.ready(future, futureWaitDuration)

    val res = future.value
    res should be (Some(Success(1)))
  }

  it should "fail task if cannot connect" in withDisabledConnection { () =>
    withWorker { (worker, queue) =>
      val callback = mock(classOf[(Connection) => Unit])
      val task = new ConnectionTask(callback)

      queue.add(task)

      val future = task.promise.future
      Await.ready(future, futureWaitDuration)

      val res = future.value
      res should be (Some(Failure(mockDriver.connectException)))
      verify(callback, never()).apply(mockDriver.connection)
    }
  }

  it should "perform two jobs on the same connection" in withWorker { (worker, queue) =>
    var conn1:Connection = null
    var conn2:Connection = null
    val callback1: (Connection) => Int = {connection => conn1 = connection; 1}
    val callback2: (Connection) => Int = {connection => conn2 = connection; 2}
    val task1 = new ConnectionTask(callback1)
    val task2 = new ConnectionTask(callback2)

    queue.add(task1)
    queue.add(task2)

    val future1 = task1.promise.future
    Await.ready(future1, futureWaitDuration)
    val future2 = task2.promise.future
    Await.ready(future2, futureWaitDuration)

    val res1 = future1.value
    val res2 = future2.value
    res1 should be (Some(Success(1)))
    res2 should be (Some(Success(2)))
    conn1 should equal (conn2)
  }

  it should "restore broken connection" in withWorker{ (withWorker, queue) =>
    val sqlException = new SQLException("connection broken")
    val callback1:(Connection) => Int = {Connection => throw sqlException}
    val callback2:(Connection) => Int = {Connection => 2}
    val task1 = new ConnectionTask(callback1)
    val task2 = new ConnectionTask(callback2)

    queue.add(task1)
    val future1 = task1.promise.future
    Await.ready(future1, futureWaitDuration)
    future1.value should be (Some(Failure(sqlException)))
    verify(connectionTester).preTaskTest(mockDriver.connection, ok, 0)

//    reset(preTaskTestConnection)
    when(connectionTester.preTaskTest(Matchers.any[Connection], Matchers.any[OptionalError], Matchers.any[Long]))
      .thenReturn(ConnectionTestResult.invalid)

    queue.add(task2)
    val future2 = task2.promise.future
    Await.ready(future2, futureWaitDuration)
    future2.value should be (Some(Success(2)))
    // verify from the previous cycle - post
    verify(connectionTester).postTaskTest(mockDriver.connection, Error(sqlException))
    // verify from this cycle - pre
    verify(connectionTester).preTaskTest(mockDriver.connection, Error(sqlException), 0)

  }

  it should "report events to meter" in withWorker { (worker, queue) =>
    val callback = mock(classOf[(Connection) => Unit])
    val task = new ConnectionTask(callback)

    queue.add(task)

    val future = task.promise.future
    Await.ready(future, futureWaitDuration)

    verify(connectionWorkerMeter, atLeastOnce()).startWaitingOnQueue()
    verify(connectionWorkerMeter).completedWaitingOnQueue()
    verify(connectionWorkerMeter).startTask()
    verify(connectionWorkerMeter).taskCompleted(ok)
  }

  def withWorker(testCode: (ConnectionWorker, BlockingQueue[ConnectionTask[_]]) => Any) {
    val queue = new LinkedBlockingQueue[ConnectionTask[_]]
    val worker = new ConnectionWorker(queue, mockDriver.mockUrl, "", "", connectionTester, connectionWorkerMeter)
    worker.start()
    try {
      testCode(worker, queue)
    }
    finally {
      worker.shutdown()
      worker.join()
    }
  }

  def withDisabledConnection(testCode: () => Any) {
    mockDriver.disable()
    try {
      testCode()
    }
    finally {
      mockDriver.enable()
    }
  }
}
