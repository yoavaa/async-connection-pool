package com.wixpress.hoopoe.asyncjdbc

import org.scalatest.{BeforeAndAfter, FlatSpec, FunSuite}
import org.scalatest.matchers.ShouldMatchers
import java.util.concurrent.{LinkedBlockingQueue, LinkedBlockingDeque, BlockingQueue, SynchronousQueue}
import java.sql.{SQLException, Connection}
import org.mockito.Mockito._
import concurrent.duration.Duration
import concurrent.Await
import scala.Some
import org.mockito.Matchers
import scala.util.{Failure, Success}

/**
 * 
 * @author Yoav
 * @since 3/27/13
 */
class ConnectionWorkerTest extends FlatSpec with ShouldMatchers with BeforeAndAfter {

  val mockDriver = MockDriver()
  val preTaskTestConnection = mock(classOf[(Connection, OptionalError) => Boolean])
  val postTaskTestConnection = mock(classOf[(Connection, OptionalError) => Boolean])
//  val futureWaitDuration: Duration = Duration("10 ms")
  val futureWaitDuration: Duration = Duration("10 s")

  before {
    reset(preTaskTestConnection, postTaskTestConnection)
    when(preTaskTestConnection.apply(Matchers.any[Connection], Matchers.any[OptionalError])).thenReturn(true)
    when(postTaskTestConnection.apply(Matchers.any[Connection], Matchers.any[OptionalError])).thenReturn(true)
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
    verify(preTaskTestConnection).apply(mockDriver.connection, ok)

    reset(preTaskTestConnection)
    when(preTaskTestConnection.apply(Matchers.any[Connection], Matchers.any[OptionalError])).thenReturn(false)

    queue.add(task2)
    val future2 = task2.promise.future
    Await.ready(future2, futureWaitDuration)
    future2.value should be (Some(Success(2)))
    // verify from the previous cycle - post
    verify(postTaskTestConnection).apply(mockDriver.connection, Error(sqlException))
    // verify from this cycle - pre
    verify(preTaskTestConnection).apply(mockDriver.connection, Error(sqlException))

  }

  def withWorker(testCode: (ConnectionWorker, BlockingQueue[AsyncTask]) => Any) {
    val queue = new LinkedBlockingQueue[AsyncTask]
    val worker = new ConnectionWorker(queue, mockDriver.mockUrl, "", "", preTaskTestConnection, postTaskTestConnection)
    worker.start()
    try {
      testCode(worker, queue)
    }
    finally {
      worker.shutdown;
      worker.join()
    }
  }

  def withDisabledConnection(testCode: () => Any) {
    mockDriver.disable
    try {
      testCode()
    }
    finally {
      mockDriver.enable
    }
  }
}
