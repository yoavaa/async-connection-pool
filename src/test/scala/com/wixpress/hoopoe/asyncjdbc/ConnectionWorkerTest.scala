package com.wixpress.hoopoe.asyncjdbc

import org.scalatest.{FlatSpec, FunSuite}
import org.scalatest.matchers.ShouldMatchers
import java.util.concurrent.{LinkedBlockingQueue, LinkedBlockingDeque, BlockingQueue, SynchronousQueue}
import java.sql.Connection
import org.mockito.Mockito._
import concurrent.duration.Duration
import concurrent.Await
import scala.Some
import util.{Failure, Success}

/**
 * 
 * @author Yoav
 * @since 3/27/13
 */
class ConnectionWorkerTest extends FlatSpec with ShouldMatchers {

  val mockDriver = MockDriver()
  def testConnection(conn: Connection) = true

  "ConnectionWorker" should "run task if connection available" in withWorker { (worker, queue) =>
    val callback = mock(classOf[(Connection) => Unit])
    val task = new ConnectionTask(callback)

    queue.add(task)

    val future = task.promise.future
    Await.ready(future, Duration("10 ms"))

    verify(callback).apply(mockDriver.connection)
  }

  it should "return the value from the connection callback" in withWorker { (worker, queue) =>
    val callback: (Connection) => Int = {Connection => 1}
    val task = new ConnectionTask(callback)

    queue.add(task)

    val future = task.promise.future
    Await.ready(future, Duration("10 ms"))

    val res = future.value
    res should be (Some(Success(1)))
  }

  it should "fail task if cannot connect" in withDisabledConnection { () =>
    withWorker { (worker, queue) =>
      val callback = mock(classOf[(Connection) => Unit])
      val task = new ConnectionTask(callback)

      queue.add(task)

      val future = task.promise.future
      Await.ready(future, Duration("10 ms"))

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
    Await.ready(future1, Duration("10 ms"))
    val future2 = task2.promise.future
    Await.ready(future2, Duration("10 ms"))

    val res1 = future1.value
    val res2 = future2.value
    res1 should be (Some(Success(1)))
    res2 should be (Some(Success(2)))
    conn1 should equal (conn2)
  }

  def withWorker(testCode: (ConnectionWorker, BlockingQueue[AsyncTask]) => Any) {
    val queue = new LinkedBlockingQueue[AsyncTask]
    val worker = new ConnectionWorker(queue, mockDriver.mockUrl, "", "", testConnection)
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
