package com.wixpress.hoopoe.asyncjdbc.impl

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import com.wixpress.hoopoe.asyncjdbc.{AsyncDataSource, QueuedDataSource}
import concurrent.{Await, Future}
import concurrent.duration.Duration
import java.util.concurrent.{LinkedBlockingQueue, BlockingQueue, TimeUnit}
import util.Success
import java.sql.Connection
import collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global

/**
 *
 * @author yoav
 * @since 4/25/13
 */
class AcceptenceTest extends FlatSpec with ShouldMatchers {


  "QueuedDataSource" should "execute sql" in withDataSource { datasource =>
    val res: Future[Unit] = datasource.doWithConnection(conn => {
      execSql(conn, "create table mytable (a int, b varchar(10))")
    })
    Await.ready(res, Duration(100, TimeUnit.SECONDS))
    res.value should equal(Some(Success()))
  }

  it should "execute subsequent sql" in withDataSource { datasource =>
    createMeTable(datasource)

    val res2: Future[Unit] = datasource.doWithConnection(conn => {
      execSql(conn, "insert into mytable (a,b) values (1, 'abc')")
    })
    Await.ready(res2, Duration(100, TimeUnit.SECONDS))
    res2.value should equal(Some(Success()))
  }


  it should "execute 2 sqls in parallel" in withDataSource { datasource =>
    createMeTable(datasource)

    val res1 = insert(datasource, 1, "abc")
    val res2 = insert(datasource, 2, "def")
    Await.ready(res1, Duration(100, TimeUnit.SECONDS))
    Await.ready(res2, Duration(100, TimeUnit.SECONDS))
    res1.value should equal(Some(Success()))
    res2.value should equal(Some(Success()))
  }

  it should "returned queried values" in withDataSource { datasource =>
    createMeTable(datasource)
    val res = insert(datasource, 1, "abc")
    Await.ready(res, Duration(100, TimeUnit.SECONDS))

    val res2: Future[Option[String]] = datasource.doWithConnection(conn => {
      val ps = conn.prepareStatement("select * from mytable where a = 1")
      try {
        val rs = ps.executeQuery()
        try {
        if (rs.next)
          Some(rs.getString("b"))
        else
          None
        }
        finally {
          rs.close()
        }
      }
      finally {
        ps.close()
      }
    })
    Await.ready(res2, Duration(100, TimeUnit.SECONDS))
    res2.value should equal(Some(Success(Some("abc"))))
  }

  def createMeTable(datasource: AsyncDataSource) {
    val res: Future[Boolean] = datasource.doWithConnection(conn => {
      execSql(conn, "create table mytable (a int, b varchar(10))")
      true
    })
    Await.ready(res, Duration(100, TimeUnit.SECONDS))
    res.value should equal(Some(Success(true)))
  }

  def insert(datasource: AsyncDataSource, index: Int, value: String): Future[Unit] = {
    datasource.doWithConnection(conn => {
      val ps = conn.prepareStatement("insert into mytable (a,b) values (?, ?)")
      try {
        ps.setInt(1, index)
        ps.setString(2, value)
        ps.execute()
        println(index)
      }
      finally {
        ps.close()
      }
    })
  }

  def select(datasource: AsyncDataSource, index: Int): Future[Option[String]] = {
    datasource.doWithConnection(conn => {
      val ps = conn.prepareStatement("select * from mytable where a = ?")
      try {
        ps.setInt(1, index)
        val rs = ps.executeQuery()
        try {
        if (rs.next)
          Some(rs.getString("b"))
        else
          None
        }
        finally {
          rs.close()
        }
      }
      finally {
        ps.close()
      }
    })
  }

  def execSql(conn: Connection, sql: String) {
    val statement = conn.prepareStatement(sql)
    try {
      statement.execute()
    }
    finally {
      statement.close()
    }
  }

  def withDataSource(testCode: (QueuedDataSource) => Any) {
    val datasource = QueuedDataSource("org.h2.Driver", "jdbc:h2:mem:db", "sa", "", 3, 10)
    try {
      testCode(datasource)
    }
    finally {
      datasource.shutdown()
    }
  }

  class WorkflowStats(val start: Long = System.nanoTime(),
                      val sqls:ArrayBuffer[Long] = new ArrayBuffer[Long]) {
    def print {
      println("%d, %d,%d,%d,%d".format(start, sqls(0), sqls(1), sqls(2), sqls(3)))
    }
  }


}
