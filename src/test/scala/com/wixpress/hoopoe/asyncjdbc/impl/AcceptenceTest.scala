package com.wixpress.hoopoe.asyncjdbc.impl

import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import com.wixpress.hoopoe.asyncjdbc.QueuedDataSource
import concurrent.{Await, Future}
import concurrent.duration.Duration
import java.util.concurrent.{LinkedBlockingQueue, BlockingQueue, TimeUnit}
import util.Success
import java.sql.Connection

/**
 *
 * @author yoav
 * @since 4/25/13
 */
class AcceptenceTest extends FlatSpec with ShouldMatchers {


  "QueuedDataSource" should "execute sql" in withDataSource { datasource =>
    val res: Future[Boolean] = datasource.doWithConnection(conn => {
      execSql(conn, "create table mytable (a int, b varchar(10))")
      true
    })
    Await.ready(res, Duration(100, TimeUnit.SECONDS))
    res.value should equal(Some(Success(true)))
  }

  it should "execute subsequent sql" in withDataSource { datasource =>
    createMeTable(datasource)

    val res2: Future[Boolean] = datasource.doWithConnection(conn => {
      execSql(conn, "insert into mytable (a,b) values (1, 'abc')")
      true
    })
    Await.ready(res2, Duration(100, TimeUnit.SECONDS))
    res2.value should equal(Some(Success(true)))
  }


  it should "execute 2 sqls in parallel" in withDataSource { datasource =>
    createMeTable(datasource)

    val res1: Future[Boolean] = datasource.doWithConnection(conn => {
      execSql(conn, "insert into mytable (a,b) values (1, 'abc')")
      true
    })
    val res2: Future[Boolean] = datasource.doWithConnection(conn => {
      execSql(conn, "insert into mytable (a,b) values (2, 'def')")
      true
    })
    Await.ready(res1, Duration(100, TimeUnit.SECONDS))
    Await.ready(res2, Duration(100, TimeUnit.SECONDS))
    res1.value should equal(Some(Success(true)))
    res2.value should equal(Some(Success(true)))
  }

  it should "returned queried values" in withDataSource { datasource =>
    createMeTable(datasource)

    val res1: Future[Boolean] = datasource.doWithConnection(conn => {
      execSql(conn, "insert into mytable (a,b) values (1, 'abc')")
      true
    })
    Await.ready(res1, Duration(100, TimeUnit.SECONDS))
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

  def createMeTable(datasource: QueuedDataSource) {
    val res: Future[Boolean] = datasource.doWithConnection(conn => {
      execSql(conn, "create table mytable (a int, b varchar(10))")
      true
    })
    Await.ready(res, Duration(100, TimeUnit.SECONDS))
    res.value should equal(Some(Success(true)))
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



}
