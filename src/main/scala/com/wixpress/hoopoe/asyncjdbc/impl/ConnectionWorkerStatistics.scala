package com.wixpress.hoopoe.asyncjdbc.impl

import java.util.concurrent.atomic.AtomicReference
import com.wixpress.hoopoe.asyncjdbc._
import java.util.concurrent.locks.ReentrantLock

/**
 * Counter class that measures a ConnectionWorker load and overhead times
 * @author Yoav
 * @since 4/3/13
 */
class ConnectionWorkerMeter(val timeSource: () => Millis = () => System.currentTimeMillis()) {

  // thread-safe
  private val atomicCounters = new AtomicReference(new Counters)
  // non-thread-safe (no need to make it thread safe)
  private var lastWaitOnQueueStart: Millis = 0
  private var lastWaitOnQueueTime: Millis = 0
  private val lock = new ReentrantLock

  // thread-safe
  def startTask() {
    flushState(State.working, false)
  }

  // thread-safe
  def taskCompleted(error: OptionalError) {
    flushState(State.overhead, error.isError)
  }

  // thread-safe
  def startWaitingOnQueue() {
    flushState(State.sleeping, false)
    lastWaitOnQueueStart = timeSource()
  }

  // thread-safe
  def completedWaitingOnQueue() {
    flushState(State.overhead, false)
    lastWaitOnQueueTime = timeSource() - lastWaitOnQueueStart
    lastWaitOnQueueStart = 0
  }

  def getLastWaitOnQueueTime: Millis = lastWaitOnQueueTime

  // thread-safe
  def snapshot: ConnectionWorkerStatistics = {
    val currentCounter = snapshotCounter
    val now = timeSource()
    var workingTime: Millis = currentCounter.workTime
    var overheadTime: Millis = currentCounter.overheadTime
    var sleepingTime: Millis = currentCounter.sleepTime
    currentCounter.state match {
      case State.working => workingTime = workingTime + now - currentCounter.lastStateChange
      case State.overhead => overheadTime = overheadTime + now - currentCounter.lastStateChange
      case State.sleeping => sleepingTime = sleepingTime + now - currentCounter.lastStateChange
    }
    ConnectionWorkerStatistics(workingTime,overheadTime,sleepingTime,currentCounter.errorCount)
  }

  /**
   * Gets a snapshot of the atomicCounters value and resets the counter
   *
   * the method uses an atomic with lock update pattern
   * we attempt to update the atomicCounters variable.
   * If we fail due to concurrency, we try again, but this time
   * we lock to ensure no one interrupts and updates our atomicCounters in the middle.
   * @return the snapshot
   */
  private def snapshotCounter: Counters = {
    var currentCounter: Counters = null
    var newCounter: Counters = null
    do {
      currentCounter = atomicCounters.get()
      newCounter = Counters(state = currentCounter.state)
    } while (!atomicCounters.compareAndSet(currentCounter, newCounter))
    val current = currentCounter
    current
  }

  /**
   * updates the atomicCounters with an additional time measurement
   *
   * the method uses an atomic with lock update pattern
   * we attempt to update the atomicCounters variable.
   * If we fail due to concurrency, we try again, but this time
   * we lock to ensure no one interrupts and updates our atomicCounters in the middle.
   * @param newState the new state the counter gets into
   * @param hasError if an error was ancountered and should be counted
   */
  private def flushState(newState: State.State, hasError: Boolean) {
    val now = timeSource()
    var counters: Counters = null
    var newCounters: Counters = null
    do {
      counters = atomicCounters.get
      var overheadTime = counters.overheadTime
      var sleepTime = counters.sleepTime
      var workTime = counters.workTime
      var errorCount = counters.errorCount
      counters.state match {
        case State.overhead => overheadTime = overheadTime + (now-counters.lastStateChange)
        case State.sleeping => sleepTime = sleepTime + (now-counters.lastStateChange)
        case State.working => workTime = workTime + (now-counters.lastStateChange)
      }
      if (hasError)
        errorCount = errorCount + 1
      newCounters = Counters(overheadTime, sleepTime, workTime, errorCount, newState, timeSource())
    } while (!atomicCounters.compareAndSet(counters, newCounters))
  }

  private[ConnectionWorkerMeter] object State extends Enumeration {
    type State = Value
    val working, sleeping, overhead = Value
  }

  private[ConnectionWorkerMeter] case class Counters(overheadTime: Millis = 0,
                                                     sleepTime: Millis = 0,
                                                     workTime: Millis = 0,
                                                     errorCount: Int = 0,
                                                     state: State.State = State.overhead,
                                                     lastStateChange: Millis = timeSource()
                                                      )

}

case class ConnectionWorkerStatistics(workTime: Millis,
                                      overheadTime: Millis,
                                      sleepTime: Millis,
                                      errorCount: Int)
