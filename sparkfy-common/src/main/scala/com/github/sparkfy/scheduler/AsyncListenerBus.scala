package com.github.sparkfy.scheduler

import java.util.concurrent.{Semaphore, LinkedBlockingQueue}
import java.util.concurrent.atomic.AtomicBoolean

import scala.util.DynamicVariable
import AsyncListenerBus._


/**
 * An asynchronous event bus which posts events to its listeners.
 * Created by huangyu on 16/3/5.
 */
abstract class AsyncListenerBus[L <: AnyRef, E](name: String, eventCapacity: Int) extends ListenerBus[L, E] {

  self =>


  def this(name: String) = this(name, EVENT_QUEUE_CAPACITY)

  private val eventQueue = new LinkedBlockingQueue[E](eventCapacity)
  // Indicate if `start()` is called
  private val started = new AtomicBoolean(false)
  // Indicate if `stop()` is called
  private val stopped = new AtomicBoolean(false)

  // Indicate if we are processing some event
  // Guarded by `self`
  private var processingEvent = false

  private val logDroppedEvent = new AtomicBoolean(false)

  // A counter that represents the number of events produced and consumed in the queue
  private val eventLock = new Semaphore(0)

  private val listenerThread = new Thread(name) {
    setDaemon(true)

    override def run(): Unit = try {
      AsyncListenerBus.withinListenerThread.withValue(true) {
        while (true) {
          eventLock.acquire()
          self.synchronized {
            processingEvent = true
          }
          try {
            val event = eventQueue.poll
            if (event == null) {
              // Get out of the while loop and shutdown the daemon thread
              if (!stopped.get) {
                throw new IllegalStateException("Polling `null` from eventQueue means" +
                  " the listener bus has been stopped. So `stopped` must be true")
              }
              return
            }
            postToAll(event)
          } finally {
            self.synchronized {
              processingEvent = false
            }
          }
        }
      }
    } catch {
      case t: Throwable => {
        doFailure(t)
      }
    }
  }

  /**
   * Start sending events to attached listeners.
   *
   * This first sends out all buffered events posted before this listener bus has started, then
   * listens for any additional events asynchronously while the listener bus is still running.
   * This should only be called once.
   */
  def start(): Unit = {
    if (started.compareAndSet(false, true)) {
      doStart()
      listenerThread.start()
    } else {
      throw new IllegalStateException(s"$name already started!")
    }
  }

  def post(event: E): Unit = {
    if (stopped.get) {
      // Drop further events to make `listenerThread` exit ASAP
      logError(s"$name has already stopped! Dropping event $event")
      return
    }
    val eventAdded = eventQueue.offer(event)
    if (eventAdded) {
      eventLock.release()
    } else {
      doDropEvent(event)
    }
  }

  /**
   *
   * Stop the listener bus. It will wait until the queued events have been processed, but drop the
   * new events after stopping.
   */
  def stop(): Unit = {
    if (!started.get()) {
      throw new IllegalStateException(s"Attempted to stop $name that has not yet started!")
    }
    if (stopped.compareAndSet(false, true)) {
      // Call eventLock.release() so that listenerThread will poll `null` from `eventQueue` and know
      // `stop` is called.
      eventLock.release()
      listenerThread.join()
      doStop()
    } else {
      // Keep quiet
    }
  }

  protected def doStart(): Unit = {}

  protected def doStop(): Unit = {}


  /**
   * Fail to process event, the process thread will stop
   *
   * @param t Throwable
   */
  protected def doFailure(t: Throwable): Unit = {
    logError("Fail to start process thread or process event,the process thread will stop", t)
  }

  /**
   * If the event queue exceeds its capacity, the new events will be dropped. The subclasses will be
   * notified with the dropped events.
   *
   * Note: `doDropEvent` can be called in any thread.
   */
  protected def doDropEvent(event: E): Unit = {
    if (logDroppedEvent.compareAndSet(false, true)) {
      // Only log the following message once to avoid duplicated annoying logs.
      logError("Dropping Event because no remaining room in event queue. " +
        "This likely means one of the SparkListeners is too slow and cannot keep up with " +
        "the rate at which tasks are being started by the scheduler.")
    }
  }

}

object AsyncListenerBus {
  // Allows for Context to check whether stop() call is made within listener thread
  val withinListenerThread: DynamicVariable[Boolean] = new DynamicVariable[Boolean](false)
  // Cap the capacity of the event queue so we get an explicit error (rather than
  // an OOM exception) if it's perpetually being added to more quickly than it's being drained.
  val EVENT_QUEUE_CAPACITY = 10000
}