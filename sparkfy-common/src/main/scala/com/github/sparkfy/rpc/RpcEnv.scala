package com.github.sparkfy.rpc

import java.util.concurrent.TimeoutException



import scala.concurrent.{Await, Awaitable}
import scala.concurrent.duration.FiniteDuration

/**
 * Created by huangyu on 15/10/11.
 */
object RpcEnv {

}


case class RpcEnvConfig(conf: Map[String, String],
                                        name: String,
                                        host: String,
                                        port: Int)

/**
 * Represents a host and port.
 */
case class RpcAddress(host: String, port: Int) {
  // TODO do we need to add the type of RpcEnv in the address?

  val hostPort: String = host + ":" + port

  override val toString: String = hostPort

}


/**
 * An exception thrown if RpcTimeout modifies a [[TimeoutException]].
 */
class RpcTimeoutException(message: String, cause: TimeoutException)
  extends TimeoutException(message) {
  initCause(cause)
}


/**
 * Associates a timeout with a description so that a when a TimeoutException occurs, additional
 * context about the timeout can be amended to the exception message.
 * @param duration timeout duration in seconds
 * @param timeoutProp the configuration property that controls this timeout
 */
class RpcTimeout(val duration: FiniteDuration, val timeoutProp: String)
  extends Serializable {

  /** Amends the standard message of TimeoutException to include the description */
  private def createRpcTimeoutException(te: TimeoutException): RpcTimeoutException = {
    new RpcTimeoutException(te.getMessage() + ". This timeout is controlled by " + timeoutProp, te)
  }

  /**
   * PartialFunction to match a TimeoutException and add the timeout description to the message
   *
   * @note This can be used in the recover callback of a Future to add to a TimeoutException
   *       Example:
   *       val timeout = new RpcTimeout(5 millis, "short timeout")
   *       Future(throw new TimeoutException).recover(timeout.addMessageIfTimeout)
   */
  def addMessageIfTimeout[T]: PartialFunction[Throwable, T] = {
    // The exception has already been converted to a RpcTimeoutException so just raise it
    case rte: RpcTimeoutException => throw rte
    // Any other TimeoutException get converted to a RpcTimeoutException with modified message
    case te: TimeoutException => throw createRpcTimeoutException(te)
  }

  /**
   * Wait for the completed result and return it. If the result is not available within this
   * timeout, throw a [[RpcTimeoutException]] to indicate which configuration controls the timeout.
   * @param  awaitable  the `Awaitable` to be awaited
   * @throws RpcTimeoutException if after waiting for the specified time `awaitable`
   *                             is still not ready
   */
  def awaitResult[T](awaitable: Awaitable[T]): T = {
    try {
      Await.result(awaitable, duration)
    } catch addMessageIfTimeout
  }
}

object SparkTimeout{
  /**
   * Lookup prioritized list of timeout properties in the configuration
   * and create a RpcTimeout with the first set property key in the
   * description.
   * Uses the given default value if property is not set
   * @param conf configuration properties containing the timeout
   * @param timeoutPropList prioritized list of property keys for the timeout in seconds
   * @param defaultValue default timeout value in seconds if no properties found
   */
  def apply(conf: Map[String,String], timeoutPropList: Seq[String], defaultValue: String): RpcTimeout = {
    require(timeoutPropList.nonEmpty)

    // Find the first set property or use the default value with the first property
    val itr = timeoutPropList.iterator
    var foundProp: Option[(String, String)] = None
    while (itr.hasNext && foundProp.isEmpty){
      val propKey = itr.next()
      conf.get(propKey).foreach { prop => foundProp = Some(propKey, prop) }
    }
    val finalProp = foundProp.getOrElse(timeoutPropList.head, defaultValue)
    val timeout = { Utils.timeStringAsSeconds(finalProp._2) seconds }
    new RpcTimeout(timeout, finalProp._1)
  }
}