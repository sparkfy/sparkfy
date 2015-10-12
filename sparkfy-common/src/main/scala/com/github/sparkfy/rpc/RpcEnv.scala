package com.github.sparkfy.rpc

import java.util.concurrent.TimeoutException

import com.github.sparkfy.util.{RpcUtils, Utils}

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{Await, Awaitable, Future}

/**
 * Created by huangyu on 15/10/11.
 */
object RpcEnv {

}


abstract class RpcEnv(conf: Map[String, String]) {
  val defaultLookupTimeout = RpcUtils.lookupRpcTimeout(conf)

  /**
   * Return RpcEndpointRef of the registered [[RpcEndpoint]]. Will be used to implement
   * [[RpcEndpoint.self]]. Return `null` if the corresponding [[RpcEndpointRef]] does not exist.
   */
  private[rpc] def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef

  /**
   * Return the address that [[RpcEnv]] is listening to.
   */
  def address: RpcAddress

  /**
   * Register a [[RpcEndpoint]] with a name and return its [[RpcEndpointRef]]. [[RpcEnv]] does not
   * guarantee thread-safety.
   */
  def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef

  /**
   * Retrieve the [[RpcEndpointRef]] represented by `uri` asynchronously.
   */
  def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef]

  /**
   * Retrieve the [[RpcEndpointRef]] represented by `uri`. This is a blocking action.
   */
  def setupEndpointRefByURI(uri: String): RpcEndpointRef = {
    defaultLookupTimeout.awaitResult(asyncSetupEndpointRefByURI(uri))
  }

  /**
   * Retrieve the [[RpcEndpointRef]] represented by `systemName`, `address` and `endpointName`
   * asynchronously.
   */
  def asyncSetupEndpointRef(
                             systemName: String, address: RpcAddress, endpointName: String): Future[RpcEndpointRef] = {
    asyncSetupEndpointRefByURI(uriOf(systemName, address, endpointName))
  }


  /**
   * Retrieve the [[RpcEndpointRef]] represented by `systemName`, `address` and `endpointName`.
   * This is a blocking action.
   */
  def setupEndpointRef(
                        systemName: String, address: RpcAddress, endpointName: String): RpcEndpointRef = {
    setupEndpointRefByURI(uriOf(systemName, address, endpointName))
  }

  /**
   * Stop [[RpcEndpoint]] specified by `endpoint`.
   */
  def stop(endpoint: RpcEndpointRef): Unit

  /**
   * Shutdown this [[RpcEnv]] asynchronously. If need to make sure [[RpcEnv]] exits successfully,
   * call [[awaitTermination()]] straight after [[shutdown()]].
   */
  def shutdown(): Unit

  /**
   * Wait until [[RpcEnv]] exits.
   *
   * TODO do we need a timeout parameter?
   */
  def awaitTermination(): Unit


  /**
   * Create a URI used to create a [[RpcEndpointRef]]. Use this one to create the URI instead of
   * creating it manually because different [[RpcEnv]] may have different formats.
   */
  def uriOf(systemName: String, address: RpcAddress, endpointName: String): String

  /**
   * [[RpcEndpointRef]] cannot be deserialized without [[RpcEnv]]. So when deserializing any object
   * that contains [[RpcEndpointRef]]s, the deserialization codes should be wrapped by this method.
   */
  def deserialize[T](deserializationAction: () => T): T

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

object RpcTimeout {
  /**
   * Lookup prioritized list of timeout properties in the configuration
   * and create a RpcTimeout with the first set property key in the
   * description.
   * Uses the given default value if property is not set
   * @param conf configuration properties containing the timeout
   * @param timeoutPropList prioritized list of property keys for the timeout in seconds
   * @param defaultValue default timeout value in seconds if no properties found
   */
  def apply(conf: Map[String, String], timeoutPropList: Seq[String], defaultValue: String): RpcTimeout = {
    require(timeoutPropList.nonEmpty)

    // Find the first set property or use the default value with the first property
    val itr = timeoutPropList.iterator
    var foundProp: Option[(String, String)] = None
    while (itr.hasNext && foundProp.isEmpty) {
      val propKey = itr.next()
      conf.get(propKey).foreach { prop => foundProp = Some(propKey, prop) }
    }
    val finalProp = foundProp.getOrElse(timeoutPropList.head, defaultValue)
    val timeout = {
      Utils.timeStringAsSeconds(finalProp._2) seconds
    }
    new RpcTimeout(timeout, finalProp._1)
  }
}