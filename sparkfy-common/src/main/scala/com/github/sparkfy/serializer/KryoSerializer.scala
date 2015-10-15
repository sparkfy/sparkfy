package com.github.sparkfy.serializer

import java.io.{EOFException, IOException, InputStream, OutputStream}
import java.nio.ByteBuffer

import com.esotericsoftware.kryo.io.{Input => KryoInput, Output => KryoOutput}
import com.esotericsoftware.kryo.{Kryo, KryoException}
import com.github.sparkfy.util.ByteUnit
import com.github.sparkfy.util.MapConfWrapper._
import com.github.sparkfy.util.MapWrapper._
import com.github.sparkfy.{Logging, SparkfyException}
import com.twitter.chill.EmptyScalaKryoInstantiator
import scala.collection.JavaConverters._

import scala.reflect.ClassTag

/**
 * A Sparkfy serializer that uses the [[https://code.google.com/p/kryo/ Kryo serialization library]].
 *
 * Note that this serializer is not guaranteed to be wire-compatible across different versions of
 * Spark. It is intended to be used to serialize/de-serialize data within a single
 * Spark application.
 */
class KryoSerializer(conf: Map[String, String]) extends Serializer with Logging with Serializable {

  private val bufferSizeKb = conf.getSizeAsKb("kryoserializer.buffer", "64k")

  if (bufferSizeKb >= ByteUnit.GiB.toKiB(2)) {
    throw new IllegalArgumentException("kryoserializer.buffer must be less than " +
      s"2048 mb, got: + ${ByteUnit.KiB.toMiB(bufferSizeKb)} mb.")
  }

  private val bufferSize = ByteUnit.KiB.toBytes(bufferSizeKb).toInt

  val maxBufferSizeMb = conf.getSizeAsMb("kryoserializer.buffer.max", "64m").toInt
  if (maxBufferSizeMb >= ByteUnit.GiB.toMiB(2)) {
    throw new IllegalArgumentException("kryoserializer.buffer.max must be less than " +
      s"2048 mb, got: + $maxBufferSizeMb mb.")
  }
  private val maxBufferSize = ByteUnit.MiB.toBytes(maxBufferSizeMb).toInt

  private val referenceTracking = conf.getBoolean("kryo.referenceTracking", true)
  private val registrationRequired = conf.getBoolean("kryo.registrationRequired", false)
  private val userRegistrator = conf.get("kryo.registrator")
  private val classesToRegister = conf.getOrElse("kryo.classesToRegister", "")
    .split(',')
    .filter(!_.isEmpty)

  private val avroSchemas = conf.getAvroSchema

  def newKryoOutput(): KryoOutput = new KryoOutput(bufferSize, math.max(bufferSize, maxBufferSize))

  def newKryo(): Kryo = {
    val instantiator = new EmptyScalaKryoInstantiator
    val kryo = instantiator.newKryo()
    kryo.setRegistrationRequired(registrationRequired)
    val oldClassLoader = Thread.currentThread.getContextClassLoader
    val classLoader = defaultClassLoader.getOrElse(Thread.currentThread.getContextClassLoader)

    // Allow disabling Kryo reference tracking if user knows their object graphs don't have loops.
    // Do this before we invoke the user registrator so the user registrator can override this.
    kryo.setReferences(referenceTracking)

    for (cls <- KryoSerializer.toRegister) {
      kryo.register(cls)
    }

    // For results returned by asJavaIterable. See JavaIterableWrapperSerializer.
    kryo.register(JavaIterableWrapperSerializer.wrapperClass, new JavaIterableWrapperSerializer)


    null
  }

  override def newInstance(): SerializerInstance = new KryoSerializerInstance(this)
}


private[serializer] object KryoSerializer {
  // Commonly used classes.
  private val toRegister: Seq[Class[_]] = Seq(
    ByteBuffer.allocate(1).getClass,
    classOf[Array[Byte]],
    classOf[Array[Short]],
    classOf[Array[Long]]
  )
}


/**
 * Interface implemented by clients to register their classes with Kryo when using Kryo
 * serialization.
 */
trait KryoRegistrator {
  def registerClasses(kryo: Kryo)
}

class KryoSerializerInstance(ks: KryoSerializer) extends SerializerInstance {


  /**
   * A re-used [[Kryo]] instance. Methods will borrow this instance by calling `borrowKryo()`, do
   * their work, then release the instance by calling `releaseKryo()`. Logically, this is a caching
   * pool of size one. SerializerInstances are not thread-safe, hence accesses to this field are
   * not synchronized.
   */
  private[this] var cachedKryo: Kryo = borrowKryo()

  /**
   * Borrows a [[Kryo]] instance. If possible, this tries to re-use a cached Kryo instance;
   * otherwise, it allocates a new instance.
   */
  private[serializer] def borrowKryo(): Kryo = {
    if (cachedKryo != null) {
      val kryo = cachedKryo
      // As a defensive measure, call reset() to clear any Kryo state that might have been modified
      // by the last operation to borrow this instance (see SPARK-7766 for discussion of this issue)
      kryo.reset()
      cachedKryo = null
      kryo
    } else {
      ks.newKryo()
    }
  }

  /**
   * Release a borrowed [[Kryo]] instance. If this serializer instance already has a cached Kryo
   * instance, then the given Kryo instance is discarded; otherwise, the Kryo is stored for later
   * re-use.
   */
  private[serializer] def releaseKryo(kryo: Kryo): Unit = {
    if (cachedKryo == null) {
      cachedKryo = kryo
    }
  }

  // Make these lazy vals to avoid creating a buffer unless we use them.
  private lazy val output = ks.newKryoOutput()
  private lazy val input = new KryoInput()


  override def serialize[T: ClassTag](t: T): ByteBuffer = {
    output.clear()
    val kryo = borrowKryo()
    try {
      kryo.writeClassAndObject(output, t)
    } catch {
      case e: KryoException if e.getMessage.startsWith("Buffer overflow") =>
        throw new SparkfyException(s"Kryo serialization failed: ${e.getMessage}. To avoid this, " +
          "increase spark.kryoserializer.buffer.max value.")
    } finally {
      releaseKryo(kryo)
    }
    ByteBuffer.wrap(output.toBytes)
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T = {
    val kryo = borrowKryo()
    try {
      input.setBuffer(bytes.array)
      kryo.readClassAndObject(input).asInstanceOf[T]
    } finally {
      releaseKryo(kryo)
    }
  }

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T = {
    val kryo = borrowKryo()
    val oldClassLoader = kryo.getClassLoader
    try {
      kryo.setClassLoader(loader)
      input.setBuffer(bytes.array)
      kryo.readClassAndObject(input).asInstanceOf[T]
    } finally {
      kryo.setClassLoader(oldClassLoader)
      releaseKryo(kryo)
    }
  }

  override def serializeStream(s: OutputStream): SerializationStream = {
    new KryoSerializationStream(this, s)
  }

  override def deserializeStream(s: InputStream): DeserializationStream = {
    new KryoDeserializationStream(this, s)
  }

}

class KryoSerializationStream(
                               serInstance: KryoSerializerInstance,
                               outStream: OutputStream) extends SerializationStream {

  private[this] var output: KryoOutput = new KryoOutput(outStream)
  private[this] var kryo: Kryo = serInstance.borrowKryo()

  override def writeObject[T: ClassTag](t: T): SerializationStream = {
    kryo.writeClassAndObject(output, t)
    this
  }

  override def flush() {
    if (output == null) {
      throw new IOException("Stream is closed")
    }
    output.flush()
  }

  override def close() {
    if (output != null) {
      try {
        output.close()
      } finally {
        serInstance.releaseKryo(kryo)
        kryo = null
        output = null
      }
    }
  }
}

class KryoDeserializationStream(
                                 serInstance: KryoSerializerInstance,
                                 inStream: InputStream) extends DeserializationStream {

  private[this] var input: KryoInput = new KryoInput(inStream)
  private[this] var kryo: Kryo = serInstance.borrowKryo()

  override def readObject[T: ClassTag](): T = {
    try {
      kryo.readClassAndObject(input).asInstanceOf[T]
    } catch {
      // DeserializationStream uses the EOF exception to indicate stopping condition.
      case e: KryoException if e.getMessage.toLowerCase.contains("buffer underflow") =>
        throw new EOFException
    }
  }

  override def close() {
    if (input != null) {
      try {
        // Kryo's Input automatically closes the input stream it is using.
        input.close()
      } finally {
        serInstance.releaseKryo(kryo)
        kryo = null
        input = null
      }
    }
  }
}

/**
 * A Kryo serializer for serializing results returned by asJavaIterable.
 *
 * The underlying object is scala.collection.convert.Wrappers$IterableWrapper.
 * Kryo deserializes this into an AbstractCollection, which unfortunately doesn't work.
 */
private class JavaIterableWrapperSerializer
  extends com.esotericsoftware.kryo.Serializer[java.lang.Iterable[_]] {

  import JavaIterableWrapperSerializer._

  override def write(kryo: Kryo, out: KryoOutput, obj: java.lang.Iterable[_]): Unit = {
    // If the object is the wrapper, simply serialize the underlying Scala Iterable object.
    // Otherwise, serialize the object itself.
    if (obj.getClass == wrapperClass && underlyingMethodOpt.isDefined) {
      kryo.writeClassAndObject(out, underlyingMethodOpt.get.invoke(obj))
    } else {
      kryo.writeClassAndObject(out, obj)
    }
  }

  override def read(kryo: Kryo, in: KryoInput, clz: Class[java.lang.Iterable[_]])
  : java.lang.Iterable[_] = {
    kryo.readClassAndObject(in) match {
      case scalaIterable: Iterable[_] => scalaIterable.asJava
      case javaIterable: java.lang.Iterable[_] => javaIterable
    }
  }
}

private object JavaIterableWrapperSerializer extends Logging {
  // The class returned by JavaConverters.asJava
  // (scala.collection.convert.Wrappers$IterableWrapper).
  val wrapperClass =
    scala.collection.convert.WrapAsJava.asJavaIterable(Seq(1)).getClass

  // Get the underlying method so we can use it to get the Scala collection for serialization.
  private val underlyingMethodOpt = {
    try Some(wrapperClass.getDeclaredMethod("underlying")) catch {
      case e: Exception =>
        logError("Failed to find the underlying field in " + wrapperClass, e)
        None
    }
  }
}