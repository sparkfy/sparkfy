package com.github.sparkfy.codec

import java.io.{IOException, InputStream, OutputStream}

import com.github.sparkfy.util.MapConfWrapper._
import com.ning.compress.lzf.{LZFInputStream, LZFOutputStream}
import net.jpountz.lz4.{LZ4BlockInputStream, LZ4BlockOutputStream}
import org.xerial.snappy.{SnappyInputStream, Snappy, SnappyOutputStream}
import com.github.sparkfy.util.Utils

/**
 * :: DeveloperApi ::
 * CompressionCodec allows the customization of choosing different compression implementations
 * to be used in block storage.
 *
 * Note: The wire protocol for a codec is not guaranteed compatible across versions of Spark.
 * This is intended for use as an internal compression utility within a single
 * Spark application.
 */
trait CompressionCodec {

  def compressedOutputStream(s: OutputStream): OutputStream

  def compressedInputStream(s: InputStream): InputStream
}

object CompressionCodec {
  private val configKey = "compression.codec"

  private val shortCompressionCodecNames = Map(
    "lz4" -> classOf[LZ4CompressionCodec].getName,
    "lzf" -> classOf[LZFCompressionCodec].getName,
    "snappy" -> classOf[SnappyCompressionCodec].getName)

  val FALLBACK_COMPRESSION_CODEC = "lzf"
  val DEFAULT_COMPRESSION_CODEC = "snappy"
  val ALL_COMPRESSION_CODECS = shortCompressionCodecNames.values.toSeq

  def getCodecName(conf: Map[String,String]): String = {
    conf.get(configKey, DEFAULT_COMPRESSION_CODEC)
  }


  def createCodec(conf: Map[String,String], codecName: String): CompressionCodec = {
    val codecClass = shortCompressionCodecNames.getOrElse(codecName.toLowerCase, codecName)
    val codec = try {
      val ctor = Utils.classForName(codecClass).getConstructor(classOf[Map[String,String]])
      Some(ctor.newInstance(conf).asInstanceOf[CompressionCodec])
    } catch {
      case e: ClassNotFoundException => None
      case e: IllegalArgumentException => None
    }
    codec.getOrElse(throw new IllegalArgumentException(s"Codec [$codecName] is not available. " +
      s"Consider setting $configKey=$FALLBACK_COMPRESSION_CODEC"))
  }

  /**
   * Return the short version of the given codec name.
   * If it is already a short name, just return it.
   */
  def getShortName(codecName: String): String = {
    if (shortCompressionCodecNames.contains(codecName)) {
      codecName
    } else {
      shortCompressionCodecNames
        .collectFirst { case (k, v) if v == codecName => k }
        .getOrElse { throw new IllegalArgumentException(s"No short name for codec $codecName.") }
    }
  }

}

/**
 * :: DeveloperApi ::
 * LZ4 implementation of [[com.github.sparkfy.codec.CompressionCodec]].
 * Block size can be configured by `spark.io.compression.lz4.blockSize`.
 *
 * Note: The wire protocol for this codec is not guaranteed to be compatible across versions
 * of Spark. This is intended for use as an internal compression utility within a single Spark
 * application.
 */
class LZ4CompressionCodec(conf: Map[String, String]) extends CompressionCodec {

  override def compressedOutputStream(s: OutputStream): OutputStream = {
    val blockSize = conf.getSizeAsBytes("compression.lz4.blockSize", "32k").toInt
    new LZ4BlockOutputStream(s, blockSize)
  }

  override def compressedInputStream(s: InputStream): InputStream = new LZ4BlockInputStream(s)
}

/**
 * :: DeveloperApi ::
 * LZF implementation of [[com.github.sparkfy.codec.CompressionCodec]].
 *
 * Note: The wire protocol for this codec is not guaranteed to be compatible across versions
 * of Spark. This is intended for use as an internal compression utility within a single Spark
 * application.
 */
class LZFCompressionCodec(conf: Map[String, String]) extends CompressionCodec {

  override def compressedOutputStream(s: OutputStream): OutputStream = {
    new LZFOutputStream(s).setFinishBlockOnFlush(true)
  }

  override def compressedInputStream(s: InputStream): InputStream = new LZFInputStream(s)
}

/**
 * :: DeveloperApi ::
 * Snappy implementation of [[com.github.sparkfy.codec.CompressionCodec]].
 * Block size can be configured by `spark.io.compression.snappy.blockSize`.
 *
 * Note: The wire protocol for this codec is not guaranteed to be compatible across versions
 *       of Spark. This is intended for use as an internal compression utility within a single Spark
 *       application.
 */
class SnappyCompressionCodec(conf: Map[String,String]) extends CompressionCodec {

  try {
    Snappy.getNativeLibraryVersion
  } catch {
    case e: Error => throw new IllegalArgumentException(e)
  }

  override def compressedOutputStream(s: OutputStream): OutputStream = {
    val blockSize = conf.getSizeAsBytes("spark.io.compression.snappy.blockSize", "32k").toInt
    new SnappyOutputStreamWrapper(new SnappyOutputStream(s, blockSize))
  }

  override def compressedInputStream(s: InputStream): InputStream = new SnappyInputStream(s)
}

/**
 * Wrapper over [[SnappyOutputStream]] which guards against write-after-close and double-close
 * issues. See SPARK-7660 for more details. This wrapping can be removed if we upgrade to a version
 * of snappy-java that contains the fix for https://github.com/xerial/snappy-java/issues/107.
 */
private final class SnappyOutputStreamWrapper(os: SnappyOutputStream) extends OutputStream {

  private[this] var closed: Boolean = false

  override def write(b: Int): Unit = {
    if (closed) {
      throw new IOException("Stream is closed")
    }
    os.write(b)
  }

  override def write(b: Array[Byte]): Unit = {
    if (closed) {
      throw new IOException("Stream is closed")
    }
    os.write(b)
  }

  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    if (closed) {
      throw new IOException("Stream is closed")
    }
    os.write(b, off, len)
  }

  override def flush(): Unit = {
    if (closed) {
      throw new IOException("Stream is closed")
    }
    os.flush()
  }

  override def close(): Unit = {
    if (!closed) {
      closed = true
      os.close()
    }
  }
}
