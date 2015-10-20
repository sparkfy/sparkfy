package com.github.sparkfy.recovery.file

import java.io.{File, FileInputStream, FileOutputStream}

import com.github.sparkfy.Logging
import com.github.sparkfy.recovery.PersistenceEngine
import com.github.sparkfy.serializer.{DeserializationStream, SerializationStream, Serializer}
import com.github.sparkfy.util.Utils

import scala.reflect.ClassTag

/**
 * Created by yellowhuang on 2015/10/20.
 */

class FileSystemPersistenceEngine(val dir: String, val serializer: Serializer)
  extends PersistenceEngine with Logging {

  new File(dir).mkdir()

  /**
   * Defines how the object is serialized and persisted. Implementation will
   * depend on the store used.
   */
  override def persist(name: String, obj: Object): Unit = {
    serializeIntoFile(new File(dir + File.separator + name), obj)
  }

  /**
   * Defines how the object referred by its name is removed from the store.
   */
  override def unpersist(name: String): Unit = {
    val f = new File(dir + File.separator + name)
    if (!f.delete()) {
      logWarning(s"Error deleting ${f.getPath()}")
    }
  }

  /**
   * Gives all objects, matching a prefix. This defines how objects are
   * read/deserialized back.
   */
  override def read[T: ClassTag](prefix: String): Seq[T] = {
    val files = new File(dir).listFiles().filter(_.getName.startsWith(prefix))
    files.map(deserializeFromFile[T])
  }

  private def serializeIntoFile(file: File, value: AnyRef) {
    val created = file.createNewFile()
    if (!created) {
      throw new IllegalStateException("Could not create file: " + file)
    }
    val fileOut = new FileOutputStream(file)
    var out: SerializationStream = null
    Utils.tryWithSafeFinally {
      out = serializer.newInstance().serializeStream(fileOut)
      out.writeObject(value)
    } {
      fileOut.close()
      if (out != null) {
        out.close()
      }
    }
  }

  private def deserializeFromFile[T](file: File)(implicit m: ClassTag[T]): T = {
    val fileIn = new FileInputStream(file)
    var in: DeserializationStream = null
    try {
      in = serializer.newInstance().deserializeStream(fileIn)
      in.readObject[T]()
    } finally {
      fileIn.close()
      if (in != null) {
        in.close()
      }
    }
  }

}
