package com.github.sparkfy.math.util

import java.io.File

import com.github.sparkfy.math.linalg.Vector
import com.github.sparkfy.math.linalg.{MapVector, SparseVector, DenseVector, VectorType}
import VectorType.VectorType
import com.github.sparkfy.math.linalg.MapVector
import com.github.sparkfy.util.AvroUtils
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DatumReader

import scala.io.Source

/**
 * Created by huangyu on 15/8/18.
 */
object VectorUtils {

  def newVectorAs(vector: Vector): Vector = {
    vector match {
      case mv: MapVector =>
        Vector()
      case dv: DenseVector =>
        Vector(new Array[Double](dv.values.length))
      case sv: SparseVector =>
        Vector(new Array[Int](sv.indices.length), new Array[Double](sv.values.length))
      case _ =>
        throw new IllegalArgumentException("Unknow vector:" + vector.getClass + "!")
    }
  }

  def newVector(vectorType: VectorType, denseLength: => Int = 0): Vector = {
    vectorType match {
      case VectorType.DENSE =>
        require(denseLength >= 0, "Create dense vector but length=%d is less than 0.".format(denseLength))
        Vector(new Array[Double](denseLength))
      case VectorType.SPARSE =>
        Vector(new Array[Int](0), new Array[Double](0))
      case VectorType.MAP =>
        Vector()
    }
  }

  def vectorType(vector: Vector): VectorType = {
    vector match {
      case mv: MapVector =>
        VectorType.MAP
      case dv: DenseVector =>
        VectorType.DENSE
      case sv: SparseVector =>
        VectorType.SPARSE
      case _ =>
        throw new IllegalArgumentException("Unknow vector:" + vector.getClass + "!")
    }
  }

  //index:value
  def load(path: String, vector: Vector): Vector = {
    Source.fromFile(path, "utf-8").getLines().foreach(l => {
      val ss = l.split(":")
      vector(ss(0).trim.toInt) = ss(1).trim.toDouble
    })
    vector
  }

  def vectorWithAttachIterator(vector: Vector, attach: Double): Iterator[(Int, Double)] = {
    val vi = vectorIterator(vector)
    new Iterator[(Int, Double)] {
      var isAttach = true

      override def hasNext: Boolean = isAttach || vi.hasNext

      override def next(): (Int, Double) = {
        val re = if (isAttach) (-1, attach) else vi.next
        isAttach = false
        re
      }
    }
  }


  def vectorIterator(vector: Vector): Iterator[(Int, Double)] = {


    def nextIndex(index: Int, length: Int, values: Array[Double]): Int = {
      var i = index
      while (i < length && (values(i) == 0)) {
        i += 1
      }
      i
    }

    def mapVectorIterator(mapVector: MapVector): Iterator[(Int, Double)] = {

      new Iterator[(Int, Double)] {

        var index = nextIndex(0, mapVector.values.length, mapVector.values)

        override def hasNext: Boolean = index < mapVector.table.length

        override def next(): (Int, Double) = {
          val v = (mapVector.table(index), mapVector.values(index))
          index = nextIndex(index + 1, mapVector.table.length, mapVector.values)
          v
        }
      }
    }

    def sparseVectorIterator(sparseVector: SparseVector): Iterator[(Int, Double)] = {
      new Iterator[(Int, Double)] {

        var index = nextIndex(0, sparseVector.values.length, sparseVector.values)

        override def hasNext: Boolean = index < sparseVector.indices.length

        override def next(): (Int, Double) = {
          val v = (sparseVector.indices(index), sparseVector.values(index))
          index = nextIndex(index + 1, sparseVector.values.length, sparseVector.values)
          v
        }
      }
    }

    def denseVectorIterator(denseVector: DenseVector): Iterator[(Int, Double)] = {
      new Iterator[(Int, Double)] {
        var index = nextIndex(0, denseVector.values.length, denseVector.values)

        override def hasNext: Boolean = index < denseVector.values.length

        override def next(): (Int, Double) = {
          val v = (index, denseVector.values(index))
          index = nextIndex(index + 1, denseVector.values.length, denseVector.values)
          v
        }
      }
    }

    vector match {
      case mv: MapVector =>
        mapVectorIterator(mv)
      case dv: DenseVector =>
        denseVectorIterator(dv)
      case sv: SparseVector =>
        sparseVectorIterator(sv)
      case _ =>
        throw new IllegalArgumentException("Unknow vector:" + vector.getClass + "!")
    }

  }

  def readAvro2VectorAddition(path: String, vector: Vector): Double = {
    var addition: Double = 0.0
    val file = new File(path)
    val reader: DatumReader[GenericRecord] = new GenericDatumReader[GenericRecord]()
    val dataFileReader = new DataFileReader[GenericRecord](file, reader)
    while (dataFileReader.hasNext) {
      val record = dataFileReader.next()
      val index = AvroUtils.getIntAvro(record, "index", false)
      val value = AvroUtils.getDoubleAvro(record, "value", false)
      if (index >= 0) {
        vector(index) = value
      } else if (index == -1) {
        addition = value
      }
    }
    addition
  }


}
