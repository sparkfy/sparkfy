package com.github.sparkfy.serializer

import com.esotericsoftware.kryo.io.{Input => KryoInput, Output => KryoOutput}
import com.esotericsoftware.kryo.{Kryo, Serializer => KSerializer}
import org.apache.avro.generic.GenericRecord

/**
 * Custom serializer used for generic Avro records. If the user registers the schemas
 * ahead of time, then the schema's fingerprint will be sent with each message instead of the actual
 * schema, as to reduce network IO.
 * Actions like parsing or compressing schemas are computationally expensive so the serializer
 * caches all previously seen values as to reduce the amount of work needed to do.
 * @param schemas a map where the keys are unique IDs for Avro schemas and the values are the
 *                string representation of the Avro schema, used to decrease the amount of data
 *                that needs to be serialized.
 */
class GenericAvroSerializer(schemas: Map[Long, String])
  extends KSerializer[GenericRecord] {



  override def write(kryo: Kryo, output: KryoOutput, datum: GenericRecord): Unit = ???

  override def read(kryo: Kryo, input: KryoInput, datumClass: Class[GenericRecord]): GenericRecord = ???
}
