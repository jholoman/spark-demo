package com.cloudera.se.spark.demo.spark
import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import com.twitter.chill.avro.AvroSerializer
import org.apache.flume.source.avro.AvroFlumeEvent
import com.cloudera.se.spark.demo.avro.UpdatedEvent
class AvroKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[AvroFlumeEvent], AvroSerializer.SpecificRecordBinarySerializer[AvroFlumeEvent])
    kryo.register(classOf[UpdatedEvent], AvroSerializer.SpecificRecordBinarySerializer[UpdatedEvent])
  }
}
