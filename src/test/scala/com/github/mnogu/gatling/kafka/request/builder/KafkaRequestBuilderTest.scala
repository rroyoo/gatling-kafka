package com.github.mnogu.gatling.kafka.request.builder

import com.github.mnogu.gatling.kafka.action.KafkaRequestActionBuilder
import io.gatling.commons.validation.Success
import io.gatling.core.session.{Expression, Session}
import jodd.util.RandomString
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class KafkaRequestBuilderTest extends AnyFlatSpec with Matchers with MockFactory {

  behavior of "A kafka request builder"

  it should "build a KafkaRequestActionBuilder with a payload" in {
    val requestNameValue: String = RandomString.get().randomAlpha(10)
    val requestName: Expression[String] = (_: Session) => Success[String](requestNameValue)
    val payloadValue: String = RandomString.get().randomAlpha(10)
    val payload: Expression[String] = (_: Session) => Success[String](payloadValue)

    val kafkaRequestActionBuilder = KafkaRequestBuilder(requestName).send(payload)

    kafkaRequestActionBuilder shouldBe a[KafkaRequestActionBuilder[_, _]]
  }

  it should "build a KafkaRequestActionBuilder with a key and a payload" in {
    val requestNameValue: String = RandomString.get().randomAlpha(10)
    val requestName: Expression[String] = (_: Session) => Success[String](requestNameValue)
    val keyValue: String = RandomString.get().randomAlpha(10)
    val key: Expression[String] = (_: Session) => Success[String](keyValue)
    val payloadValue: Array[Byte] = RandomString.get().randomAlpha(10).getBytes
    val payload: Expression[Array[Byte]] = (_: Session) => Success[Array[Byte]](payloadValue)

    val kafkaRequestActionBuilder = KafkaRequestBuilder(requestName).send(payload, key)

    kafkaRequestActionBuilder shouldBe a[KafkaRequestActionBuilder[_, _]]
  }

  it should "send an avro event" in {
    KafkaRequestBuilder(_ => Success("test"))
      .send(getPayload)
  }

  private def getPayload[T]: Session => Success[GenericRecord] = {
    val userSchema: String = """{"type":"record",""" +
      """"name":"myrecord",""" +
      """"fields":[{"name":"f1","type":"string"}]}""";
    val parser: Schema.Parser = new Schema.Parser()
    val schema: Schema = parser.parse(userSchema)
    val avroRecord: GenericData.Record = new GenericData.Record(schema)
    avroRecord.put("f1", "value1")
    (_: Session) => Success[GenericRecord](avroRecord)
  }
}
