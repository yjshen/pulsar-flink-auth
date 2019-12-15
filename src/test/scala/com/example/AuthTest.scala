package com.example

import java.nio.charset.StandardCharsets

import org.apache.flink.api.common.serialization.{SerializationSchema, SimpleStringSchema}
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.connectors.pulsar.partitioner.PulsarKeyExtractor
import org.apache.flink.streaming.connectors.pulsar.{FlinkPulsarProducer, PulsarSourceBuilder}
import org.apache.pulsar.client.api.{MessageId, PulsarClient}
import org.apache.pulsar.client.impl.conf.{ClientConfigurationData, ProducerConfigurationData}
import org.apache.pulsar.common.naming.TopicName

class AuthTest extends FlinkTest {

  val SUBSCRIPTION_NAME = "test1222"
  val AUTH_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ5aWppZSJ9.I-v7SN9cUmQOqKsqYowUvTjADutJ1uiMb1jkK0c5ihs"
  val SERVICE_URL = "pulsar://localhost:6650"
  val INPUT_TOPIC = TopicName.get("tp2").toString
  val OUTPUT_TOPIC = TopicName.get("tp-out-2").toString

  test("auth") {

    val see = StreamExecutionEnvironment.getExecutionEnvironment
    see.getConfig.disableSysoutLogging()
    see.setParallelism(1)

    val messages = Array("A", "B", "C")

    sendMessages(INPUT_TOPIC, messages, SERVICE_URL)

    val confData = new ClientConfigurationData()
    confData.setAuthPluginClassName("org.apache.pulsar.client.impl.auth.AuthenticationToken")
    confData.setAuthParams("token:" + AUTH_TOKEN)

    val prodData = new ProducerConfigurationData()
    prodData.setTopicName(OUTPUT_TOPIC)

    val src: SourceFunction[String] =
      PulsarSourceBuilder
        .builder(new SimpleStringSchema())
        .pulsarAllClientConf(confData)
        .serviceUrl(SERVICE_URL)
        .topic(INPUT_TOPIC)
        .subscriptionName(SUBSCRIPTION_NAME)
        .build()

    implicit val typeInfo = BasicTypeInfo.STRING_TYPE_INFO

    val ds = see.addSource(src)

    ds.addSink(
      new FlinkPulsarProducer[String](
        confData,
        prodData,
        new SerializationSchema[String] {
          override def serialize(t: String): Array[Byte] = t.getBytes(StandardCharsets.UTF_8)
        },
        new PulsarKeyExtractor[String] {
          override def getKey(in: String): String = "test"
        }
      ))

    see.execute("flink pulsar api")

  }

  def sendMessages(
    topic: String,
    messages: Array[String],
    serviceUrl: String): Seq[(String, MessageId)] = {

    val client = PulsarClient
      .builder()
      .authentication("org.apache.pulsar.client.impl.auth.AuthenticationToken", "token:" + AUTH_TOKEN)
      .serviceUrl(serviceUrl)
      .build()

    val producer = client.newProducer().topic(topic).create()

    val offsets = try {
      messages.map { m =>
        val mid = producer.send(m.getBytes(StandardCharsets.UTF_8))
        println(s"\t Sent $m of mid: $mid")
        (m, mid)
      }
    } finally {
      producer.flush()
      producer.close()
      client.close()
    }
    offsets
  }

}