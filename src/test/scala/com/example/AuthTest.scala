package com.example

import java.nio.charset.StandardCharsets

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.connectors.pulsar.PulsarSourceBuilder
import org.apache.pulsar.client.api.{MessageId, PulsarClient}
import org.apache.pulsar.common.naming.TopicName

class AuthTest extends FlinkTest {

  val SUBSCRIPTION_NAME = "test1"
  val AUTH_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ5aWppZSJ9.I-v7SN9cUmQOqKsqYowUvTjADutJ1uiMb1jkK0c5ihs"
  val SERVICE_URL = "pulsar://localhost:6650"
  val INPUT_TOPIC = TopicName.get("tp1").toString

  test("auth") {

    val see = StreamExecutionEnvironment.getExecutionEnvironment
    see.getConfig.disableSysoutLogging()

    val messages = Array("A", "B", "C")

    sendMessages(INPUT_TOPIC, messages, SERVICE_URL)

    val src: SourceFunction[String] =
      PulsarSourceBuilder
        .builder(new SimpleStringSchema())
        .authentication("org.apache.pulsar.client.impl.auth.AuthenticationToken", "token:" + AUTH_TOKEN)
        .serviceUrl(SERVICE_URL)
        .topic(INPUT_TOPIC)
        .subscriptionName(SUBSCRIPTION_NAME)
        .build()

    implicit val typeInfo = BasicTypeInfo.STRING_TYPE_INFO

    val dataStream = see.addSource(src)
    dataStream.print()

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