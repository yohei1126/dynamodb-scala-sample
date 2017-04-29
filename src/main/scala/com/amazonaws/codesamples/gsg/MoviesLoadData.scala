package com.amazonaws.codesamples.gsg

import java.io.File

import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.Item
import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode

import scala.util.control.Breaks

object MoviesLoadData {
  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val client = AmazonDynamoDBClientBuilder.standard.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-west-2")).build
    val dynamoDB = new DynamoDB(client)
    val table = dynamoDB.getTable("Movies")
    val parser = new JsonFactory().createParser(new File(getClass.getClassLoader.getResource("moviedata.json").getFile))
    val rootNode: JsonNode = new ObjectMapper().readTree(parser)
    val iter = rootNode.iterator
    val mybreaks = new Breaks
    import mybreaks.{break, breakable}
    breakable {
      while ( {
        iter.hasNext
      }) {
        val currentNode: ObjectNode = iter.next.asInstanceOf[ObjectNode]
        val year = currentNode.path("year").asInt
        val title = currentNode.path("title").asText
        try {
          table.putItem(new Item().withPrimaryKey("year", year, "title", title).withJSON("info", currentNode.path("info").toString))
          System.out.println("PutItem succeeded: " + year + " " + title)
        } catch {
          case e: Exception =>
            System.err.println("Unable to add movie: " + year + " " + title)
            System.err.println(e.getMessage)
            break
        }
      }
    }
    parser.close()
  }
}