package com.amazonaws.codesamples.gsg

import java.util
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.Item

object MoviesItemOps01 {
  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val client = AmazonDynamoDBClientBuilder.standard.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-west-2")).build
    val dynamoDB = new DynamoDB(client)
    val table = dynamoDB.getTable("Movies")
    val year = 2015
    val title = "The Big New Movie"
    val infoMap = new util.HashMap[String, Any]
    infoMap.put("plot", "Nothing happens at all.")
    infoMap.put("rating", 0)
    try {
      System.out.println("Adding a new item...")
      val outcome = table.putItem(new Item().
        withPrimaryKey("year", year, "title", title).
        withMap("info", infoMap))
      System.out.println("PutItem succeeded:\n" + outcome.getPutItemResult)
    } catch {
      case e: Exception =>
        System.err.println("Unable to add item: " + year + " " + title)
        System.err.println(e.getMessage)
    }
  }
}