package com.amazonaws.codesamples.gsg

import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec

object MoviesItemOps02 {
  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val client = AmazonDynamoDBClientBuilder.standard.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-west-2")).build
    val dynamoDB = new DynamoDB(client)
    val table = dynamoDB.getTable("Movies")
    val year = 2015
    val title = "The Big New Movie"
    val spec = new GetItemSpec().withPrimaryKey("year", year, "title", title)
    try {
      System.out.println("Attempting to read the item...")
      val outcome = table.getItem(spec)
      System.out.println("GetItem succeeded: " + outcome)
    } catch {
      case e: Exception =>
        System.err.println("Unable to read item: " + year + " " + title)
        System.err.println(e.getMessage)
    }
  }
}