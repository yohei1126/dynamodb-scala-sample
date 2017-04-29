package com.amazonaws.codesamples.gsg

import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.document.DynamoDB

object MoviesDeleteTable {
  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val client = AmazonDynamoDBClientBuilder.standard.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-west-2")).build
    val dynamoDB = new DynamoDB(client)
    val table = dynamoDB.getTable("Movies")
    try {
      System.out.println("Attempting to delete table; please wait...")
      table.delete
      table.waitForDelete()
      System.out.print("Success.")
    } catch {
      case e: Exception =>
        System.err.println("Unable to delete table: ")
        System.err.println(e.getMessage)
    }
  }
}