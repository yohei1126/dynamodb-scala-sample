package com.amazonaws.codesamples.gsg

import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.PrimaryKey
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec

object MoviesItemOps06 {
  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val client = AmazonDynamoDBClientBuilder.standard.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-west-2")).build
    val dynamoDB = new DynamoDB(client)
    val table = dynamoDB.getTable("Movies")
    val year = 2015
    val title = "The Big New Movie"
    // Conditional delete (we expect this to fail)
    /*val deleteItemSpec = new DeleteItemSpec().
      withPrimaryKey(new PrimaryKey("year", year, "title", title)).
      withConditionExpression("info.rating <= :val").
      withValueMap(new ValueMap().
      withNumber(":val", 5.0))*/
    val deleteItemSpec = new DeleteItemSpec()
      .withPrimaryKey(new PrimaryKey("year", 2015, "title", "The Big New Movie"));
    try {
      System.out.println("Attempting a conditional delete...")
      table.deleteItem(deleteItemSpec)
      System.out.println("DeleteItem succeeded")
    } catch {
      case e: Exception =>
        System.err.println("Unable to delete item: " + year + " " + title)
        System.err.println(e.getMessage)
    }
  }
}