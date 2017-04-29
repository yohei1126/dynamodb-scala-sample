package com.amazonaws.codesamples.gsg

import java.util

import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder

object MoviesCreateTable {
  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val client = AmazonDynamoDBClientBuilder.standard.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-west-2")).build
    val dynamoDB = new DynamoDB(client)
    val tableName = "Movies"
    try {
      System.out.println("Attempting to create table; please wait...")
      val table = dynamoDB.createTable(tableName, util.Arrays.asList(new KeySchemaElement("year", KeyType.HASH), // Partition
        // key
        new KeySchemaElement("title", KeyType.RANGE)), // Sort key
        util.Arrays.asList(
          new AttributeDefinition("year", ScalarAttributeType.N),
          new AttributeDefinition("title", ScalarAttributeType.S)),
          new ProvisionedThroughput(10L, 10L))
      table.waitForActive
      System.out.println("Success.  Table status: " + table.getDescription.getTableStatus)
    } catch {
      case e: Exception =>
        System.err.println("Unable to create table: ")
        System.err.println(e.getMessage)
    }
  }
}