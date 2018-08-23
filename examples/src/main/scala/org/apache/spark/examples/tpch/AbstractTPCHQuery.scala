/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.apache.spark.examples.tpch

import org.apache.spark.sql.DataFrame

case class Customer(
  c_custkey: Int,
  c_name: String,
  c_address: String,
  c_nationkey: Int,
  c_phone: String,
  c_acctbal: Double,
  c_mktsegment: String,
  c_comment: String)

case class Lineitem(
  l_orderkey: Int,
  l_partkey: Int,
  l_suppkey: Int,
  l_linenumber: Int,
  l_quantity: Double,
  l_extendedprice: Double,
  l_discount: Double,
  l_tax: Double,
  l_returnflag: String,
  l_linestatus: String,
  l_shipdate: String,
  l_commitdate: String,
  l_receiptdate: String,
  l_shipinstruct: String,
  l_shipmode: String,
  l_comment: String)

case class Nation(
  n_nationkey: Int,
  n_name: String,
  n_regionkey: Int,
  n_comment: String)

case class Order(
  o_orderkey: Int,
  o_custkey: Int,
  o_orderstatus: String,
  o_totalprice: Double,
  o_orderdate: String,
  o_orderpriority: String,
  o_clerk: String,
  o_shippriority: Int,
  o_comment: String)

case class Part(
  p_partkey: Int,
  p_name: String,
  p_mfgr: String,
  p_brand: String,
  p_type: String,
  p_size: Int,
  p_container: String,
  p_retailprice: Double,
  p_comment: String)

case class Partsupp(
  ps_partkey: Int,
  ps_suppkey: Int,
  ps_availqty: Int,
  ps_supplycost: Double,
  ps_comment: String)

case class Region(
  r_regionkey: Int,
  r_name: String,
  r_comment: String)

case class Supplier(
  s_suppkey: Int,
  s_name: String,
  s_address: String,
  s_nationkey: Int,
  s_phone: String,
  s_acctbal: Double,
  s_comment: String)

abstract class AbstractTPCHQuery(path: String) extends SubmitQuery {

  import spark.implicits._
  import org.apache.spark.sql.functions._

  val lineitem_path: String = path + "/lineitem"
  val part_path: String = path + "/part"
  val cust_path: String = path + "/customer"
  val nation_path: String = path + "/nation"
  val region_path: String = path + "/region"
  val partsupp_path: String = path + "/partsupp"
  val supplier_path: String = path + "/supplier"
  val order_path: String = path + "/orders"

  val lineitem = spark.read.textFile(lineitem_path).map(_.split('|'))
.map(p => Lineitem(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt,
 p(3).trim.toInt, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble,
 p(7).trim.toDouble, p(8).trim, p(9).trim, p(10).trim, p(11).trim,
 p(12).trim, p(13).trim, p(14).trim, p(15).trim)).toDF()

  val part = spark.read.textFile(part_path).map(_.split('|'))
.map(p => Part(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim, p(4).trim,
 p(5).trim.toInt, p(6).trim, p(7).trim.toDouble, p(8).trim)).toDF()

  val customer = spark.read.textFile(cust_path).map(_.split('|'))
.map(p => Customer(p(0).trim.toInt, p(1).trim, p(2).trim, p(3).trim.toInt,
 p(4).trim, p(5).trim.toDouble, p(6).trim, p(7).trim)).toDF()

  val nation = spark.read.textFile(nation_path).map(_.split('|'))
.map(p => Nation(p(0).trim.toInt, p(1).trim, p(2).trim.toInt, p(3).trim)).toDF()

  val region = spark.read.textFile(region_path).map(_.split('|'))
.map(p => Region(p(0).trim.toInt, p(1).trim, p(2).trim)).toDF()

  val partsupp = spark.read.textFile(partsupp_path).map(_.split('|'))
.map(p => Partsupp(p(0).trim.toInt, p(1).trim.toInt, p(2).trim.toInt,
 p(3).trim.toDouble, p(4).trim)).toDF()

  val supplier = spark.read.textFile(supplier_path).map(_.split('|'))
.map(p => Supplier(p(0).trim.toInt, p(1).trim, p(2).trim,
 p(3).trim.toInt, p(4).trim, p(5).trim.toDouble, p(6).trim)).toDF()

  val order = spark.read.textFile(order_path).map(_.split('|'))
.map(p => Order(p(0).trim.toInt, p(1).trim.toInt, p(2).trim,
 p(3).trim.toDouble, p(4).trim, p(5).trim, p(6).trim, p(7).trim.toInt,
 p(8).trim)).toDF()

  def submit(): Unit

}
