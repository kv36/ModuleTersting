import java.io.{IOException, Serializable}
import java.lang.Iterable
import java.math.{BigDecimal, BigInteger}
import java.net.URI
import java.text.{DateFormat, SimpleDateFormat}
import java.util
import java.util.{Comparator, Date, Locale}

import com.clearspring.analytics.util.Lists
import com.datastax.driver.core._
import com.datastax.spark.connector.ColumnRef
import com.datastax.spark.connector.cql.{CassandraConnector, TableDef}
import com.datastax.spark.connector.japi.CassandraJavaUtil._
import com.datastax.spark.connector.writer.{RowWriter, RowWriterFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.api.java.JavaSparkContext._
import org.apache.spark.api.java.function.{Function, PairFunction}
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD, JavaSparkContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

      class Order(var id: Int, var applicationId: Int, var orderId: String, var orderNumber: String, var status: String, var financialStatus: String, var customerIsGuest: Boolean, var customerId: BigInteger, var firstName: String, var lastName: String, var email: String, var subTotalPrice: BigDecimal, var totalDiscounts: BigDecimal, var storeCredit: BigDecimal, var totalPrice: BigDecimal, var currencyCode: String, var source: String, var createdAt: Date, var updatedAt: Date, var customerCreatedAt: Date, var isSynced: Boolean, var billingAddressId: String, var shippingAddressId: String, var created: Date, var modified: Date, var consumerOrderId: Integer, var previousStatus: String, var isPartialData: Boolean) extends Serializable
      {
      this.id = id
      this.applicationId = applicationId
      this.orderId = orderId
      this.orderNumber = orderNumber
      this.status = status
      this.financialStatus = financialStatus
      this.customerIsGuest = customerIsGuest
      this.customerId = customerId
      this.firstName = firstName
      this.lastName = lastName
      this.email = email
      this.subTotalPrice = subTotalPrice
      this.totalDiscounts = totalDiscounts
      this.storeCredit = storeCredit
      this.totalPrice = totalPrice
      this.currencyCode = currencyCode
      this.source = source
      this.createdAt = createdAt
      this.updatedAt = updatedAt
      this.customerCreatedAt = customerCreatedAt
      this.isSynced = isSynced
      this.billingAddressId = billingAddressId
      this.shippingAddressId = shippingAddressId
      this.created = created
      this.modified = modified
      this.consumerOrderId = consumerOrderId
      this.previousStatus = previousStatus
      this.isPartialData = isPartialData

      def getId: Int = id

      def getApplicationId: Integer = int2Integer (applicationId)

      def getOrderId: String = orderId

      def getOrderNumber: String = orderNumber

      def getStatus: String = status

      def getFinancialStatus: String = financialStatus

      def getCustomerIsGuest: Boolean = customerIsGuest

      def getCustomerId: BigInteger = customerId

      def getFirstName: String = firstName

      def getLastName: String = lastName

      def getEmail: String = email

      def getSubTotalPrice: BigDecimal = subTotalPrice

      def getTotalDiscounts: BigDecimal = totalDiscounts

      def getStoreCredit: BigDecimal = storeCredit

      def getTotalPrice: BigDecimal = totalPrice

      def getCurrencyCode: String = currencyCode

      def getSource: String = source

      def getCreatedAt: Date = createdAt

      def getUpdatedAt: Date = updatedAt

      def getCustomerCreatedAt: Date = customerCreatedAt

      def getIsSynced: Boolean = isSynced

      def getBillingAddressId: String = billingAddressId

      def getshippingAddressId: String = shippingAddressId

      def getCreated: Date = created

      def getModified: Date = modified

      def getConsumerOrderId: Integer = consumerOrderId

      def getPreviousStatus: String = previousStatus

      def getIsPartialData: Boolean = isPartialData


        override def toString: String = {
          try {
            val outputDateFormatter: SimpleDateFormat = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss", Locale.US)
            val formattedCreatedAtDate: String = if (createdAt != null) outputDateFormatter.format (createdAt)
            else null
            val formattedUpdatedAtDate: String = if (updatedAt != null) outputDateFormatter.format (updatedAt)
            else null
            val formattedCustomerCreatedAtDate: String = if (customerCreatedAt != null) outputDateFormatter.format (customerCreatedAt)
            else null
            val formattedCreatedDate: String = if (created != null) outputDateFormatter.format (created)
            else null
            val formattedModifiedDate: String = if (modified != null) outputDateFormatter.format (modified)
            else null

           // val customerIsGuestString: String = if (customerIsGuest != null) customerIsGuest.toString else null


           // String.format ("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s", id.toString, applicationId.toString, orderId, orderNumber, status, financialStatus, customerIsGuestString, customerId.toString, firstName, lastName, email, subTotalPrice.toString, totalDiscounts.toString, storeCredit.toString, totalPrice.toString, currencyCode, source, formattedCreatedAtDate, formattedUpdatedAtDate, formattedCustomerCreatedAtDate, isSynced.toString, billingAddressId, shippingAddressId, formattedCreatedDate, formattedModifiedDate, consumerOrderId.toString, previousStatus, isPartialData.toString)
            // String.format(String, id, applicationId, orderId, orderNumber, status, financialStatus, customerIsGuest, customerId, firstName, lastName, email, subTotalPrice, totalDiscounts, storeCredit, totalPrice, currencyCode, source, formattedCreatedAtDate, formattedUpdatedAtDate, formattedCustomerCreatedAtDate, isSynced, billingAddressId, shippingAddressId, formattedCreatedDate, formattedModifiedDate, consumerOrderId, previousStatus, isPartialData)
            s"$id,$applicationId,$orderId,$orderNumber,$status,$financialStatus,$customerIsGuest,$customerId,$firstName,$lastName,$email,$subTotalPrice,$totalDiscounts,$storeCredit,$totalPrice,$currencyCode,$source,$formattedCreatedAtDate,$formattedUpdatedAtDate,$formattedCustomerCreatedAtDate,$isSynced,$billingAddressId,$shippingAddressId,$formattedCreatedDate,$formattedModifiedDate,$consumerOrderId,$previousStatus,$isPartialData"
          }
        catch {
          case ex: Exception =>
            //_logger.error ("Failed to parse order: " + line)
            val message: String = ex.getMessage
            null
        }
      }
    }

       class Customer(var id: BigInteger, var applicationId: Int, var customerId: BigInteger, var customerGroup: String, var firstName: String, var lastName: String, var email: String, var optInNewsletter: Boolean, var createdAt: Date, var updatedAt: Date, var isSynced: Boolean, var created: Date, var modified: Date, var consumerCustomerId: Integer, var isPartialData: Boolean)  {

        this.id = id
        this.applicationId = applicationId
        this.customerId = customerId
        this.customerGroup = customerGroup
        this.firstName = firstName
        this.lastName = lastName
        this.email = email
        this.optInNewsletter = optInNewsletter
        this.createdAt = createdAt
        this.updatedAt = updatedAt
        this.isSynced = isSynced
        this.created = created
        this.modified = modified
        this.consumerCustomerId = consumerCustomerId
        this.isPartialData = isPartialData


        def getId: BigInteger = id

        def getApplicationId: Integer = int2Integer (applicationId)

        def getCustomerId: BigInteger = customerId

        def getCustomerGroup: String = customerGroup

        def getFirstName: String = firstName

        def getLastName: String = lastName

        def getEmail: String = email

        def getOptInNewsletter: Boolean = optInNewsletter

        def getCreatedAt: Date = createdAt

        def getUpdatedAt: Date = updatedAt

        def getIsSynced: Boolean = isSynced

        def getCreated: Date = created

        def getModified: Date = modified

        def getConsumerCustomerId: Integer = consumerCustomerId

        def getIsPartialData: Boolean = isPartialData

         override def toString: String = {

           val outputDateFormatter: SimpleDateFormat = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss", Locale.US)

           val formattedCreatedAtDate: String = if (createdAt != null) outputDateFormatter.format (createdAt)
           else null
           val formattedUpdatedAtDate: String = if (updatedAt != null) outputDateFormatter.format (updatedAt)
           else null
           val formattedCreatedDate: String = if (created != null) outputDateFormatter.format (created)
           else null
           val formattedModifiedDate: String = if (modified != null) outputDateFormatter.format (modified)
           else null

          // String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s", id.toString, applicationId.toString, customerId.toString, customerGroup.toString, firstName, lastName, email, optInNewsletter.toString, formattedCreatedAtDate, formattedUpdatedAtDate, isSynced.toString, formattedCreatedDate, formattedModifiedDate, consumerCustomerId.toString, isPartialData.toString)

           s"$id,$applicationId,$customerId,$customerGroup,$firstName,$lastName,$email,$optInNewsletter,$formattedCreatedAtDate,$formattedUpdatedAtDate,$isSynced,$formattedCreatedDate,$formattedModifiedDate,$consumerCustomerId,$isPartialData"
         }
      }

class LatencyData(var customerId: BigInteger, var FirstAndSecondOrder_Latency: Double, var SecondAndThirdOrder_Latency: Double, var ThirdAndFourthOrder_Latency: Double) extends Serializable {

  def getCustomerId: BigInteger = customerId
  def getFirstAndSecondOrder_Latency: Double = FirstAndSecondOrder_Latency
  def getSecondAndThirdOrder_Latency: Double = SecondAndThirdOrder_Latency
  def getThirdAndFourthOrder_Latency: Double =  ThirdAndFourthOrder_Latency
}


class LatencyDataRowWriter extends RowWriter[LatencyData] {
  override val columnNames = scala.IndexedSeq("customerId", "FirstAndSecondOrder_Latency", "secondAndThirdOrder_Latency", "ThirdAndFourthOrder_Latency")

  override def readColumnValues(latencyData: LatencyData, buffer: Array[Any]) = {
    buffer (0) = latencyData.getCustomerId
    buffer (1) = latencyData.getFirstAndSecondOrder_Latency
    buffer (2) = latencyData.getSecondAndThirdOrder_Latency
    buffer (3) = latencyData.getThirdAndFourthOrder_Latency
  }
}



class LatencyDataRowWriterFactory extends RowWriterFactory[LatencyData] {
  override def rowWriter(table: TableDef, selectedColumns: scala.IndexedSeq[ColumnRef]): RowWriter[LatencyData] = new LatencyDataRowWriter
}



class Latencycalculation extends Serializable {

  val currentDateTimePath: String = getCurrentDateTimePath
  val _cassandraSchemaName: String = "revenue_conduit"
  val _cassandraLatencyResultsTableName: String = "latency_results"
  val _cassandraLatencyLastRunTableName: String = "Latency_last_run"
  val _logger = LoggerFactory.getLogger (classOf [Latencycalculation])


  def getCurrentDateTimePath: String = {

    val dateFormat: DateFormat = new SimpleDateFormat ("yyyy_MM_dd_HH_mm_ss")
    val date: Date = new Date ()
    val currentDateTime: String = dateFormat.format (date)
    currentDateTime
  }


  def insertFirstJobRunDate(session: Session) {
    session.execute ("%s %s %s %s".format ("INSERT INTO %s.%s (id, last_run_date) values (1, '%s')", _cassandraSchemaName, _cassandraLatencyResultsTableName, currentDateTimePath))
  }


  def deleteNewData(path: String, hdfsPath: String) {


    val conf: Configuration = getConfiguration
    val hdfs: FileSystem = FileSystem.get (URI.create (hdfsPath), conf)

     var newpath: String = null

    if (path.endsWith ("*")) {
      newpath = path substring(0, path.length () - 2)
    }

    else {
      newpath = path
    }

    if (hdfs.exists (new Path (newpath)))
    {
      hdfs.delete (new Path (newpath), true)
    }

  }


  def getConfiguration: Configuration = {
    val conf: Configuration = new Configuration

    conf.set ("fs.hdfs.impl", classOf [FileSystem].getName)
    conf.set ("fs.file.impl", classOf [FileSystem].getName)
    conf
  }




  def doesPathExist(path: String, hdfsPath: String): Boolean = {
    val conf: Configuration = getConfiguration
    val hdfs: FileSystem = FileSystem.get (URI.create (hdfsPath), conf)

    var pathWithoutWildcard : String = null

    if (path.endsWith ("*")) {
      pathWithoutWildcard = path substring(0, path.length () - 2)
    }
    else {
      pathWithoutWildcard = path
    }

    if (hdfs.exists (new Path (pathWithoutWildcard)))
      true
    else
      false
  }


  def RemoveEscapeSequences(line: String): String = {

    // This is the escape sequence used to output the data (by default backslash escape commas in the string)
    // e.g. - sqoop --escaped-by \\ ...

    val escapeSequence: String = "\\,"
    var lineWithoutEscapeSequences: String = line

    if (line != null && !line.isEmpty && line.contains (escapeSequence)) {
      val regexEscapeSequence: String = "\\\\,"
      lineWithoutEscapeSequences = line.replaceAll (regexEscapeSequence, "")
    }
    lineWithoutEscapeSequences

  }



  def TryParseBigDecimal(decimalString: String): BigDecimal = {

    var decimal: BigDecimal = null
    val nullString: String = new String ("null")

    if (decimalString != null && !decimalString.isEmpty && decimalString != nullString) {

      decimal = new BigDecimal (decimalString)
    }
    decimal

  }



  def TryParseBigInteger(bigIntValue: String): BigInteger = {

    var bigInteger: BigInteger = null
    val nullString: String = new String ("null")

    if (bigIntValue != null && !bigIntValue.isEmpty && bigIntValue != nullString) {

      bigInteger = new BigInteger (bigIntValue)
    }

    bigInteger
  }



  def TryParseInteger(integerValue: String): Integer = {

    var integer: Integer = null
    val nullString: String = new String ("null")

    if (integerValue != null && !integerValue.isEmpty && integerValue != nullString) {

      integer = integerValue.toInt

    }

    integer
  }



  def TryParseBoolean(boolValue: String): Boolean = {
    var bool: Boolean = false
    val nullString: String = new String ("null")

    if (boolValue != null && !boolValue.isEmpty && boolValue != nullString) {
      bool = boolValue.toBoolean
    }
    bool
  }



  def TryParseDate(dateValue: String, dateFormat: SimpleDateFormat): Date = {


    var date: Date = null
    val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US)

    val nullString: String = new String ("null")

    if (dateValue != null && !dateValue.isEmpty && dateValue != nullString) date = simpleDateFormat.parse (dateValue)
    date
  }



  def GetLatestOrder(order1: Order, order2: Order): Order = {
    var latestOrder: Order = null
    val tuple1OrderCreatedDate: Date = order1.getCreated
    val tuple2OrderCreatedDate: Date = order2.getCreated
    if (tuple1OrderCreatedDate != null && tuple2OrderCreatedDate != null) {
      if (tuple1OrderCreatedDate.compareTo (tuple2OrderCreatedDate) > 0) {
        latestOrder = order1
      }
      else {
        latestOrder = order2
      }
    }
    else if (tuple1OrderCreatedDate != null) {
      latestOrder = order1
    }
    else {
      latestOrder = order2
    }
    latestOrder
  }



  def InitializeCassandra(sc: SparkContext): Unit = {

    var session: Session = null

    val connector: CassandraConnector = CassandraConnector.apply (sc.getConf)

    session = connector.openSession ()

    session.execute (String.format ("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}", _cassandraSchemaName))

    session.execute (String.format ("CREATE TABLE IF NOT EXISTS %s.%s (customerId BigInt, FirstandSecondOrder_Latency Double, SecondandThirdOrder_Latency Double, ThirdandFourthOrderLatency Double, " + "PRIMARY KEY (customerId))", _cassandraSchemaName, _cassandraLatencyResultsTableName))

    session.execute (String.format ("CREATE TABLE IF NOT EXISTS %s.%s (id BigInt PRIMARY KEY, last_run_date TEXT)", _cassandraSchemaName, _cassandraLatencyLastRunTableName))

    //session.execute (String.format ("CREATE TABLE IF NOT EXISTS %s.%s (application_id INT, customerId VARINT, customer_name TEXT, company_name TEXT, customer_group TEXT, customer_city TEXT, customer_state TEXT, customer_country TEXT, customer_email TEXT, " + "orders_sub_total DECIMAL, orders_count INT, first_order_date TIMESTAMP, last_order_date TIMESTAMP, average_days_between_orders INT, first_order_amount DECIMAL, last_order_amount DECIMAL, average_order_price DECIMAL, customer_created_at TIMESTAMP, " + "PRIMARY KEY (application_id, customerId))", _cassandraSchemaName, _cassandraLatencyResultsTableName))
    if (session != null && !session.isClosed)
      session.close ()

  }



  def cleanUpCassandra(sc: SparkContext): Unit = {
    val connector: CassandraConnector = CassandraConnector.apply (sc.getConf)

    val session: Session = connector.openSession ()

    val lastRunDateSet: ResultSet = session.execute (String.format ("SELECT id, last_run_date FROM %s.%s", _cassandraSchemaName, _cassandraLatencyLastRunTableName))

    if (lastRunDateSet != null) {

      val allDates: util.List[Row] = lastRunDateSet.all ()

      val allDatesSize: Integer = allDates.size

      if (allDatesSize > 0) {

        allDates.sort (new Comparator[Row]() {
          def compare(row1: Row, row2: Row): Int = {
            val row1Id: Integer = row1.getInt (0)
            val row2Id: Integer = row2.getInt (0)
            row1Id.compareTo (row2Id)
          }
        })
        val allDatesLastRow: Row = allDates.get (allDatesSize - 1)
        val lastRunId: Integer = allDatesLastRow.getInt (0)
        val lastRunDate: String = allDatesLastRow.getString (1)
        val currentRunId: Integer = Integer2int (lastRunId) + 1

        session.execute (String.format ("INSERT INTO %s.%s (id, last_run_date) values (%s)", _cassandraSchemaName, _cassandraLatencyResultsTableName, currentDateTimePath))

       // var i: Int = 1
        for (i <- 0 to Integer2int (allDatesSize)) {
          {
            val previousRunRow: Row = allDates.get (i)
            val previousRunId: Integer = previousRunRow.getInt (0)
            val previousRunDate: String = previousRunRow.getString (1)
            if (previousRunDate != null && !previousRunDate.isEmpty) {
              val cassandraLastRunLatencyResultsTableName: String = String.format ("latency_results_%s", lastRunDate)
              session.execute (String.format ("DROP TABLE IF EXISTS %s.%s", _cassandraSchemaName, cassandraLastRunLatencyResultsTableName))
            }
          }
        }
      }

      else insertFirstJobRunDate (session)
    }
    else insertFirstJobRunDate (session)

    if (session != null && !session.isClosed) {
      session.close ()
    }

  }




  def parseOrders(ordersPath: String, sc: JavaSparkContext): JavaRDD[Order] = {
    sc.textFile(ordersPath).map[Order](new Function[String, Order]() {
      def call(line: String): Order = {
        try {
          val lineWithoutEscapeCharacters: String = RemoveEscapeSequences (line)
          val fields: Array[String] = lineWithoutEscapeCharacters.split (",")
          val inputDateFormat: SimpleDateFormat = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss", Locale.US)
          if (fields.length == 28) {
            val id: Int = fields (0).toInt
            val applicationId: Int = fields (1).toInt
            val orderId: String = if (fields (2) != null && !fields (2).isEmpty) fields (2).trim
            else null
            val orderNumber: String = if (fields (3) != null && !fields (3).isEmpty) fields (3).trim
            else null
            val status: String = if (fields (4) != null && !fields (4).isEmpty) fields (4).trim
            else null
            val financialStatus: String = if (fields (5) != null && !fields (5).isEmpty) fields (5).trim
            else null
            val customerIsGuest: Boolean = TryParseBoolean (fields (6))
            val customerId: BigInteger = TryParseBigInteger (fields (7))
            val firstName: String = if (fields (8) != null && !fields (8).isEmpty) fields (8).trim
            else null
            val lastName: String = if (fields (9) != null && !fields (9).isEmpty) fields (9).trim
            else null
            val email: String = if (fields (10) != null && !fields (10).isEmpty) fields (10).trim
            else null
            val subTotalPrice: BigDecimal = TryParseBigDecimal (fields (11))
            val totalDiscounts: BigDecimal = TryParseBigDecimal (fields (12))
            val storeCredit: BigDecimal = TryParseBigDecimal (fields (13))
            val totalPrice: BigDecimal = TryParseBigDecimal (fields (14))
            val currencyCode: String = if (fields (15) != null && !fields (15).isEmpty) fields (15).trim
            else null
            val source: String = if (fields (16) != null && !fields (16).isEmpty) fields (16).trim
            else null
            val createdAt: Date = TryParseDate (fields (17), inputDateFormat)
            val updatedAt: Date = TryParseDate (fields (18), inputDateFormat)
            val customerCreatedAt: Date = TryParseDate (fields (19), inputDateFormat)
            val isSynced: Boolean = TryParseBoolean (fields (20))
            val billingAddressId: String = if (fields (21) != null && !fields (21).isEmpty) fields (21).trim
            else null
            val shippingAddressId: String = if (fields (22) != null && !fields (22).isEmpty) fields (22).trim
            else null
            val created: Date = TryParseDate (fields (23), inputDateFormat)
            val modified: Date = TryParseDate (fields (24), inputDateFormat)
            val consumerOrderId: Integer = TryParseInteger (fields (25))
            val previousStatus: String = if (fields (26) != null && !fields (26).isEmpty) fields (26).trim
            else null
            val isPartialData: Boolean = TryParseBoolean (fields (27))
            val order: Order = new Order (id, applicationId, orderId, orderNumber, status, financialStatus, customerIsGuest, customerId, firstName, lastName, email, subTotalPrice, totalDiscounts, storeCredit, totalPrice, currencyCode, source, createdAt, updatedAt, customerCreatedAt, isSynced, billingAddressId, shippingAddressId, created, modified, consumerOrderId, previousStatus, isPartialData)
            order
          }
          else {
            val errorMessage: String = "%s %s %s".format ("Order '%s' cannot be parsed due to invalid length of %s.", line, fields.length)
            _logger.error (errorMessage)
            null
          }
        }
        catch {
          case ex: Exception =>
            _logger.error ("Failed to parse order: " + line)
            null
        }
      }

    })
  }

  def parseCustomerData(customersPath: String, sc: JavaSparkContext): JavaRDD[Customer] = {
    sc.textFile(customersPath).map[Customer](new Function[String, Customer]() {
      @throws[Exception]
      def call(line: String): Customer = {
        try {
          val lineWithoutEscapeCharacters: String = RemoveEscapeSequences (line)
          println(lineWithoutEscapeCharacters)
          val fields: Array[String] = lineWithoutEscapeCharacters.split (",")
          val inputDateFormat: SimpleDateFormat = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss", Locale.US)
          if (fields.length == 15) {
            val id: BigInteger = TryParseBigInteger (fields (0))
            val applicationId: Int = fields (1).toInt
            val customerId: BigInteger = TryParseBigInteger (fields (2))
            val customerGroup: String = if (fields (3) != null && !fields (3).isEmpty) fields (3).trim
            else null
            val firstName: String = if (fields (4) != null && !fields (4).isEmpty) fields (4).trim
            else null
            val lastName: String = if (fields (5) != null && !fields (5).isEmpty) fields (5).trim
            else null
            val email: String = if (fields (6) != null && !fields (6).isEmpty) fields (6).trim
            else null
            val optInNewsletter: Boolean = TryParseBoolean (fields (7))
            val createdAt: Date = TryParseDate (fields (8), inputDateFormat)
            val updatedAt: Date = TryParseDate (fields (9), inputDateFormat)
            val isSynced: Boolean = TryParseBoolean (fields (10))
            val created: Date = TryParseDate (fields (11), inputDateFormat)
            val modified: Date = TryParseDate (fields (12), inputDateFormat)
            val consumerCustomerId: Integer = TryParseInteger (fields (13))
            val isPartialData: Boolean = TryParseBoolean (fields (14))
            val customer: Customer = new Customer (id, applicationId, customerId, customerGroup, firstName, lastName, email, optInNewsletter, createdAt, updatedAt, isSynced, created, modified, consumerCustomerId, isPartialData)
            customer
          }
          else {
            val errorMessage: String = "%s %s %s".format ("Customer '%s' cannot be parsed due to invalid length of %s.", lineWithoutEscapeCharacters, fields.length)
            _logger.error (errorMessage)
            null
          }
        }
        catch {
          case ex: Exception =>
            _logger.error ("Failed to parse customer: " + line)
            null
        }
      }
    })
  }

  def partitionCustomersByApplication(sc: JavaSparkContext, customersDataCsv: JavaRDD[Customer], currentDateTime: String, masterDataPath: String) {
    val customersPairByApplicationIdRdd: JavaPairRDD[String, String] = customersDataCsv.mapToPair (new PairFunction[Customer, String, String]() {
      @throws[Exception]
      def call(customer: Customer): (String, String) = {

        if (customer != null && customer.getApplicationId != null) {
          new Tuple2[String, String](customer.getApplicationId.toString, customer.toString)
        }
        else {
          new Tuple2[String, String](null, null)
        }
      }
    })
    val customersPairGroupedByApplicationIdRdd: JavaPairRDD[String, Iterable[String]] = customersPairByApplicationIdRdd.groupByKey

    val keys: util.List[String] = customersPairGroupedByApplicationIdRdd.keys.collect

    val values: util.List[Iterable[String]] = customersPairGroupedByApplicationIdRdd.values.collect


    for (i <- 0 until keys.size ) {

        val applicationIdKey: String = keys.get (i)

        if (applicationIdKey != null) {
          val customersByAppValuesList: util.List[String] = Lists.newArrayList (values.get (i))
          val customersByAppValuesRdd: JavaRDD[String] = sc.parallelize (customersByAppValuesList)

          val customersPath: String = String.format ("%s/customers/%s/%s/", masterDataPath, applicationIdKey.toString, currentDateTime)

          customersByAppValuesRdd.saveAsTextFile (customersPath)
        }
    }
  }




  def partitionOrdersByApplication(sc: JavaSparkContext, ordersDataCsv: JavaRDD[Order], currentDateTime: String, masterDataPath: String) {
    val ordersPairByApplicationIdRdd: JavaPairRDD[String, String] = ordersDataCsv.mapToPair (new PairFunction[Order, String, String]() {
      @throws[Exception]
      def call(order: Order): (String, String) = {
        if (order != null && order.getApplicationId != null) {
          new Tuple2[String, String](order.getApplicationId.toString, order.toString)
        }
        else {
          new Tuple2[String, String](null, null)
        }
      }
    })
    val ordersPairGroupedByApplicationIdRdd: JavaPairRDD[String, Iterable[String]] = ordersPairByApplicationIdRdd.groupByKey

    val keys: util.List[String] = ordersPairGroupedByApplicationIdRdd.keys.collect

    val values: util.List[Iterable[String]] = ordersPairGroupedByApplicationIdRdd.values.collect

    //var i: Int = 1

    for ( i <- 0 until keys.size) {

        val applicationIdKey: String = keys.get (i)
        if (applicationIdKey != null) {
          val ordersByAppValuesList: util.List[String] = Lists.newArrayList (values.get (i))
          val ordersByAppValuesRdd: JavaRDD[String] = sc.parallelize (ordersByAppValuesList)
          val ordersPath: String = String.format ("%s/orders/%s/%s/", masterDataPath, applicationIdKey.toString, currentDateTime)
          ordersByAppValuesRdd.saveAsTextFile (ordersPath)
        }
    }
  }




  def getApplicationIds(masterDataPath: String, hdfsPath: String): util.ArrayList[Integer] = {
    val applicationIds: util.ArrayList[Integer] = new util.ArrayList[Integer]
    try {
      val customersPath: String = String.format ("%s/customers", masterDataPath)
      if (doesPathExist (customersPath, hdfsPath)) {
        val conf: Configuration = getConfiguration
        val hdfs: FileSystem = FileSystem.get (URI.create (hdfsPath), conf)
        val status: Array[FileStatus] = hdfs.listStatus (new Path (customersPath))

        for (i <- status.indices) {
          {
            val directoryName: String = status (i).getPath.getName
            val applicationId: Integer = TryParseInteger (directoryName)
            if (applicationId != null) {
              val customersByAppId: String = String.format ("%s/customers/%s", masterDataPath, applicationId)
              val ordersPathByAppId: String = String.format ("%s/orders/%s", masterDataPath, applicationId)
              if (doesPathExist (customersByAppId, hdfsPath) && doesPathExist (ordersPathByAppId, hdfsPath)) {
                applicationIds.add (applicationId)
              }
            }
          }
        }
      }
    }
    catch {
      case ex: IOException =>
        _logger.error (String.format ("Getting application id's failed: %s.", ex.getMessage))
        throw ex
    }
    applicationIds
  }



  def ingestNewData(hdfsPath: String, masterDataPath: String, customersPath: String, ordersPath: String, sc: SparkContext, currentDateTimePath: String) {

    // var customersDataCsv: JavaRDD[Customer] = null


    // If there is new data for customers, then parse that data, partition it by application id into the master data set, then delete the new data

    if (doesPathExist (customersPath, hdfsPath)) {
      val customersDataCsv: JavaRDD[Customer] = parseCustomerData (customersPath, sc)
      partitionCustomersByApplication (JavaSparkContext.fromSparkContext (sc), customersDataCsv, currentDateTimePath, masterDataPath)
      deleteNewData (customersPath, masterDataPath)
    }

    // If there is new data for orders, then parse that data, partition it by application id into the master data set, then delete the new data

    if (doesPathExist (ordersPath, hdfsPath)) {
     val ordersDataCsv: JavaRDD[Order] = parseOrders (ordersPath, sc)
      partitionOrdersByApplication (sc, ordersDataCsv, currentDateTimePath, masterDataPath)
      deleteNewData (ordersPath, masterDataPath)
    }
  }



  def calculateLatency(sc: SparkContext, applicationId: Int, customersMasterDataPath: String, ordersMasterDataPath: String, LatencyOutputResultspath: String): JavaRDD[LatencyData] = {


    // Load up customers and orders as JavaSpark RDD's

    val customersDataCsv: JavaRDD[Customer] = parseCustomerData(customersMasterDataPath, fromSparkContext (sc))
    val ordersDataCsv: JavaRDD[Order] = parseOrders(ordersMasterDataPath, fromSparkContext (sc))

    // Map customers to a JavaPairRDD of (id(customer id) => Customer)




    val customersPairRdd: JavaPairRDD[BigInteger, Customer] = customersDataCsv.mapToPair[BigInteger, Customer](new PairFunction[Customer, BigInteger, Customer]() {
      @throws[Exception]
      def call(customer: Customer): (BigInteger, Customer) = {
        if (customer != null) {
          new Tuple2[BigInteger, Customer](customer.getId, customer)
        }
        else {
          new Tuple2[BigInteger, Customer](null, null)
        }
      }
    })


    // Map orders to a JavaPairRDD of (customer id => Order)
    val ordersPairRDD: JavaPairRDD[BigInteger, Order] = ordersDataCsv.mapToPair[BigInteger, Order](new PairFunction[Order, BigInteger, Order]() {
      def call(order: Order): (BigInteger, Order) = {
        if (order != null) {
          new Tuple2[BigInteger, Order](order.getCustomerId, order)
        }
        else {
          new Tuple2[BigInteger, Order](null, null)
        }
      }
    })



    val sortedOrdersPairRDD = ordersPairRDD.sortBy (f => f._2.getCreatedAt, ascending = false).groupByKey ()

    val latencyDataJavaRdd : JavaRDD[LatencyData] = sortedOrdersPairRDD.map(x => {
      val customerId : BigInteger = x._1
      val orders = x._2.toList

      val FirstOrderDate : Date = if (orders.size >= 1 && orders(0) != null) orders(0).getCreatedAt else null
      val SecondOrderDate : Date = if (orders.size >= 2 && orders(1) != null) orders(1).getCreatedAt else null
      val ThirdOrderDate : Date = if (orders.size >= 3 && orders(2) != null) orders(2).getCreatedAt else null
      val FourthOrderDate : Date = if (orders.size >= 4 && orders(3) != null) orders(3).getCreatedAt else null

      var FirstAndSecondOrder_Latency : Double = 0
      var SecondAndThirdOrder_Latency : Double = 0
      var ThirdAndFourthOrder_Latency : Double = 0
      val daysDivisor: Int = 24 * 60 * 60 * 1000

      if (SecondOrderDate != null && FirstOrderDate != null) {

        //val diff = (SecondOrderDate.getTime - FirstOrderDate.getTime)
        //FirstAndSecondOrder_Latency = TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS) / daysDivisor;
        FirstAndSecondOrder_Latency = ((SecondOrderDate.getTime - FirstOrderDate.getTime) / daysDivisor).abs
      }

      if (ThirdOrderDate != null && SecondOrderDate != null) {
        SecondAndThirdOrder_Latency = ((ThirdOrderDate.getTime - SecondOrderDate.getTime) / daysDivisor).abs
      }

      if (FourthOrderDate != null && ThirdOrderDate != null) {
        ThirdAndFourthOrder_Latency = ((FourthOrderDate.getTime - ThirdOrderDate.getTime) / daysDivisor).abs
      }

      val latencyData : LatencyData = new LatencyData(customerId, FirstAndSecondOrder_Latency, SecondAndThirdOrder_Latency, ThirdAndFourthOrder_Latency)

      latencyData
    })

    latencyDataJavaRdd

//    val zeroValue: (BigInteger, Date, Date, Date, Date) = Tuple5 [BigInteger, Date, Date, Date, Date](null, null, null, null, null)





//    def seqOp(Tuple2: (BigInteger, Order), Tuple5: (BigInteger, Date, Date, Date, Date)) = {
//      val customerId = Tuple2._1
//      val order: Order = Tuple2._2
//
//      if (Tuple5 == null || Tuple5 == zeroValue) {
//        new Tuple5 (customerId, order.getCreated, null, null, null)
//      }
//      else {
//        if (Tuple5._3 == null) {
//          new Tuple5 (customerId, Tuple5._2, order.getCreated, null, null)
//        }
//        else if (Tuple5._4 == null) {
//          new Tuple5 (customerId, Tuple5._1, Tuple5._2, order.getCreated, null)
//        }
//        else if (Tuple5._5 == null) {
//          new Tuple5 (customerId, Tuple5._1, Tuple5._2, Tuple5._3, order.getCreated)
//        }
//      }
//    }


//    def combinerFunc(x: (BigInteger, Date, Date, Date, Date), y: (BigInteger, Date, Date, Date, Date)) : (BigInteger, Date, Date, Date, Date) = {
//      null
//    }
//

//    val AggregatedsortedorderspairRDD = sortedOrdersPairRDD.aggregateByKey(zeroValue)((x, y) => {
//      val customerId = x._1
//      val order: Order = y.head
//
//      if (Tuple5 == null) {
//        new Tuple5 (customerId, order.getCreated, null, null, null)
//      }
//      else {
//        if (x._3 == null) {
//          new Tuple5 (customerId, x._2, order.getCreated, null, null)
//        }
//        else if (x._4 == null) {
//          new Tuple5 (customerId, x._1, x._2, order.getCreated, null)
//        }
//        else if (x._5 == null) {
//          new Tuple5 (customerId, x._1, x._2, x._3, order.getCreated)
//        }
//        null
//      }
//    }, combinerFunc)


//    val latencyDataJavaRdd : JavaRDD[LatencyData] = AggregatedsortedorderspairRDD.map(x =>
//    {
//      val customerId : BigInteger = x._1
//      val FirstOrderDate : Date = x._2._2
//      val SecondOrderDate : Date = x._2._3
//      val ThirdOrderDate : Date = x._2._4
//      val FourthOrderDate : Date = x._2._5
//
//      var FirstAndSecondOrder_Latency : Double = 0
//      var SecondAndThirdOrder_Latency : Double = 0
//      var ThirdAndFourthOrder_Latency : Double = 0
//      val daysDivisor: Int = 24 * 60 * 60 * 1000
//
//      if (SecondOrderDate != null && FirstOrderDate != null) {
//        FirstAndSecondOrder_Latency = (SecondOrderDate.getTime - FirstOrderDate.getTime) / daysDivisor
//      }
//
//      if (ThirdOrderDate != null && SecondOrderDate != null) {
//        SecondAndThirdOrder_Latency = (ThirdOrderDate.getTime - SecondOrderDate.getTime) / daysDivisor
//      }
//
//      if (FourthOrderDate != null && ThirdOrderDate != null) {
//        ThirdAndFourthOrder_Latency = (FourthOrderDate.getTime - ThirdOrderDate.getTime) / daysDivisor
//      }
//
//      val latencyData : LatencyData = new LatencyData(customerId, FirstAndSecondOrder_Latency, SecondAndThirdOrder_Latency, ThirdAndFourthOrder_Latency)
//
//      latencyData
//    })

//   // latencyDataJavaRdd.saveAsTextFile(LatencyOutputResultspath)
//    latencyDataJavaRdd

  }
}


  object Sample {
    def main(args: Array[String]) = {

      //var i: Int = 0


      val hdfsPath: String = args (0)
      val masterDataPath: String = args (1)
      val cassandraHostname: String = args (2)
      val cassandraUsername: String = args (3)
      val cassandraPassword: String = args (4)
      val customersPath: String = args (5)
      val ordersPath: String = args (6)
      val LatencyResultsPath: String = args (7)
      val _cassandraSchemaName: String = "revenue_conduit"
      val _cassandraLatencyResultsTableName: String = "latency_results"

      val _logger = LoggerFactory.getLogger (classOf [Latencycalculation])


      _logger.info ("Starting LatencyCalc.")
      _logger.info ("hdfsPath:" + hdfsPath)
      _logger.info ("masterDataPath:" + masterDataPath)
      _logger.info ("cassandraHostname:" + cassandraHostname)
      _logger.info ("customersPath:" + customersPath)
      _logger.info ("ordersPath:" + ordersPath)
      _logger.info ("LatencyResultsPath:" + LatencyResultsPath)

      // create Spark configuration

      val conf: SparkConf = new SparkConf (true).setAppName (classOf [Latencycalculation].getSimpleName)
        .set ("spark.cassandra.connection.host", cassandraHostname)
        .set ("spark.cassandra.auth.username", cassandraUsername)
        .set ("spark.cassandra.auth.password", cassandraPassword)
        .set ("spark.cassandra.connection.timeout_ms", "300000") // 5 minutes
        .set ("spark.cassandra.connection.keep_alive_ms", "300000") // 5 minutes
        .set ("spark.cassandra.connection.compression", "SNAPPY")

      val sc: JavaSparkContext = new JavaSparkContext (conf)
      //val latdata = new LatencyData
      val LatencyCalc = new Latencycalculation
      LatencyCalc.getConfiguration
      val currentDateTimePath: String = LatencyCalc.getCurrentDateTimePath
      LatencyCalc.InitializeCassandra (JavaSparkContext.toSparkContext (sc))
      //LatencyCalc.cleanUpCassandra (JavaSparkContext.toSparkContext (sc))
     // LatencyCalc.InitializeCassandra (JavaSparkContext.toSparkContext (sc))
      //LatencyCalc.cleanUpCassandra (JavaSparkContext.toSparkContext (sc))

      // Partition new data from the database into the master data set

       LatencyCalc.ingestNewData (hdfsPath, masterDataPath, customersPath, ordersPath, sc, currentDateTimePath)

      val applicationIds: util.ArrayList[Integer] = LatencyCalc.getApplicationIds (masterDataPath, hdfsPath)

      //Loop through the each application and Run Latency Calculations on it based on the master data sets customers, and orders for that application.

      if (applicationIds != null && applicationIds.size > 0) {
        _logger.info ("%s %s".format ("Starting Latency calculation for %s applications.", applicationIds.size ()))

        // Set hadoop to read input directory recursively so we get all customers and orders for the entire master data set for the given application.

        sc.hadoopConfiguration.set ("mapreduce.input.fileinputformat.input.dir.recursive", "true")


        //Loop through the each application and Run Latency Calculations on it based on the master data sets customers, and orders for that application.

        // Loop through each application and calculate its Latency data and save it.
        for (i <- 0 until applicationIds.size ) {
          val applicationId: Integer = applicationIds.get (i)
          val customersMasterDataPath: String = String.format ("%s/customers/%s/*", masterDataPath, applicationId)
          val ordersMasterDataPath: String = String.format ("%s/orders/%s/*", masterDataPath, applicationId)
          val LatencyOutputResultsPath: String = String.format ("%s%s/%s", LatencyResultsPath, applicationId, currentDateTimePath)


          // Calculate Latency data for the customer

          val FinalLatencyDataRdd: JavaRDD[LatencyData]  = LatencyCalc.calculateLatency (sc, Integer2int (applicationId), customersMasterDataPath, ordersMasterDataPath, LatencyOutputResultsPath)
          FinalLatencyDataRdd.cache ()
          FinalLatencyDataRdd.saveAsTextFile(LatencyOutputResultsPath)

          // Save the customers' Latency data to Cassandra
          javaFunctions (FinalLatencyDataRdd).writerBuilder (_cassandraSchemaName, _cassandraLatencyResultsTableName, new LatencyDataRowWriterFactory).saveToCassandra ()


        }
        LatencyCalc.cleanUpCassandra (sc)
        _logger.info ("Latency Data calculation is complete")
      }
      sc.stop ()
      _logger.info ("LatencyCalculation is complete")


      new LatencyDataRowWriter
      new LatencyDataRowWriterFactory




      val sdf: SimpleDateFormat = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss")
      val d1: Date = sdf.parse ("2016-10-10 10:10:10")
      val d2: Date = sdf.parse ("2016-10-10 10:10:10")
      val testCust = new Customer (new BigInteger ("123"), 123, new BigInteger ("234"), "abcd", "Kartheek", "Vad", "kv@gmail.com", false, d1, d2, true, d1, d2, int2Integer (457), false)
      //val testOrder = new Order (123, 368, "145667", "kartheek", "paid", "later", true, new BigInteger ("1243"), "kartheek", "vadlamani", "kv36@zips.uakron.edu", new BigDecimal ("124.56"), new BigDecimal ("65.46"), new BigDecimal ("155.88"), new BigDecimal ("143.43"), "USD", "No Idea", d3, d4, d5, false, "Fir Hills", "same", d3, d4, 345, "Crazy Idea", true)


      val ID = testCust.getId
      val appId = testCust.getApplicationId
      val custId = testCust.customerId
      val custgroup = testCust.getCustomerGroup
      val Ftname = testCust.getFirstName
      val ltname = testCust.getLastName
      val eml = testCust.getEmail
      val onl = testCust.getOptInNewsletter
      val Ct = testCust.getCreatedAt
      val ut = testCust.getUpdatedAt
      val is = testCust.getIsSynced
      val cret = testCust.getCreated
      val mod = testCust.getModified
      val cci = testCust.getConsumerCustomerId
      val ipd = testCust.getIsPartialData



      val d3: Date = sdf.parse ("2016-10-10 10:10:10")
      val d4: Date = sdf.parse ("2016-10-10 10:10:10")
      val d5: Date = sdf.parse ("2016-10-10 10:10:10")
      val testOrder = new Order (123, 368, "145667", "kartheek", "paid", "later", true, new BigInteger ("1243"), "kartheek", "vadlamani", "kv36@zips.uakron.edu", new BigDecimal ("124.56"), new BigDecimal ("65.46"), new BigDecimal ("155.88"), new BigDecimal ("143.43"), "USD", "No Idea", d3, d4, d5, false, "Fir Hills", "same", d3, d4, 345, "Crazy Idea", true)

      println (testOrder)
      val idd = testOrder.getId
      val applicId = testOrder.getApplicationId
      val odId = testOrder.getOrderId
      val ordnum = testOrder.getOrderNumber
      val stat = testOrder.getStatus

    }

  }



