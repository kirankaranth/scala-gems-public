package io.prophecy.scalagems.dataset

import ai.x.play.json.Encoders.encoder
import ai.x.play.json.Jsonx
import io.prophecy.gems._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.datasetSpec._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.uiSpec._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json._

import scala.util.Try

class jdbc extends DatasetSpec {

  val name: String = "jdbc"
  val datasetType: String = "Warehouse"
  val docUrl: String = "https://docs.prophecy.io/low-code-spark/gems/source-target/warehouse/jdbc"

  type PropertiesType = JDBCProperties

  // TODO:
  //  These 4 props are defined but not used in any binding in the dialogs: customSchema, keytab, principal, refreshKrb5Config
  case class JDBCProperties(
    @Property("Schema")
    schema: Option[StructType] = None,
    @Property("Description")
    description: Option[String] = Some(""),
    @Property("Credential Type")
    credType: String = "databricksSecrets",
    @Property("Credential Scope")
    credentialScope: Option[String] = None,
    @Property("Username")
    textUsername: Option[String] = None,
    @Property("Password")
    textPassword: Option[String] = None,
    @Property("jdbcUrl", "")
    jdbcUrl: String = "",
    @Property("readFromSource")
    readFromSource: String = "dbtable",
    @Property("dbtable")
    dbtable: Option[String] = None,
    @Property("query", "The query to read from in JDBC")
    query: Option[String] = None,
    @Property("driver")
    driver: String = "",
    @Property("partitionColumn", "Partitioning column.")
    partitionColumn: Option[String] = None,
    @Property("lowerbound")
    lowerBound: Option[String] = None,
    @Property("upperbound")
    upperBound: Option[String] = None,
    @Property("numPartitions")
    numPartitions: Option[String] = None,
    @Property(
      "queryTimeout",
      "The number of seconds the driver will wait for a Statement object to execute to the given number of seconds. Zero means there is no limit. In the write path, this option depends on how JDBC drivers implement the API setQueryTimeout, e.g., the h2 JDBC driver checks the timeout of each query instead of an entire JDBC batch."
    )
    queryTimeout: Option[String] = None,
    @Property(
      "fetchsize",
      "The JDBC fetch size, which determines how many rows to fetch per round trip. This can help performance on JDBC drivers which default to low fetch size (e.g. Oracle with 10 rows)."
    )
    fetchsize: Option[String] = None,
    @Property(
      "batchsize",
      "The JDBC batch size, which determines how many rows to insert per round trip. This can help performance on JDBC drivers. This option applies only to writing."
    )
    batchsize: Option[String] = None,
    @Property(
      "isolationLevel",
      "The transaction isolation level, which applies to current connection. It can be one of NONE, READ_COMMITTED, READ_UNCOMMITTED, REPEATABLE_READ, or SERIALIZABLE, corresponding to standard transaction isolation levels defined by JDBC's Connection object, with default of READ_UNCOMMITTED. Please refer the documentation in java.sql.Connection."
    )
    isolationLevel: Option[String] = None,
    @Property(
      "sessionInitStatement",
      "After each database session is opened to the remote DB and before starting to read data, this option executes a custom SQL statement (or a PL/SQL block)."
    )
    sessionInitStatement: Option[String] = None,
    @Property(
      "truncate",
      "When SaveMode.Overwrite is enabled, this option causes Spark to truncate an existing table instead of dropping and recreating it. This can be more efficient, and prevents the table metadata (e.g., indices) from being removed. However, it will not work in some cases, such as when the new data has a different schema. In case of failures, users should turn off truncate option to use DROP TABLE again. Also, due to the different behavior of TRUNCATE TABLE among DBMS, it's not always safe to use this. MySQLDialect, DB2Dialect, MsSqlServerDialect, DerbyDialect, and OracleDialect supports this while PostgresDialect and default JDBCDirect doesn't. For unknown and unsupported JDBCDirect, the user option truncate is ignored."
    )
    truncate: Option[Boolean] = None,
    @Property(
      "cascadeTruncate",
      "If enabled and supported by the JDBC database (PostgreSQL and Oracle at the moment), this options allows execution of a TRUNCATE TABLE t CASCADE (in the case of PostgreSQL a TRUNCATE TABLE ONLY t CASCADE is executed to prevent inadvertently truncating descendant tables). This will affect other tables, and thus should be used with care."
    )
    cascadeTruncate: Option[Boolean] = None,
    @Property(
      "createTableOptions",
      "this option allows setting of database-specific table and partition options when creating a table (e.g., CREATE TABLE t (name string) ENGINE=InnoDB.)."
    )
    createTableOptions: Option[String] = None,
    @Property(
      "createTableColumnTypes",
      "The database column data types to use instead of the defaults, when creating the table. Data type information should be specified in the same format as CREATE TABLE columns syntax (e.g: \"name CHAR(64), comments VARCHAR(1024)\"). The specified types should be valid spark sql data types."
    )
    createTableColumnTypes: Option[String] = None,
    @Property(
      "customSchema",
      "The custom schema to use for reading data from JDBC connectors. For example, \"id DECIMAL(38, 0), name STRING\". You can also specify partial fields, and the others use the default type mapping. For example, \"id DECIMAL(38, 0)\". The column names should be identical to the corresponding column names of JDBC table. Users can specify the corresponding data types of Spark SQL instead of using the defaults."
    )
    customSchema: Option[String] = None,
    @Property(
      "pushDownPredicate",
      "The option to enable or disable predicate push-down into the JDBC data source. The default value is true, in which case Spark will push down filters to the JDBC data source as much as possible. Otherwise, if set to false, no filter will be pushed down to the JDBC data source and thus all filters will be handled by Spark. Predicate push-down is usually turned off when the predicate filtering is performed faster by Spark than by the JDBC data source."
    )
    pushDownPredicate: Option[Boolean] = Some(true),
    @Property(
      "pushDownAggregate",
      "The option to enable or disable aggregate push-down into the JDBC data source. The default value is false, in which case Spark will not push down aggregates to the JDBC data source. Otherwise, if sets to true, aggregates will be pushed down to the JDBC data source. Aggregate push-down is usually turned off when the aggregate is performed faster by Spark than by the JDBC data source. Please note that aggregates can be pushed down if and only if all the aggregate functions and the related filters can be pushed down. Spark assumes that the data source can't fully complete the aggregate and does a final aggregate over the data source output.\t"
    )
    pushDownAggregate: Option[Boolean] = None,
    @Property(
      "keytab",
      "Location of the kerberos keytab file (which must be pre-uploaded to all nodes either by --files option of spark-submit or manually) for the JDBC client. When path information found then Spark considers the keytab distributed manually, otherwise --files assumed. If both keytab and principal are defined then Spark tries to do kerberos authentication."
    )
    keytab: Option[String] = None,
    @Property(
      "principal",
      "Specifies kerberos principal name for the JDBC client. If both keytab and principal are defined then Spark tries to do kerberos authentication.\t"
    )
    principal: Option[String] = None,
    @Property(
      "refreshKrb5Config",
      "This option controls whether the kerberos configuration is to be refreshed or not for the JDBC client before establishing a new connection."
    )
    refreshKrb5Config: Option[Boolean] = None,
    @Property("Write Mode", """(default: "error") Specifies the behavior when data or table already exists.""")
    writeMode: Option[String] = None,
    @Property("preSQL", "SQL to be executed before loading")
    preSQL: Option[String] = None,
    @Property("postSQL", "SQL to be executed after loading")
    postSQL: Option[String] = None
  ) extends DatasetProperties
  
  implicit val jdbcPropertiesFormat: Format[JDBCProperties] = Jsonx.formatCaseClass[JDBCProperties]
  def sourceDialog: DatasetDialog = {
    val fieldPicker = FieldPicker(height = Some("100%"))
      .addField(
        TextArea("Description", 2, placeholder = "Dataset description..."),
        "description",
        true
      )

    def addCommonFields(fp: FieldPicker): FieldPicker = {
      fp.addField(TextBox("Driver").bindPlaceholder("org.postgresql.Driver"), "driver", true)
        .addField(TextBox("Number of Partitions").bindPlaceholder(""), "numPartitions")
        .addField(TextBox("Query Timeout").bindPlaceholder(""), "queryTimeout")
        .addField(TextBox("Fetch Size").bindPlaceholder(""), "fetchsize")
        // todo check what to use in place of text box here
        .addField(
          TextBox("Session Init Statement")
            .bindPlaceholder(
              "BEGIN execute immediate 'alter session set \"_serial_direct_read\"=true'; END;"
            ),
          "sessionInitStatement"
        )
        // todo check about these checkboxes. One of them has default true, another has false.
        .addField(Checkbox("Push-down Predicate"), "pushDownPredicate")
        .addField(Checkbox("Push-down Aggregate"), "pushDownAggregate")
        .addField(
          TextArea("Execute Pre Query", 3, placeholder = "update logs set action='loading_start' where job_id=101"),
          "preSQL"
        )
        .addField(
          TextArea("Execute Post Query", 3, placeholder = "update logs set action='loading_end' where job_id=101"),
          "postSQL"
        )
    }

    val sqlQueryFieldPicker = addCommonFields(fieldPicker)
    val dbTableFieldPicker =
      addCommonFields(
        fieldPicker
          .addField(
            SchemaColumnsDropdown("Partition Column")
              .bindSchema("schema")
              .showErrorsFor("partitionColumn")
              .allowClearSelection(),
            "partitionColumn",
            true
          )
          .addField(TextBox("Lower Bound").bindPlaceholder(""), "lowerBound", true)
          .addField(TextBox("Upper Bound").bindPlaceholder(""), "upperBound", true)
      )

    DatasetDialog("jdbc")
      .addSection(
        "LOCATION",
        ColumnsLayout()
          .addColumn(
            StackLayout(direction = Some("vertical"), gap = Some("1rem"))
              //            .addElement(TitleElement(title = "Credentials"))
              .addElement(
                StackLayout()
                  .addElement(
                    RadioGroup("Credentials")
                      .addOption("Databricks Secrets", "databricksSecrets")
                      .addOption("Username & Password", "userPwd")
                      .addOption("Environment variables", "userPwdEnv")
                      .bindProperty("credType")
                  )
                  .addElement(
                    Condition()
                      .ifEqual(PropExpr("component.properties.credType"), StringExpr("databricksSecrets"))
                      .then(Credentials("").bindProperty("credentialScope"))
                      .otherwise(
                        StackLayout()
                          .addElement(
                            ColumnsLayout(gap = Some("1rem"))
                              .addColumn(TextBox("Username").bindPlaceholder("username").bindProperty("textUsername"))
                              .addColumn(
                                TextBox("Password")
                                  .isPassword()
                                  .bindPlaceholder("password")
                                  .bindProperty("textPassword")
                              )
                          )
                          .addElement(
                            ColumnsLayout()
                              .addColumn(
                                AlertBox(
                                  children = List(
                                    Markdown("Storing plain-text passwords poses a security risk and is not recommended. Please see [here](https://docs.prophecy.io/low-code-spark/best-practices/use-dbx-secrets) for suggested alternatives")
                                  )
                                )
                              )
                          )
                      )
                  )
              )
              .addElement(TitleElement(title = "URL"))
              .addElement(
                TextBox("JDBC URL")
                  .bindPlaceholder("jdbc:<sqlserver>://<jdbcHostname>:<jdbcPort>/<jdbcDatabase>")
                  .bindProperty("jdbcUrl")
              )
              //            .addElement(TitleElement(title = "Table"))
              .addElement(
                StackLayout()
                  .addElement(
                    RadioGroup("Data Source")
                      .addOption("DB Table", "dbtable")
                      .addOption("SQL Query", "sqlQuery")
                      .bindProperty("readFromSource")
                  )
                  .addElement(
                    Condition()
                      .ifEqual(PropExpr("component.properties.readFromSource"), StringExpr("dbtable"))
                      .then(TextBox("Table").bindPlaceholder("tablename").bindProperty("dbtable"))
                      .otherwise(
                        TextBox("SQL Query").bindPlaceholder("select c1, c2 from t1").bindProperty("query")
                      )
                  )
              )
          )
      )
      .addSection(
        "PROPERTIES",
        ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
          .addColumn(
            ScrollBox().addElement(
              StackLayout(height = Some("100%"))
                .addElement(
                  Condition()
                    .ifEqual(PropExpr("component.properties.readFromSource"), StringExpr("dbtable"))
                    .then(
                      dbTableFieldPicker
                    )
                    .otherwise(
                      sqlQueryFieldPicker
                    )
                )
            ),
            "auto"
          )
          .addColumn(SchemaTable("").isReadOnly().bindProperty("schema"), "5fr")
      )
      .addSection(
        "PREVIEW",
        PreviewTable("").bindProperty("schema")
      )
  }

  def targetDialog: DatasetDialog = DatasetDialog("jdbc")
    .addSection(
      "LOCATION",
      ColumnsLayout()
        .addColumn(
          StackLayout(direction = Some("vertical"), gap = Some("1rem"))
            //            .addElement(TitleElement(title = "Credentials"))
            .addElement(
              StackLayout()
                .addElement(
                  RadioGroup("Credentials")
                    .addOption("Databricks Secrets", "databricksSecrets")
                    .addOption("Username & Password", "userPwd")
                    .addOption("Environment variables", "userPwdEnv")
                    .bindProperty("credType")
                )
                .addElement(
                  Condition()
                    .ifEqual(PropExpr("component.properties.credType"), StringExpr("databricksSecrets"))
                    .then(Credentials("").bindProperty("credentialScope"))
                    .otherwise(
                      StackLayout()
                        .addElement(
                          ColumnsLayout(gap = Some("1rem"))
                            .addColumn(TextBox("Username").bindPlaceholder("username").bindProperty("textUsername"))
                            .addColumn(
                              TextBox("Password")
                                .isPassword()
                                .bindPlaceholder("password")
                                .bindProperty("textPassword")
                            )
                        )
                        .addElement(
                          ColumnsLayout()
                            .addColumn(
                              AlertBox(
                                children = List(
                                  Markdown("Storing plain-text passwords poses a security risk and is not recommended. Please see [here](https://docs.prophecy.io/low-code-spark/best-practices/use-dbx-secrets) for suggested alternatives")
                                )
                              )
                            )
                        )
                    )
                )
            )
            .addElement(TitleElement(title = "URL"))
            .addElement(
              TextBox("JDBC URL")
                .bindPlaceholder("jdbc:<sqlserver>://<jdbcHostname>:<jdbcPort>/<jdbcDatabase>")
                .bindProperty("jdbcUrl")
            )
            .addElement(TitleElement(title = "Table"))
            .addElement(TextBox("Table").bindPlaceholder("tablename").bindProperty("dbtable"))
        )
    )
    .addSection(
      "PROPERTIES",
      ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
        .addColumn(
          ScrollBox().addElement(
            StackLayout(height = Some("100%"))
              .addElement(
                StackItem(grow = Some(1)).addElement(
                  FieldPicker(height = Some("100%"))
                    .addField(
                      TextArea("Description", 2, placeholder = "Dataset description..."),
                      "description",
                      true
                    )
                    .addField(
                      TextBox("Driver").bindPlaceholder("org.postgresql.Driver"),
                      "driver",
                      true
                    )
                    .addField(
                      SelectBox("Write Mode")
                        .addOption("error", "error")
                        .addOption("overwrite", "overwrite")
                        .addOption("append", "append")
                        .addOption("ignore", "ignore"),
                      "writeMode"
                    )
                    .addField(TextBox("Number of Partitions").bindPlaceholder(""), "numPartitions")
                    .addField(TextBox("Query Timeout").bindPlaceholder(""), "queryTimeout")
                    .addField(TextBox("Batch Size").bindPlaceholder(""), "batchsize")
                    .addField(
                      SelectBox("Isolation Level")
                        .addOption("READ_UNCOMMITTED", "READ_UNCOMMITTED")
                        .addOption("NONE", "NONE")
                        .addOption("READ_COMMITTED", "READ_COMMITTED")
                        .addOption("REPEATABLE_READ", "REPEATABLE_READ")
                        .addOption("SERIALIZABLE", "SERIALIZABLE"),
                      "isolationLevel"
                    )
                    .addField(Checkbox("Truncate"), "truncate")
                    // todo this one's default is dependent on jdbc dialect.
                    .addField(Checkbox("Cascade Truncate"), "cascadeTruncate")
                    // todo find example and bind that as placeholder
                    .addField(TextBox("Create Table Options"), "createTableOptions")
                    .addField(
                      TextBox("Create Table Column Types")
                        .bindPlaceholder("name CHAR(64), comments VARCHAR(1024)"),
                      "createTableColumnTypes"
                    )
                    .addField(
                      TextArea(
                        "Execute Pre Query",
                        3,
                        placeholder = "update logs set action='loading_start' where job_id=101"
                      ),
                      "preSQL"
                    )
                    .addField(
                      TextArea(
                        "Execute Post Query",
                        3,
                        placeholder = "update logs set action='loading_end' where job_id=101 "
                      ),
                      "postSQL"
                    )
                  // skipping kerberos things for now
                )
              )
          ),
          "auto"
        )
        .addColumn(SchemaTable("").isReadOnly().withoutInferSchema().bindProperty("schema"), "5fr")
    )

  override def validate(component: Component)(implicit context: WorkflowContext): List[Diagnostic] = {
    import scala.collection.mutable.ListBuffer
    val diagnostics = ListBuffer[Diagnostic]()
    diagnostics ++= super.validate(component)

    component.properties.credType match {
      case "databricksSecrets" ⇒
        if (component.properties.credentialScope.getOrElse("").trim.isEmpty) {
          diagnostics += Diagnostic(
            "properties.credentialScope",
            "Credential Scope cannot be empty [Location]",
            SeverityLevel.Error
          )
        }
      case "userPwd" | "userPwdEnv" ⇒
        if (
          component.properties.credType == "userPwd" && !component.properties.textPassword
            .getOrElse("")
            .trim
            .startsWith("${")
        ) {
          diagnostics += Diagnostic(
            "properties.textPassword",
            "Storing plain-text passwords poses a security risk and is not recommended.",
            SeverityLevel.Warning
          )
        }
        if (component.properties.textUsername.getOrElse("").trim.isEmpty) {
          diagnostics += Diagnostic(
            "properties.textUsername",
            "Username cannot be empty [Location]",
            SeverityLevel.Error
          )
        } else if (component.properties.textPassword.getOrElse("").trim.isEmpty) {
          diagnostics += Diagnostic(
            "properties.textPassword",
            "Password cannot be empty [Location]",
            SeverityLevel.Error
          )
        }
    }

    if (component.properties.jdbcUrl.trim.isEmpty) {
      diagnostics += Diagnostic("properties.jdbcUrl", "JDBC URL cannot be empty [Location]", SeverityLevel.Error)
    }

    val PC = {
      val pc = component.properties.partitionColumn
      if (pc.isEmpty || pc.get.isEmpty) None else pc
    }
    val LB = {
      val lb = component.properties.lowerBound
      if (lb.isEmpty || lb.get.isEmpty) None else lb
    }
    val UB = {
      val ub = component.properties.upperBound
      if (ub.isEmpty || ub.get.isEmpty) None else ub
    }
    (PC, LB, UB) match {
      case (None, None, None) ⇒
        // none of the three is specified. This is okay
        // if these are not specified, run independent validation on numPartitions.
        component.properties.numPartitions match {
          case None | Some("") ⇒
          case Some(value) ⇒
            if (Try(value.trim.toInt).isFailure) {
              diagnostics += Diagnostic(
                "properties.numPartitions",
                "Number of partitions has to be a Number [Properties]",
                SeverityLevel.Error
              )
            }
          // this is okay. if other three are not specified
        }
      case (maybePartCol, maybeLowerBound, maybeUpperBound) ⇒
        // full or partial specification.
        // if any one of these is specified the all 4 have to be specified
        // now, if any of them is missing, we gotta slap this error
        val diagMsg =
          "Partition Columns, Lower Bound, Upper Bound: These options must all be specified if any of them is specified. " +
            "In addition, Number of Partitions must be specified. [Properties]"
        component.properties.numPartitions match {
          case None | Some("") ⇒
            diagnostics += Diagnostic("properties.numPartitions", diagMsg, SeverityLevel.Error)
          case Some(numPartition) ⇒
            if (Try(numPartition.trim.toInt).isFailure) {
              diagnostics += Diagnostic(
                "properties.numPartitions",
                "Number of partitions has to be a Number [Properties]",
                SeverityLevel.Error
              )
            }
        }
        if (maybePartCol.isEmpty || maybePartCol.get.trim.isEmpty) {
          diagnostics += Diagnostic("properties.partitionColumn", diagMsg, SeverityLevel.Error)
        }
        maybeLowerBound match {
          case None | Some("") ⇒ diagnostics += Diagnostic("properties.lowerBound", diagMsg, SeverityLevel.Error)
          case Some(lowerBound) ⇒
          //            if (Try(lowerBound.trim.toLong).isFailure) {
          //              diagnostics += Diagnostic(
          //                "properties.lowerBound",
          //                "Lower Bound has to be a Long [Properties]",
          //                SeverityLevel.Error
          //              )
          //            }
        }
        maybeUpperBound match {
          case None | Some("") ⇒ diagnostics += Diagnostic("properties.upperBound", diagMsg, SeverityLevel.Error)
          case Some(upperBound) ⇒
          //            if (Try(upperBound.trim.toLong).isFailure) {
          //              diagnostics += Diagnostic(
          //                "properties.upperBound",
          //                "Upper Bound has to be a Long [Properties]",
          //                SeverityLevel.Error
          //              )
          //            }
        }

    }

    if (component.properties.driver.trim.isEmpty) {
      diagnostics += Diagnostic("properties.driver", "JDBC Driver cannot be empty [Properties]", SeverityLevel.Error)
    }

    if (component.properties.schema.isEmpty) {
      //      diagnostics += Diagnostic("properties.schema", "Schema cannot be empty [Properties]", SeverityLevel.Error)
    }

    diagnostics.toList
  }

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = newState

  override def deserializeProperty(props: String): PropertiesType = Json.parse(props).as[PropertiesType]

  override def serializeProperty(props: PropertiesType): String = Json.stringify(Json.toJson(props))
  class JDBCFormatCode(props: JDBCProperties) extends ComponentCode {

    def sourceApply(spark: SparkSession): DataFrame = {
      val jdbc_username: String = if (props.credType == "databricksSecrets") {
        import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
        dbutils.secrets.get(scope = props.credentialScope.get, key = "username")
      } else if (props.credType == "userPwd") {
        s"${props.textUsername.get}"
      } else if (props.credType == "userPwdEnv") {
        sys.env(s"${props.textUsername.get}")
      } else {
        throw new RuntimeException(s"Invalid credType: ${props.credType}")
      }

      val jdbc_password: String = if (props.credType == "databricksSecrets") {
        import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
        dbutils.secrets.get(scope = props.credentialScope.get, key = "password")
      } else if (props.credType == "userPwd") {
        s"${props.textPassword.get}"
      } else if (props.credType == "userPwdEnv") {
        sys.env(s"${props.textPassword.get}")
      } else {
        throw new RuntimeException(s"Invalid credType: ${props.credType}")
      }

      props.preSQL.foreach { sql =>
        import java.sql.{Connection, DriverManager}
        var connection: Connection = null
        try {
          connection = DriverManager.getConnection(
            props.jdbcUrl,
            jdbc_username,
            jdbc_password
          )
          val statement = connection.prepareStatement(sql)
          try statement.executeUpdate()
          finally statement.close()
        } finally if (connection != null) connection.close()
      }

      var reader = spark.read.format("jdbc")
      reader = reader
        .option("url", props.jdbcUrl)
        .option("user", jdbc_username)
        .option("password", jdbc_password)
        .option("partitionColumn", props.partitionColumn)
        .option("lowerBound", props.lowerBound)
        .option("upperBound", props.upperBound)
        .option("numPartitions", props.numPartitions)
        .option("queryTimeout", props.queryTimeout)
        .option("fetchsize", props.fetchsize)
        .option("sessionInitStatement", props.sessionInitStatement)
        //      .option("customSchema", props.customSchema)
        .option("pushDownPredicate", props.pushDownPredicate)
        .option("pushDownAggregate", props.pushDownAggregate)
        .option("driver", props.driver)

      reader = props.readFromSource match {
        case "dbtable" ⇒ reader.option("dbtable", props.dbtable.get)
        case "sqlQuery" ⇒ reader.option("query", props.query.get)
      }

      var df = reader.load()
      props.postSQL.foreach { sql =>
        import java.sql.{Connection, DriverManager}
        df = df.cache()
        var connection: Connection = null
        try {
          connection = DriverManager.getConnection(
            props.jdbcUrl,
            jdbc_username,
            jdbc_password
          )
          val statement = connection.prepareStatement(sql)
          try statement.executeUpdate()
          finally statement.close()
        } finally if (connection != null) connection.close()
      }
      df
    }

    def targetApply(spark: SparkSession, in: DataFrame): Unit = {
      val jdbc_username: String = if (props.credType == "databricksSecrets") {
        import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
        dbutils.secrets.get(scope = props.credentialScope.get, key = "username")
      } else if (props.credType == "userPwd") {
        s"${props.textUsername.get}"
      } else if (props.credType == "userPwdEnv") {
        sys.env(s"${props.textUsername.get}")
      } else {
        throw new RuntimeException(s"Invalid credType: ${props.credType}")
      }

      val jdbc_password: String = if (props.credType == "databricksSecrets") {
        import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
        dbutils.secrets.get(scope = props.credentialScope.get, key = "password")
      } else if (props.credType == "userPwd") {
        s"${props.textPassword.get}"
      } else if (props.credType == "userPwdEnv") {
        sys.env(s"${props.textPassword.get}")
      } else {
        throw new RuntimeException(s"Invalid credType: ${props.credType}")
      }

      props.preSQL.foreach { sql =>
        import java.sql.{Connection, DriverManager}

        var connection: Connection = null
        try {
          connection = DriverManager.getConnection(
            props.jdbcUrl,
            jdbc_username,
            jdbc_password
          )
          val statement = connection.prepareStatement(sql)
          try statement.executeUpdate()
          finally statement.close()
        } finally if (connection != null) connection.close()
      }

      var writer = in.write.format("jdbc")

      writer = writer
        .option("url", props.jdbcUrl)
        .option("dbtable", props.dbtable.get)
        .option("user", jdbc_username)
        .option("password", jdbc_password)
        .option("numPartitions", props.numPartitions)
        .option("queryTimeout", props.queryTimeout)
        .option("batchsize", props.batchsize)
        .option("isolationLevel", props.isolationLevel)
        .option("truncate", props.truncate)
        .option("cascadeTruncate", props.cascadeTruncate)
        .option("createTableOptions", props.createTableOptions)
        .option("createTableColumnTypes", props.createTableColumnTypes)
        .option("driver", props.driver)

      props.writeMode.foreach(mode ⇒ writer = writer.mode(mode))

      writer.save()
      props.postSQL.foreach { sql =>
        import java.sql.{Connection, DriverManager}
        var connection: Connection = null
        try {
          connection = DriverManager.getConnection(
            props.jdbcUrl,
            jdbc_username,
            jdbc_password
          )
          val statement = connection.prepareStatement(sql)
          try statement.executeUpdate()
          finally statement.close()
        } finally if (connection != null) connection.close()
      }
    }
  }
}
