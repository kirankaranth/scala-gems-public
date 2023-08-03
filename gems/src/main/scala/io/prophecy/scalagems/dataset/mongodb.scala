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
import play.api.libs.json.{Format, Json}

class mongodb extends DatasetSpec {

  type PropertiesType = MongoDBProperties
  val name: String = "mongodb"
  val datasetType: String = "Warehouse"
  val docUrl: String = "https://docs.prophecy.io/low-code-spark/gems/source-target/warehouse/mongodb"

  def sourceDialog: DatasetDialog = {
    val urlSection = StackLayout()
      .addElement(TitleElement(title = "Build Connection URI"))
      .addElement(
        CodeBlock(title = "Connection URI")
          .bindProperty("connectionUri")
      )
      .addElement(
        ColumnsLayout(gap = Some("1rem"))
          .addColumn(TextBox("Driver").bindPlaceholder("mongodb+srv").bindProperty("driver"))
          .addColumn(
            TextBox("Cluter IP Address and Options")
              .bindPlaceholder("cluster0.prophecy.mongodb.net/?retryWrites=true&w=majority")
              .bindProperty("clusterIpAddress")
          )
      )
      .addElement(TitleElement(title = "Source"))
      .addElement(
        ColumnsLayout(gap = Some("1rem"))
          .addColumn(TextBox("Database").bindPlaceholder("database").bindProperty("database"))
          .addColumn(TextBox("Collection").bindPlaceholder("collection").bindProperty("collection"))
      )

    DatasetDialog("mongodb")
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
                      .bindProperty("credType")
                  )
                  .addElement(
                    Condition()
                      .ifEqual(PropExpr("component.properties.credType"), StringExpr("databricksSecrets"))
                      .then(Credentials("").bindProperty("credentialScope"))
                      .otherwise(
                        ColumnsLayout(gap = Some("1rem"))
                          .addColumn(TextBox("Username").bindPlaceholder("username").bindProperty("textUsername"))
                          .addColumn(
                            TextBox("Password").isPassword().bindPlaceholder("password").bindProperty("textPassword")
                          )
                      )
                  )
              )
              .addElement(urlSection)
          )
      )
      .addSection(
        "PROPERTIES",
        ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
          .addColumn(
            ScrollBox()
              .addElement(
                StackLayout()
                  .addElement(
                    StackItem(grow = Some(1)).addElement(
                      FieldPicker(height = Some("100%"))
                        .addField(
                          TextArea("Description", 2, placeholder = "Dataset description..."),
                          "description",
                          true
                        )
                        .addField(
                          TextBox(
                            "Mongo client factory",
                            placeholder = "com.mongodb.spark.sql.connector.connection.DefaultMongoClientFactory"
                          ),
                          "mongoClientFactory"
                        )
                        .addField(
                          TextBox(
                            "Partitioner class name",
                            placeholder = "com.mongodb.spark.sql.connector.read.partitioner.SamplePartitioner"
                          ),
                          "partitioner"
                        )
                        .addField(
                          TextBox("Partition field", placeholder = "_id"),
                          "partitionerOptionsPartitionField"
                        )
                        .addField(
                          TextBox("Partition size", placeholder = "64"),
                          "partitionerOptionsPartitionSize"
                        )
                        .addField(
                          TextBox("Number of samples per partition", placeholder = "10"),
                          "partitionerOptionsSamplesPerPartition"
                        )
                        .addField(
                          TextBox("Minimum no. of Docs for Schema inference", placeholder = "1000"),
                          "sampleSize"
                        )
                        .addField(
                          SelectBox("Enable Map types when inferring schema")
                            .addOption("true", "true")
                            .addOption("false", "false"),
                          "sqlInferSchemaMapTypesEnabled"
                        )
                        .addField(
                          TextBox("Minimum no. of a StructType for MapType inference", placeholder = "250"),
                          "sqlInferSchemaMapTypesMinimumKeySize"
                        )
                        .addField(
                          TextBox("Pipeline aggregation", placeholder = """{"$match": {"closed": false}}"""),
                          "aggregationPipeline"
                        )
                        .addField(
                          SelectBox("Enable AllowDiskUse aggregation")
                            .addOption("true", "true")
                            .addOption("false", "false"),
                          "aggregationAllowDiskUse"
                        )
                    )
                  )
              ),
            "auto"
          )
          .addColumn(
            SchemaTable("").bindProperty("schema")
          )
      )
      .addSection(
        "PREVIEW",
        PreviewTable("").bindProperty("schema")
      )
  }

  def targetDialog: DatasetDialog = {
    val urlSection = StackLayout()
      .addElement(TitleElement(title = "Build Connection URI"))
      .addElement(
        CodeBlock(title = "Connection URI")
          .bindProperty("connectionUri")
      )
      .addElement(
        ColumnsLayout(gap = Some("1rem"))
          .addColumn(TextBox("Driver").bindPlaceholder("mongodb+srv").bindProperty("driver"))
          .addColumn(
            TextBox("Cluter IP Address and Options")
              .bindPlaceholder("cluster0.prophecy.mongodb.net/?retryWrites=true&w=majority")
              .bindProperty("clusterIpAddress")
          )
      )
      .addElement(TitleElement(title = "Target"))
      .addElement(
        ColumnsLayout(gap = Some("1rem"))
          .addColumn(TextBox("Database").bindPlaceholder("database").bindProperty("database"))
          .addColumn(TextBox("Collection").bindPlaceholder("collection").bindProperty("collection"))
      )

    DatasetDialog("mongodb")
      .addSection(
        "LOCATION",
        ColumnsLayout()
          .addColumn(
            StackLayout(direction = Some("vertical"), gap = Some("1rem"))
              .addElement(
                StackLayout()
                  .addElement(
                    RadioGroup("Credentials")
                      .addOption("Databricks Secrets", "databricksSecrets")
                      .addOption("Username & Password", "userPwd")
                      .bindProperty("credType")
                  )
                  .addElement(
                    Condition()
                      .ifEqual(PropExpr("component.properties.credType"), StringExpr("databricksSecrets"))
                      .then(Credentials("").bindProperty("credentialScope"))
                      .otherwise(
                        ColumnsLayout(gap = Some("1rem"))
                          .addColumn(TextBox("Username").bindPlaceholder("username").bindProperty("textUsername"))
                          .addColumn(
                            TextBox("Password").isPassword().bindPlaceholder("password").bindProperty("textPassword")
                          )
                      )
                  )
              )
              .addElement(urlSection)
          )
      )
      .addSection(
        "PROPERTIES",
        ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
          .addColumn(
            ScrollBox().addElement(
              StackLayout(height = Some("100%")).addElement(
                StackItem(grow = Some(1)).addElement(
                  FieldPicker(height = Some("100%"))
                    .addField(
                      TextArea("Description", 2, placeholder = "Dataset description..."),
                      "description",
                      true
                    )
                    .addField(
                      SelectBox("Write Mode")
                        .addOption("overwrite", "overwrite")
                        .addOption("append", "append"),
                      "writeMode"
                    )
                    .addField(
                      TextBox(
                        "Mongo client factory",
                        placeholder = "com.mongodb.spark.sql.connector.connection.DefaultMongoClientFactory"
                      ),
                      "mongoClientFactory"
                    )
                    .addField(TextBox("Maximum batch size", placeholder = "512"), "maxBatchSize")
                    .addField(
                      SelectBox("Ordered")
                        .addOption("true", "true")
                        .addOption("false", "false"),
                      "ordered"
                    )
                    .addField(
                      SelectBox("Operation type")
                        .addOption("insert", "insert")
                        .addOption("replace", "replace")
                        .addOption("update", "update"),
                      "operationType"
                    )
                    .addField(
                      TextBox("List of id fields", placeholder = "_id"),
                      "idFieldList"
                    )
                    .addField(
                      SelectBox("Write concern")
                        .addOption("MAJORITY", "MAJORITY")
                        .addOption("W1", "W1")
                        .addOption("W2", "W2")
                        .addOption("W3", "W3")
                        .addOption("ACKNOWLEDGED", "ACKNOWLEDGED")
                        .addOption("UNACKNOWLEDGED", "UNACKNOWLEDGED"),
                      "writeConcernW"
                    )
                    .addField(
                      SelectBox("Enable Write journal")
                        .addOption("true", "true")
                        .addOption("false", "false"),
                      "writeConcernJournal"
                    )
                    .addField(
                      TextBox("Write timeout in Milliseconds", placeholder = "0"),
                      "writeConcernWTimeoutMs"
                    )
                )
              )
            ),
            "auto"
          )
          .addColumn(
            StackLayout()
              .addElement(
                StackLayout(height = Some("80bh"))
                  .addElement(SchemaTable("").isReadOnly().withoutInferSchema().bindProperty("schema"))
              ),
            "5fr"
          )
      )
  }

  override def validate(component: Component)(implicit context: WorkflowContext): List[Diagnostic] = {
    import scala.collection.mutable.ListBuffer
    val diagnostics = ListBuffer[Diagnostic]()
    diagnostics ++= super.validate(component)

    component.properties.credType match {
      case "databricksSecrets" ⇒
        if (component.properties.credentialScope.getOrElse("").trim.isEmpty) {
          diagnostics += Diagnostic(
            "properties.credentialScope",
            "Credential Scope cannot be Empty",
            SeverityLevel.Error
          )
        }
      case "userPwd" ⇒
        if (component.properties.textUsername.getOrElse("").trim.isEmpty) {
          diagnostics += Diagnostic(
            "properties.textUsername",
            "Username cannot be empty",
            SeverityLevel.Error
          )
        } else if (component.properties.textPassword.getOrElse("").trim.isEmpty) {
          diagnostics += Diagnostic(
            "properties.textPassword",
            "Password cannot be empty",
            SeverityLevel.Error
          )
        }
    }

    if (component.properties.driver.getOrElse("").trim.isEmpty)
      diagnostics += Diagnostic(
        "properties.driver",
        "Driver cannot be empty, example values [mongodb, mongodb+srv]",
        SeverityLevel.Error
      )

    if (component.properties.clusterIpAddress.getOrElse("").trim.isEmpty)
      diagnostics += Diagnostic(
        "properties.clusterIpAddress",
        "Cluster IP Address and Options cannot be empty, eg. 'cluster0.prophecy.mongodb.net/?retryWrites=true&w=majority'",
        SeverityLevel.Error
      )

    if (component.properties.database.getOrElse("").trim.isEmpty)
      diagnostics += Diagnostic("properties.database", "Database name cannot be empty", SeverityLevel.Error)

    if (component.properties.collection.getOrElse("").trim.isEmpty)
      diagnostics += Diagnostic("properties.collection", "Collection name cannot be empty", SeverityLevel.Error)

    diagnostics.toList
  }

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = newState

  case class MongoDBProperties(
    // DEFAULT
    @Property("Schema")
    schema: Option[StructType] = None,
    @Property("Description")
    description: Option[String] = Some(""),
    @Property("Credential Type")
    credType: String = "databricksSecrets",
    @Property("Credential Scope")
    credentialScope: Option[String] = None,
    @Property("Write Mode", """(default: "error") Specifies the behavior when data or table already exists.""")
    writeMode: Option[String] = Some("overwrite"),

    // Common Props
    @Property("Username")
    textUsername: Option[String] = None,
    @Property("Password")
    textPassword: Option[String] = None,
    @Property("database", """The database name configuration.""")
    database: Option[String] = None,
    @Property("collection", """The collection name configuration.""")
    collection: Option[String] = None,
    @Property("Connection URI", "Full connection URI generated using information by User")
    connectionUri: Option[String] = Some("<driver>://<username>:<password>@<cluster_ip_and_options>"),
    @Property(
      "mongoClientFactory",
      "You can specify a custom implementation which must implement the com.mongodb.spark.sql.connector.connection.MongoClientFactory interface."
    )
    mongoClientFactory: Option[String] = None,
    @Property("Driver", """Driver String to be used""")
    driver: Option[String] = None,
    @Property("Cluster IP address", """ip address to cluster, informat """)
    clusterIpAddress: Option[String] = None,

    // Read Props

    @Property("partitioner", """The partitioner full class name""")
    partitioner: Option[String] = None,
    @Property(
      "partitioner.options.partition.field",
      """The field to use for partitioning, which must be a unique field."""
    )
    partitionerOptionsPartitionField: Option[String] = None,
    @Property("partitioner.options.partition.size", """The size (in MB) for each partition.""")
    partitionerOptionsPartitionSize: Option[String] = None,
    @Property("partitioner.options.samples.per.partition", """The number of samples to take per partition.""")
    partitionerOptionsSamplesPerPartition: Option[String] = None,
    @Property("sampleSize", "The number of documents to sample from the collection when inferring the schema.")
    sampleSize: Option[String] = None,
    @Property("sql.inferSchema.mapTypes.enabled", "Whether to enable Map types when inferring the schema.")
    sqlInferSchemaMapTypesEnabled: Option[String] = None,
    @Property(
      "sql.inferSchema.mapTypes.minimum.key.size",
      "Minimum size of a StructType before inferring as a MapType."
    )
    sqlInferSchemaMapTypesMinimumKeySize: Option[String] = None,
    @Property(
      "aggregation.pipeline",
      "Specifies a custom aggregation pipeline to apply to the collection before sending data to Spark."
    )
    aggregationPipeline: Option[String] = None,
    @Property(
      "aggregation.allowDiskUse",
      "Specifies a custom aggregation pipeline to apply to the collection before sending data to Spark."
    )
    aggregationAllowDiskUse: Option[String] = None,

    // Write Props
    @Property("maxBatchSize", "Specifies the maximum number of operations to batch in bulk operations.")
    maxBatchSize: Option[String] = None,
    @Property("ordered", "Specifies whether to perform ordered bulk operations.")
    ordered: Option[String] = None,
    @Property("operationType", "Specifies the type of write operation to perform.")
    operationType: Option[String] = None,
    @Property("idFieldList", "Field or list of fields by which to split the collection data.")
    idFieldList: Option[String] = None,
    @Property(
      "writeConcern.w",
      "Specifies w, a write concern option to acknowledge the level to which the change propagated in the MongoDB replica set."
    )
    writeConcernW: Option[String] = None,
    @Property(
      "writeConcern.journal",
      "Specifies j, a write concern option to enable request for acknowledgment that the data is confirmed on on-disk journal for the criteria specified in the w option."
    )
    writeConcernJournal: Option[String] = None,
    @Property(
      "writeConcern.wTimeoutMS",
      "Specifies wTimeoutMS, a write concern option to return an error when a write operation exceeds the number of millisecond."
    )
    writeConcernWTimeoutMs: Option[String] = None
  ) extends DatasetProperties
  
  implicit val mongoDBPropertiesFormat: Format[MongoDBProperties] = Jsonx.formatCaseClass[MongoDBProperties]
  class MongoDBFormatCode(props: MongoDBProperties) extends ComponentCode {

    def sourceApply(spark: SparkSession): DataFrame = {
      import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

      val conn_uri = props.credType match {
        case "databricksSecrets" ⇒
          f"""${props.driver.get}://${dbutils.secrets.get(
            scope = props.credentialScope.get,
            key = "username"
          )}:${dbutils.secrets.get(
            scope = props.credentialScope.get,
            key = "password"
          )}@${props.clusterIpAddress.get}"""
        case "userPwd" ⇒
          f"""${props.driver.get}://${props.textUsername.get}:${props.textPassword.get}@${props.clusterIpAddress.get}"""
      }
      val reader = spark.read
        .format("mongodb")
        .option("connection.uri", conn_uri.trim)
        .option("database", props.database)
        .option("collection", props.collection)
        // Optionals
        .option("mongoClientFactory", props.mongoClientFactory)
        .option("partitioner", props.partitioner)
        .option("partitioner.options.partition.field", props.partitionerOptionsPartitionField)
        .option("partitioner.options.partition.size", props.partitionerOptionsPartitionSize)
        .option("partitioner.options.samples.per.partition", props.partitionerOptionsSamplesPerPartition)
        .option("sampleSize", props.sampleSize)
        .option("sql.inferSchema.mapTypes.enabled", props.sqlInferSchemaMapTypesEnabled)
        .option("sql.inferSchema.mapTypes.minimum.key.size", props.sqlInferSchemaMapTypesMinimumKeySize)
        .option("aggregation.pipeline", props.aggregationPipeline)
        .option("aggregation.allowDiskUse", props.aggregationAllowDiskUse)

      reader.load()

    }

    def targetApply(spark: SparkSession, df: DataFrame): Unit = {
      import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

      val conn_uri = props.credType match {
        case "databricksSecrets" ⇒
          f"""${props.driver.get}://${dbutils.secrets.get(
            scope = props.credentialScope.get,
            key = "username"
          )}:${dbutils.secrets.get(
            scope = props.credentialScope.get,
            key = "password"
          )}@${props.clusterIpAddress.get}"""
        case "userPwd" ⇒
          f"""${props.driver.get}://${props.textUsername.get}:${props.textPassword.get}@${props.clusterIpAddress.get}"""
      }

      val writer = df.write
        .format("mongodb")
        .mode(props.writeMode.get)
        .option("connection.uri", conn_uri.trim)
        .option("database", props.database)
        .option("collection", props.collection)
        // Optionals
        .option("mongoClientFactory", props.mongoClientFactory)
        .option("maxBatchSize", props.maxBatchSize)
        .option("ordered", props.ordered)
        .option("operationType", props.operationType)
        .option("idFieldList", props.idFieldList)
        .option("writeConcern.w", props.writeConcernW)
        .option("writeConcern.journal", props.writeConcernJournal)
        .option("writeConcern.wTimeoutMS", props.writeConcernWTimeoutMs)

      writer.save()
    }

  }

  override def deserializeProperty(props: String): PropertiesType = Json.parse(props).as[PropertiesType]

  override def serializeProperty(props: PropertiesType): String = Json.stringify(Json.toJson(props))
}
