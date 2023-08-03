package io.prophecy.scalagems.dataset

import io.prophecy.gems._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.datasetSpec._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.uiSpec._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.{Format, Json}

class KafkaStream extends DatasetSpec {

  val name: String = "KafkaStream"
  val datasetType: String = "File"
  val docUrl: String = "https://docs.prophecy.io/low-code-spark/gems/source-target/file/kafka"

  type PropertiesType = KafkaStreamFormatProperties
  case class KafkaStreamFormatProperties(
    @Property("Schema")
    schema: Option[StructType] = None,
    @Property("Description")
    description: Option[String] = Some(""),
    @Property("Credential Type")
    credType: String = "userPwd",
    @Property("Credential Scope")
    credentialScope: Option[String] = None,
    @Property("Username")
    textUsername: Option[String] = None,
    @Property("Password")
    textPassword: Option[String] = None,
    @Property("brokerList", "")
    brokerList: String = "",
    @Property("groupId", "")
    groupId: String = "",
    @Property("sessionTimeout", "")
    sessionTimeout: String = "6000",
    @Property("security", "")
    security: Option[String] = Some("SASL_SSL"),
    @Property("saslMechanism", "")
    saslMechanism: Option[String] = Some("SCRAM-SHA-256"),
    @Property("kafkaTopic", "")
    kafkaTopic: String = "",
    @Property("offsetMetaTable", "")
    offsetMetaTable: String = "",
    @Property("messageKey", "")
    messageKey: Option[SColumn] = None
  ) extends DatasetProperties

  implicit val kafkaStreamFormatPropertiesFormat: Format[KafkaStreamFormatProperties] = Json.format
  def sourceDialog: DatasetDialog = {
    DatasetDialog("KafkaStream")
      .addSection(
        "LOCATION",
        StackLayout()
          .addElement(
            TextBox("Broker List")
              .bindPlaceholder("broker1.aws.com:9094,broker2.aws.com:9094")
              .bindProperty("brokerList")
          )
          .addElement(TextBox("Group Id").bindPlaceholder("group_id_1").bindProperty("groupId"))
          .addElement(
            TextBox("Session timeout (in ms)")
              .bindPlaceholder("6000")
              .bindProperty("sessionTimeout")
          )
          .addElement(
            SelectBox("Security Protocol")
              .addOption("SASL_SSL", "SASL_SSL")
              .bindProperty("security")
          )
          .addElement(
            StackLayout()
              .addElement(
                SelectBox("SASL Mechanisms")
                  .addOption("SCRAM-SHA-256", "SCRAM-SHA-256")
                  .bindProperty("saslMechanism")
              )
              .addElement(
                StackLayout()
                  .addElement(
                    RadioGroup("Credentials")
                      .addOption("Databricks Secrets", "databricksSecrets")
                      .addOption("Username & Password", "userPwd")
                      .bindProperty("credType")
                  )
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
          .addElement(
            ColumnsLayout(gap = Some("1rem"))
              .addColumn(
                TextBox("Kafka topic")
                  .bindPlaceholder("my_first_topic,my_second_topic")
                  .bindProperty("kafkaTopic")
              )
              .addColumn(
                TextBox("Metadata Table")
                  .bindPlaceholder("db.metaTable")
                  .bindProperty("offsetMetaTable")
              )
          )
      )
      .addSection(
        "PROPERTIES",
        ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
          .addColumn(
            StackLayout(height = Some("100%"))
              .addElement(
                StackItem(grow = Some(1))
                  .addElement(
                    FieldPicker(height = Some("100%"))
                      .addField(
                        TextArea("Description", 2, placeholder = "Dataset description..."),
                        "description",
                        true
                      )
                  )
              ),
            "auto"
          )
          .addColumn(
            SchemaTable("").isReadOnly().bindProperty("schema"),
            "5fr"
          )
      )
      .addSection("PREVIEW", PreviewTable("").bindProperty("schema"))
  }

  def targetDialog: DatasetDialog = {
    import scala.collection.mutable
    DatasetDialog("KafkaStream").addSection(
      "PROPERTIES",
      StackLayout()
        .addElement(
          TextArea("Description", 2, placeholder = "Dataset description...").bindProperty("description")
        )
        .addElement(
          TextBox("Broker List")
            .bindPlaceholder("broker1.aws.com:9094,broker2.aws.com:9094")
            .bindProperty("brokerList")
        )
        .addElement(
          SelectBox("Security Protocol")
            .addOption("SASL_SSL", "SASL_SSL")
            .bindProperty("security")
        )
        .addElement(
          StackLayout()
            .addElement(
              SelectBox("SASL Mechanisms")
                .addOption("SCRAM-SHA-256", "SCRAM-SHA-256")
                .bindProperty("saslMechanism")
            )
            .addElement(
              StackLayout()
                .addElement(
                  RadioGroup("Credentials")
                    .addOption("Databricks Secrets", "databricksSecrets")
                    .addOption("Username & Password", "userPwd")
                    .bindProperty("credType")
                )
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
        .addElement(
          ColumnsLayout(gap = Some("1rem"))
            .addColumn(
              TextBox("Kafka topic")
                .bindPlaceholder("my_first_topic,my_second_topic")
                .bindProperty("kafkaTopic")
            )
        )
        .addElement(
          StackLayout()
            .addElement(
              NativeText(
                "Message Unique Key (Optional)"
              )
            )
            .addElement(
              ExpressionBox(ignoreTitle = true)
                .makeFieldOptional()
                .withSchemaSuggestions()
                .bindPlaceholders(
                  mutable
                    .Map(
                      "scala" → """concat(col("col1"), col("col2"))""",
                      "python" → """concat(col("col1"), col("col2"))""",
                      "sql" → """concat(col1, col2)"""
                    )
                )
                .bindProperty("messageKey")
            )
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
            "Credential Scope cannot be empty [Location]",
            SeverityLevel.Error
          )
        }
      case "userPwd" ⇒
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

    if (component.properties.security.get.trim.isEmpty) {
      diagnostics += Diagnostic("properties.security", "Security cannot be empty [Location]", SeverityLevel.Error)
    }

    if (component.properties.kafkaTopic.trim.isEmpty) {
      diagnostics += Diagnostic("properties.kafkaTopic", "Kafka Topic cannot be empty [Location]", SeverityLevel.Error)
    }

    if (component.properties.brokerList.trim.isEmpty) {
      diagnostics += Diagnostic("properties.brokerList", "Broker List cannot be empty [Location]", SeverityLevel.Error)
    }

    diagnostics.toList
  }

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = newState

  override def deserializeProperty(props: String): PropertiesType = Json.parse(props).as[PropertiesType]

  override def serializeProperty(props: PropertiesType): String = Json.stringify(Json.toJson(props))

  class KafkaStreamFormatCode(props: KafkaStreamFormatProperties) extends ComponentCode {

    def sourceApply(spark: SparkSession): DataFrame = {
      import scala.util.Try
      if (Try(Class.forName("io.prophecy.core.code.SimpleScalaEvaluator")).isFailure) {
        import _root_.io.delta.tables._
        import play.api.libs.json.Json

        import scala.collection.mutable.Map
        var username = ""
        var password = ""
        props.credType match {
          case "databricksSecrets" ⇒
            import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
            username = dbutils.secrets.get(scope = props.credentialScope.get, key = "username")
            password = dbutils.secrets.get(scope = props.credentialScope.get, key = "password")
          case "userPwd" ⇒
            username = s"${props.textUsername.get}"
            password = s"${props.textPassword.get}"
        }

        val jaas_config =
          s"kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username='${username}' password='${password}';"

        var reader = spark.read
          .format("kafka")
          .option("kafka.sasl.jaas.config", jaas_config)
          .option("kafka.sasl.mechanism", props.saslMechanism.get)
          .option("kafka.security.protocol", props.security.get)
          .option("kafka.bootstrap.servers", props.brokerList)
          .option("kafka.session.timeout.ms", props.sessionTimeout)
          .option("group.id", props.groupId)
          .option("subscribe", props.kafkaTopic)

        if (spark.catalog.tableExists(props.offsetMetaTable)) {
          val deltaTable = DeltaTable
            .forName(
              spark,
              s"${props.offsetMetaTable}"
            )
            .toDF

//          val rows = deltaTable.collect().toList
          var offset_dict: Map[String, Map[String, Int]] = Map()
          for (row <- deltaTable.collect().toList) {
            if (offset_dict.contains(row.getString(1))) {
              offset_dict(row.getString(1)) =
                offset_dict(row.getString(1)) + (row.getInt(0).toString -> (row.getInt(2) + 1))
            } else {
              offset_dict = offset_dict + (row.getString(1) -> Map(row.getInt(0).toString -> (row.getInt(2) + 1)))
            }
          }
          reader = reader.option("startingOffsets", Json.toJson(offset_dict).toString())
        }

        reader
          .load()
          .withColumn("value", col("value").cast("string"))
          .withColumn("key", col("key").cast("string"))
      } else {
        spark.createDataFrame(Seq(("1", "1"), ("2", "2")))
      }
    }

    def targetApply(spark: SparkSession, in: DataFrame): Unit = {
      var username = ""
      var password = ""
      props.credType match {
        case "databricksSecrets" ⇒
          import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
          username = dbutils.secrets.get(scope = props.credentialScope.get, key = "username")
          password = dbutils.secrets.get(scope = props.credentialScope.get, key = "password")
        case "userPwd" ⇒
          username = s"${props.textUsername.get}"
          password = s"${props.textPassword.get}"
      }

      val jaas_config =
        s"kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username='${username}' password='${password}';"

      val writer = if (props.messageKey.nonEmpty) {
        in.select(props.messageKey.get.column.alias("key"), to_json(struct("*")).alias("value"))
          .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      } else {
        in.select(to_json(struct("*")).alias("value"))
          .selectExpr("CAST(value AS STRING)")
      }

      writer.write
        .format("kafka")
        .option("kafka.sasl.jaas.config", jaas_config)
        .option("kafka.sasl.mechanism", props.saslMechanism.get)
        .option("kafka.security.protocol", props.security.get)
        .option("kafka.bootstrap.servers", props.brokerList)
        .option("topic", props.kafkaTopic)
        .save()
    }

  }

}
