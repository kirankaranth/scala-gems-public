package io.prophecy.scalagems.dataset

import io.prophecy.gems._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.datasetSpec._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.uiSpec._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.{Format, Json}

class redshift extends DatasetSpec {

  val name: String = "redshift"
  val datasetType: String = "Warehouse"
  val docUrl: String = "https://docs.prophecy.io/low-code-spark/gems/source-target/warehouse/redshift"

  type PropertiesType = RedshiftProperties
  case class RedshiftProperties(
    @Property("Schema")
    schema: Option[StructType] = None,
    @Property("Description")
    description: Option[String] = Some(""),
    @Property("Database")
    path: String = "",
    @Property("Table")
    tableName: String = "",
    @Property("Credential Scope")
    credentialScope: String = "",
    @Property("jdbcDriver", "")
    jdbcDriver: String = "jdbc:redshift",
    @Property("jdbcUrl", "")
    jdbcUrl: String = "",
    @Property("jdbcHost", "")
    jdbcHost: String = "",
    @Property("jdbcPort", "")
    jdbcPort: String = "",
    @Property("jdbcTempDir", "")
    jdbcTempDir: Option[String] = None,
    @Property(
      "forward_spark_s3_credentials",
      "Specifies the behavior when data or table already exists."
    )
    forward_spark_s3_credentials: Option[Boolean] = Some(true),
    @Property(
      "aws_iam_role",
      "Fully specified ARN of the IAM Role attached to the Redshift cluster, ex: arn:aws:iam::123456789000:role/redshift_iam_role"
    )
    aws_iam_role: Option[String] = None,
    @Property("temporary_aws_access_key_id", "AWS access key, must have write permissions to the S3 bucket.")
    temporary_aws_access_key_id: Option[Boolean] = None,
    @Property("query", "The query to read from in Redshift")
    query: Option[String] = None,
    @Property(
      "diststyle",
      "The Redshift Distribution Style to be used when creating a table. Can be one of EVEN, KEY or ALL (see Redshift docs). When using KEY, you must also set a distribution key with the distkey option."
    )
    diststyle: Option[String] = Some("error"),
    @Property("distkey", "The name of a column in the table to use as the distribution key when creating a table.")
    distkey: Option[String] = Some(""),
    @Property("SQL before loading")
    sqlPreAction: Option[SColumn] = None,
    @Property("SQL after loading")
    sqlPostAction: Option[SColumn] = None,
    @Property("Write Mode", """(default: "error") Specifies the behavior when data or table already exists.""")
    writeMode: Option[String] = None
  ) extends DatasetProperties

  implicit val redshiftPropertiesFormat: Format[RedshiftProperties] = Json.format

  def sourceDialog: DatasetDialog = DatasetDialog("redshift")
    .addSection(
      "LOCATION",
      ColumnsLayout()
        .addColumn(
          StackLayout(direction = Some("vertical"), gap = Some("1rem"))
            .addElement(TitleElement(title = "Credentials"))
            .addElement(
              Credentials("")
                .bindProperty("credentialScope")
            )
            .addElement(TitleElement(title = "URL"))
            .addElement(
              ColumnsLayout(gap = Some("1rem"))
                .addColumn(
                  SelectBox("JDBC Driver")
                    .addOption("Redshift", "jdbc:redshift")
                    .bindProperty("jdbcDriver"),
                  width = "1fr"
                )
                .addColumn(TextBox("JDBC URL").bindPlaceholder("").bindProperty("jdbcUrl"), width = "4fr")
            )
            .addElement(
              ColumnsLayout(gap = Some("1rem"))
                .addColumn(TextBox("Host").bindPlaceholder("").bindProperty("jdbcHost"), width = "4fr")
                .addColumn(TextBox("Port").bindPlaceholder("").bindProperty("jdbcPort"), width = "1fr")
            )
            .addElement(TextBox("Temporary Directory").bindPlaceholder("").bindProperty("jdbcTempDir"))
            .addElement(TitleElement(title = "Table"))
            .addElement(CatalogTableDB("").bindProperty("path").bindTableProperty("tableName"))
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
                        Checkbox("Forward S3 access credentials to databricks"),
                        "forward_spark_s3_credentials"
                      )
                      .addField(TextBox("AWS IAM Role").bindPlaceholder(""), "aws_iam_role")
                      .addField(
                        Checkbox("Temporary AWS access key id"),
                        "temporary_aws_access_key_id"
                      )
                      .addField(TextBox("Read Query").bindPlaceholder(""), "query")
                  )
                )
            ),
          "auto"
        )
        .addColumn(SchemaTable("").bindProperty("schema"), "5fr")
    )
    .addSection(
      "PREVIEW",
      PreviewTable("").bindProperty("schema")
    )

  def targetDialog: DatasetDialog = DatasetDialog("redshift")
    .addSection(
      "LOCATION",
      ColumnsLayout(gap = Some("1rem"))
        .addColumn(
          StackLayout(direction = Some("vertical"), gap = Some("1rem"))
            .addElement(TitleElement(title = "Credentials"))
            .addElement(
              Credentials("")
                .bindProperty("credentialScope")
            )
            .addElement(TitleElement(title = "URL"))
            .addElement(
              ColumnsLayout(gap = Some("1rem"))
                .addColumn(
                  SelectBox("JDBC Driver")
                    .addOption("Redshift", "jdbc:redshift")
                    .bindProperty("jdbcDriver"),
                  width = "1fr"
                )
                .addColumn(TextBox("JDBC URL").bindPlaceholder("").bindProperty("jdbcUrl"), width = "4fr")
            )
            .addElement(
              ColumnsLayout(gap = Some("1rem"))
                .addColumn(TextBox("Host").bindPlaceholder("").bindProperty("jdbcHost"), width = "4fr")
                .addColumn(TextBox("Port").bindPlaceholder("").bindProperty("jdbcPort"), width = "1fr")
            )
            .addElement(TextBox("Temporary Directory").bindPlaceholder("").bindProperty("jdbcTempDir"))
            .addElement(TitleElement(title = "Table"))
            .addElement(CatalogTableDB("").bindProperty("path").bindTableProperty("tableName"))
        )
    )
    .addSection(
      "PROPERTIES",
      ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
        .addColumn(
          ScrollBox().addElement(
            StackLayout()
              .addElement(
                Checkbox("Forward S3 access credentials to databricks").bindProperty("forward_spark_s3_credentials")
              )
              .addElement(TextBox("AWS IAM Role").bindPlaceholder("").bindProperty("aws_iam_role"))
              .addElement(
                Checkbox("Temporary AWS access key id").bindProperty("temporary_aws_access_key_id")
              )
              .addElement(
                SelectBox("Write Mode")
                  .addOption("error", "error")
                  .addOption("overwrite", "overwrite")
                  .addOption("append", "append")
                  .addOption("ignore", "ignore")
                  .bindProperty("writeMode")
              )
              .addElement(
                SelectBox("Row distribution style for new table")
                  .addOption("EVEN", "EVEN")
                  .addOption("KEY", "KEY")
                  .addOption("ALL", "ALL")
                  .bindProperty("diststyle")
              )
              .addElement(TextBox("Distribution key for new table").bindPlaceholder("").bindProperty("distkey"))
          ),
          "auto"
        )
        .addColumn(
          StackLayout()
            .addElement(TitleElement("Steps To Run"))
            .addElement(TitleElement("1. SQL before loading:", level = Some(5)))
            .addElement(Editor(height = Some("500px")).bindProperty("sqlPreAction.expression"))
            .addElement(NativeText("2. Loading the data to: <Table_Name>"))
            .addElement(TitleElement("3. SQL after loading:", level = Some(5)))
            .addElement(Editor(height = Some("500px")).bindProperty("sqlPostAction.expression"))
            .addElement(SchemaTable("").isReadOnly().withoutInferSchema().bindProperty("schema")),
          "5fr"
        )
    )

  override def validate(component: Component)(implicit context: WorkflowContext): List[Diagnostic] = {
    import scala.collection.mutable.ListBuffer
    val diagnostics = ListBuffer[Diagnostic]()
    diagnostics ++= super.validate(component)

    println("redshift validate component: ", component)

    if (component.properties.path.isEmpty) {
      diagnostics += Diagnostic("properties.path", "path variable cannot be empty [Location]", SeverityLevel.Error)
    }
    if (component.properties.schema.isEmpty) {
//      diagnostics += Diagnostic("properties.schema", "Schema cannot be empty [Properties]", SeverityLevel.Error)
    }

    if (component.properties.sqlPreAction.isDefined && component.properties.sqlPreAction.get.expression.isEmpty)
      diagnostics += Diagnostic(
        "properties.sqlPreAction.expression",
        s"Unsupported expression ${component.properties.sqlPreAction.get.expression}",
        SeverityLevel.Error
      )
    if (component.properties.sqlPostAction.isDefined && component.properties.sqlPostAction.get.expression.isEmpty)
      diagnostics += Diagnostic(
        "properties.sqlPostAction.expression",
        s"Unsupported expression ${component.properties.sqlPostAction.get.expression}",
        SeverityLevel.Error
      )

    diagnostics.toList
  }

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = {
    val newProps = newState.properties
    val oldProps = oldState.properties

    println(s"oldProps => s$oldProps")
    println(s"newProps => s$newProps")

    // If url is updated => update host, port and database
    if (oldProps.jdbcUrl != newProps.jdbcUrl) {
      val splitAtColon = newProps.jdbcUrl.split(":")
      val (updatedHost, portAndDB) = (splitAtColon(0), splitAtColon(1))
      val splitAtSlash = portAndDB.split("/")
      val (updatedPort, updatedDB) = splitAtSlash.size match {
        case 1 ⇒ (splitAtSlash(0), "")
        case 2 ⇒
          val extractedDBName = splitAtSlash(1).split("[;?&]")(0)
          (splitAtSlash(0), extractedDBName)
      }
      newState.copy(properties = newProps.copy(jdbcHost = updatedHost, jdbcPort = updatedPort, path = updatedDB))
    }
    // If host or port or DB is updated => update url
    else {
      val updatedJdbcUrl = s"${newProps.jdbcHost}:${newProps.jdbcPort}/${newProps.path}"
      newState.copy(properties = newProps.copy(jdbcUrl = updatedJdbcUrl))
    }

  }

  class RedshiftFormatCode(props: RedshiftProperties) extends ComponentCode {

    def sourceApply(spark: SparkSession): DataFrame = {
      import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
      import spark.implicits._

      var reader = spark.read.format("com.databricks.spark.redshift")

      props.schema.foreach(schema ⇒ reader = reader.schema(schema))

      val jdbcURLWithDriver = s"${props.jdbcDriver}://${props.jdbcUrl}"

      reader = reader
        .option("aws_iam_role", props.aws_iam_role)
        .option("forward_spark_s3_credentials", props.forward_spark_s3_credentials)
        .option("tempDir", props.jdbcTempDir)
        .option("url", jdbcURLWithDriver)
        .option("user", dbutils.secrets.get(scope = props.credentialScope, key = "username"))
        .option("password", dbutils.secrets.get(scope = props.credentialScope, key = "password"))

      reader.load()
    }

    def targetApply(spark: SparkSession, in: DataFrame): Unit = {

      import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
      import spark.implicits._
      var writer = in.write
        .format("com.databricks.spark.redshift")

      props.sqlPreAction.foreach { preactions: SColumn ⇒ writer = writer.option("preactions", preactions.expression) }
      props.sqlPostAction.foreach { postactions: SColumn ⇒
        writer = writer.option("postactions", postactions.expression)
      }

      writer = writer
        .option("distkey", props.distkey)
        .option("aws_iam_role", props.aws_iam_role)
        .option("tempDir", props.jdbcTempDir)
        .option("dbtable", props.tableName)
        .option("diststyle", props.diststyle)
        .option("forward_spark_s3_credentials", props.forward_spark_s3_credentials)
        .option("url", props.jdbcUrl)
        .option("temporary_aws_access_key_id", props.temporary_aws_access_key_id)
        .option("user", dbutils.secrets.get(scope = "credentials", key = "username"))
        .option("password", dbutils.secrets.get(scope = "credentials", key = "password"))

      props.writeMode.foreach(mode ⇒ writer = writer.mode(mode))

      writer.save()
    }

  }

  override def deserializeProperty(props: String): PropertiesType = Json.parse(props).as[PropertiesType]

  override def serializeProperty(props: PropertiesType): String = Json.stringify(Json.toJson(props))
}
