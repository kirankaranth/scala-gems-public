package io.prophecy.scalagems.dataset

import io.prophecy.gems._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.datasetSpec._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.uiSpec._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.{Format, Json}

class JDBC_QUERY extends DatasetSpec {

  val name: String = "JDBC_QUERY"
  val datasetType: String = "File"

  type PropertiesType = SQLStatementProperties
  case class SQLStatementProperties(
    @Property("Schema")
    schema: Option[StructType] = Some(
      StructType(
        Array(
          StructField("success", BooleanType, true)
        )
      )
    ),
    @Property("Description")
    description: Option[String] = Some(""),
    @Property("driver")
    driver: String = "org.postgresql.Driver",
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
    @Property("Input Port Names", "")
    sql: String = "DELETE FROM customers WHERE customer_id > 0"
  ) extends DatasetProperties

  implicit val SQLStatementPropertiesFormat: Format[SQLStatementProperties] = Json.format
  @Property("Header Wrapper")
  case class HeaderValue(
    @Property("Header") header: String
  )

  def sourceDialog: DatasetDialog = DatasetDialog("csv")
    .addSection(
      "LOCATION",
      ScrollBox()
        .addElement(
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
            .addElement(TitleElement(title = "URL"))
            .addElement(
              TextBox("Driver").bindPlaceholder("org.postgresql.Driver").bindProperty("driver")
            )
            .addElement(
              TextBox("JDBC URL")
                .bindPlaceholder("jdbc:<sqlserver>://<jdbcHostname>:<jdbcPort>/<jdbcDatabase>")
                .bindProperty("jdbcUrl")
            )
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
                  )
                )
            ),
          "auto"
        )
        .addColumn(
          StackLayout()
            .addElement(TitleElement(title = "Query"))
            .addElement(
              Editor(height = Some("60bh"))
                .withSchemaSuggestions()
                .bindProperty("sql")
            )
        )
    )
    .addSection(
      "PREVIEW",
      PreviewTable("").bindProperty("schema")
    )

  def targetDialog: DatasetDialog = DatasetDialog("csv")

  override def validate(component: Component)(implicit context: WorkflowContext): List[Diagnostic] =
    Nil

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = newState

  override def deserializeProperty(props: String): PropertiesType = Json.parse(props).as[PropertiesType]

  override def serializeProperty(props: PropertiesType): String = Json.stringify(Json.toJson(props))

  class SQLCode(props: SQLStatementProperties) extends ComponentCode {

    def sourceApply(spark: SparkSession): DataFrame = {
      import java.sql._
      var connection: Connection = null
      try {
        connection = props.credType match {
          case "databricksSecrets" ⇒
            import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
            DriverManager.getConnection(
              props.jdbcUrl,
              dbutils.secrets.get(scope = props.credentialScope.get, key = "username"),
              dbutils.secrets.get(scope = props.credentialScope.get, key = "password")
            )
          case "userPwd" ⇒
            DriverManager.getConnection(
              props.jdbcUrl,
              props.textUsername.get,
              props.textPassword.get
            )
        }
        val statement = connection.prepareStatement(props.sql)
        try statement.executeUpdate()
        finally statement.close()
      } finally if (connection != null) connection.close()
      spark.range(1).select(lit(true).as("result"))
    }

    def targetApply(spark: SparkSession, in: DataFrame): Unit = {}
  }
}
