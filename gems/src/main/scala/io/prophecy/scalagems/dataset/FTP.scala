package io.prophecy.scalagems.dataset

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import io.prophecy.gems._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.datasetSpec._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.uiSpec._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.{Format, Json}

class FTP extends DatasetSpec {

  val name: String = "FTP"
  val datasetType: String = "File"
  val docUrl: String = "https://docs.prophecy.io/low-code-spark/gems/source-target/file/ftp"

  type PropertiesType = FTPProperties
  case class FTPProperties(
    @Property("Schema")
    schema: Option[StructType] = Some(
      StructType(
        Array(
          StructField("value", BinaryType, true)
        )
      )
    ),
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
    @Property("host", "")
    host: String = "",
    @Property("path", "")
    path: String = ""
  ) extends DatasetProperties

  implicit val FTPPropertiesFormat: Format[FTPProperties] = Json.format
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
              TextBox("Host")
                .bindPlaceholder("192.168.1.5")
                .bindProperty("host")
            )
            .addElement(
              TextBox("Path")
                .bindPlaceholder("file.txt")
                .bindProperty("path")
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
          SchemaTable("")
            .bindProperty("schema")
            .withoutInferSchema()
            .isReadOnly()
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

  class FTPCode(props: FTPProperties) extends ComponentCode {

    def sourceApply(spark: SparkSession): DataFrame = {
      var username: String = null
      var password: String = null

      props.credType match {
        case "databricksSecrets" ⇒
          username = dbutils.secrets.get(scope = props.credentialScope.get, key = "username")
          password = dbutils.secrets.get(scope = props.credentialScope.get, key = "password")
        case "userPwd" ⇒
          username = props.textUsername.get
          password = props.textPassword.get
      }

      import org.apache.commons.net.ftp._

      import java.io.FileOutputStream

      val client = new FTPClient
      client.connect(props.host)
      client.enterLocalPassiveMode()
      client.login(username, password)

      val sourcePath = s"/tmp/${props.path}"
      val finalPath = s"dbfs:/FileStore/${props.path}"
      val stream = new FileOutputStream(sourcePath)
      client.retrieveFile(props.path, stream)
      stream.close()
      dbutils.fs.cp("file:" + sourcePath, finalPath)

      spark.read.format("text").load(finalPath)
    }

    def targetApply(spark: SparkSession, in: DataFrame): Unit = {}

  }

}
