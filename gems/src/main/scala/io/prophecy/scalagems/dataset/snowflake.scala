package io.prophecy.scalagems.dataset

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import io.prophecy.gems._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.uiSpec._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.datasetSpec._
import play.api.libs.json.{Format, Json}

class snowflake extends DatasetSpec {

  val name: String = "snowflake"
  val datasetType: String = "Warehouse"
  val docUrl: String = "https://docs.prophecy.io/low-code-spark/gems/source-target/warehouse/snowflake"

  type PropertiesType = SnowflakeProperties
  case class SnowflakeProperties(
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
    @Property("Private key path")
    keyFilepath: Option[String] = None,
    @Property("Private key passphrase")
    keyPasskey: Option[String] = None,
    @Property("url", "")
    url: String = "",
    @Property("database", "")
    database: String = "",
    @Property("schemaName", "")
    schemaName: String = "",
    @Property("warehouse", "")
    warehouse: String = "",
    @Property("role", "")
    role: Option[String] = Some(""),
    @Property("readFromSource")
    readFromSource: String = "dbtable",
    @Property("dbtable")
    dbtable: Option[String] = None,
    @Property("query", "The query to read from in Snowflake")
    query: Option[String] = None,
    @Property("Write Mode", """(default: "error") Specifies the behavior when data or table already exists.""")
    writeMode: Option[String] = Some("overwrite"),
    @Property("postSql")
    postSql: Option[String] = None,
    @Property("shouldPostSql", "")
    shouldPostSql: Option[Boolean] = None
  ) extends DatasetProperties

  implicit val snowflakePropertiesFormat: Format[SnowflakeProperties] = Json.format

  def sourceDialog: DatasetDialog = {
    val urlSection = StackLayout()
      .addElement(TitleElement(title = "URL"))
      .addElement(
        ColumnsLayout(gap = Some("1rem"))
          .addColumn(
            TextBox("URL")
              .bindPlaceholder("https://***.us-east-1.snowflakecomputing.com")
              .bindProperty("url")
          )
          .addColumn(TextBox("Database").bindPlaceholder("database").bindProperty("database"))
      )
      .addElement(
        ColumnsLayout(gap = Some("1rem"))
          .addColumn(TextBox("Schema").bindPlaceholder("schema").bindProperty("schemaName"))
          .addColumn(TextBox("Warehouse").bindPlaceholder("warehouse").bindProperty("warehouse"))
          .addColumn(TextBox("Role").bindPlaceholder("role").bindProperty("role"))
      )

    DatasetDialog("snowflake")
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
                      .addOption("Key Pair Authentication", "userP8")
                      .bindProperty("credType")
                  )
                  .addElement(
                    Condition()
                      .ifEqual(PropExpr("component.properties.credType"), StringExpr("databricksSecrets"))
                      .then(Credentials("").bindProperty("credentialScope"))
                      .otherwise(
                        Condition()
                          .ifEqual(PropExpr("component.properties.credType"), StringExpr("userP8"))
                          .then(
                            ColumnsLayout(gap = Some("1rem"))
                              .addColumn(TextBox("Username").bindPlaceholder("username").bindProperty("textUsername"))
                              .addColumn(
                                TextBox("Private key path in PKCS8 format")
                                  .bindPlaceholder("${config_private_key_path}")
                                  .bindProperty("keyFilepath")
                              )
                              .addColumn(
                                TextBox("Private key passphrase")
                                  .isPassword()
                                  .bindPlaceholder("${config_passphrase}")
                                  .bindProperty("keyPasskey")
                              )
                          )
                          .otherwise(
                            StackLayout()
                              .addElement(
                                ColumnsLayout(gap = Some("1rem"))
                                  .addColumn(
                                    TextBox("Username").bindPlaceholder("username").bindProperty("textUsername")
                                  )
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
              )
              .addElement(urlSection)
              .addElement(
                StackLayout()
                  .addElement(
                    RadioGroup("Data Source")
                      .addOption("DB Table", "dbtable")
                      .addOption("SQL Query", "query")
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
      .addElement(TitleElement(title = "URL"))
      .addElement(
        ColumnsLayout(gap = Some("1rem"))
          .addColumn(
            TextBox("URL")
              .bindPlaceholder("https://***.us-east-1.snowflakecomputing.com")
              .bindProperty("url")
          )
          .addColumn(TextBox("Database").bindPlaceholder("database").bindProperty("database"))
      )
      .addElement(
        ColumnsLayout(gap = Some("1rem"))
          .addColumn(TextBox("Schema").bindPlaceholder("schema").bindProperty("schemaName"))
          .addColumn(TextBox("Warehouse").bindPlaceholder("warehouse").bindProperty("warehouse"))
          .addColumn(TextBox("Role").bindPlaceholder("role").bindProperty("role"))
      )

    DatasetDialog("snowflake")
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
                      .addOption("Key Pair Authentication", "userP8")
                      .bindProperty("credType")
                  )
                  .addElement(
                    Condition()
                      .ifEqual(PropExpr("component.properties.credType"), StringExpr("databricksSecrets"))
                      .then(Credentials("").bindProperty("credentialScope"))
                      .otherwise(
                        Condition()
                          .ifEqual(PropExpr("component.properties.credType"), StringExpr("userP8"))
                          .then(
                            ColumnsLayout(gap = Some("1rem"))
                              .addColumn(TextBox("Username").bindPlaceholder("username").bindProperty("textUsername"))
                              .addColumn(
                                TextBox("Private key path in PKCS8 format")
                                  .bindPlaceholder("${config_private_key_path}")
                                  .bindProperty("keyFilepath")
                              )
                              .addColumn(
                                TextBox("Private key passphrase")
                                  .isPassword()
                                  .bindPlaceholder("${config_passphrase}")
                                  .bindProperty("keyPasskey")
                              )
                          )
                          .otherwise(
                            StackLayout()
                              .addElement(
                                ColumnsLayout(gap = Some("1rem"))
                                  .addColumn(
                                    TextBox("Username").bindPlaceholder("username").bindProperty("textUsername")
                                  )
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
              )
              .addElement(urlSection)
              .addElement(TitleElement(title = "Table"))
              .addElement(TextBox("Table").bindPlaceholder("tablename").bindProperty("dbtable"))
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
                        .addOption("error", "error")
                        .addOption("overwrite", "overwrite")
                        .addOption("append", "append")
                        .addOption("ignore", "ignore"),
                      "writeMode"
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
              )
              .addElement(TitleElement(title = "Query"))
              .addElement(Checkbox("Run post-script SQL").bindProperty("shouldPostSql"))
              .addElement(
                Condition()
                  .ifEqual(PropExpr("component.properties.shouldPostSql"), BooleanExpr(true))
                  .then(
                    Editor(height = Some("60bh"))
                      .withSchemaSuggestions()
                      .bindProperty("postSql")
                  )
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
        if (!component.properties.textPassword.getOrElse("").trim.startsWith("${")) {
          diagnostics += Diagnostic(
            "properties.textPassword",
            "Storing plain-text passwords poses a security risk and is not recommended.",
            SeverityLevel.Warning
          )
        }
      case "userP8" =>
        if (component.properties.textUsername.getOrElse("").trim.isEmpty) {
          diagnostics += Diagnostic(
            "properties.textUsername",
            "Username cannot be empty [Location]",
            SeverityLevel.Error
          )
        }
        if (component.properties.keyFilepath.getOrElse("").trim.isEmpty) {
          diagnostics += Diagnostic(
            "properties.keyFilepath",
            "Private key file path cannot be empty [Location]",
            SeverityLevel.Error
          )
        }
    }

    component.properties.readFromSource match {
      case "dbtable" ⇒
        if (component.properties.dbtable.getOrElse("").trim.isEmpty) {
          diagnostics += Diagnostic(
            "properties.dbtable",
            "Table cannot be empty [Location]",
            SeverityLevel.Error
          )
        }
      case "query" ⇒
        if (component.properties.query.getOrElse("").trim.isEmpty) {
          diagnostics += Diagnostic(
            "properties.query",
            "Query cannot be empty [Location]",
            SeverityLevel.Error
          )
        }
    }

    diagnostics.toList
  }

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = newState

  class SnowflakeFormatCode(props: SnowflakeProperties) extends ComponentCode {

    def sourceApply(spark: SparkSession): DataFrame = {
      import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
      val sfRole = if (props.role.isDefined) props.role.get else ""
      val options = props.credType match {
        case "databricksSecrets" ⇒
          Map(
            "sfUrl" → props.url,
            "sfUser" → dbutils.secrets.get(scope = props.credentialScope.get, key = "username"),
            "sfPassword" → dbutils.secrets.get(scope = props.credentialScope.get, key = "password"),
            "sfDatabase" → props.database,
            "sfSchema" → props.schemaName,
            "sfWarehouse" → props.warehouse,
            "sfRole" -> sfRole
          )
        case "userPwd" ⇒
          Map(
            "sfUrl" → props.url,
            "sfUser" → props.textUsername.get,
            "sfPassword" → props.textPassword.get,
            "sfDatabase" → props.database,
            "sfSchema" → props.schemaName,
            "sfWarehouse" → props.warehouse,
            "sfRole" -> sfRole
          )
        case "userP8" =>
          val keyContent = spark.sparkContext
            .wholeTextFiles(props.keyFilepath.get)
            .first()
            ._2
            .replaceAll("(-+.*PRIVATE KEY-+)|\n", "")

          import org.bouncycastle.jce.provider.BouncyCastleProvider
          import org.bouncycastle.util.encoders.Base64

          import java.security.Security

          Security.addProvider(new BouncyCastleProvider())
          val privateKey = if (props.keyPasskey.isEmpty || props.keyPasskey.get.isEmpty) {
            import java.security.spec.PKCS8EncodedKeySpec
            new String(Base64.encode(new PKCS8EncodedKeySpec(Base64.decode(keyContent)).getEncoded))
          } else {
            import org.bouncycastle.openssl.jcajce.{JcaPEMKeyConverter, JceOpenSSLPKCS8DecryptorProviderBuilder}
            import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo
            val pkcs8Prov = new JceOpenSSLPKCS8DecryptorProviderBuilder()
              .build(props.keyPasskey.get.toCharArray)
            val pkInfo = new PKCS8EncryptedPrivateKeyInfo(Base64.decode(keyContent))
            new String(
              Base64.encode(
                new JcaPEMKeyConverter()
                  .setProvider(BouncyCastleProvider.PROVIDER_NAME)
                  .getPrivateKey(pkInfo.decryptPrivateKeyInfo(pkcs8Prov))
                  .getEncoded
              )
            )
          }

          Map(
            "sfUrl" -> props.url,
            "sfUser" -> props.textUsername.get,
            "sfDatabase" -> props.database,
            "sfSchema" -> props.schemaName,
            "sfWarehouse" -> props.warehouse,
            "pem_private_key" -> privateKey,
            "sfRole" -> sfRole
          )
      }

      var reader = spark.read
        .format("snowflake")
        .options(options)

      reader = props.readFromSource match {
        case "dbtable" ⇒ reader.option("dbtable", props.dbtable.get)
        case "query" ⇒ reader.option("query", props.query.get)
      }

      reader.load()

    }

    def targetApply(spark: SparkSession, in: DataFrame): Unit = {

      import net.snowflake.spark.snowflake.Utils
      import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
      val sfRole = if (props.role.isDefined) props.role.get else ""
      val options = props.credType match {
        case "databricksSecrets" ⇒
          Map(
            "sfUrl" → props.url,
            "sfUser" → dbutils.secrets.get(scope = props.credentialScope.get, key = "username"),
            "sfPassword" → dbutils.secrets.get(scope = props.credentialScope.get, key = "password"),
            "sfDatabase" → props.database,
            "sfSchema" → props.schemaName,
            "sfWarehouse" → props.warehouse,
            "sfRole" -> sfRole
          )
        case "userPwd" ⇒
          Map(
            "sfUrl" → props.url,
            "sfUser" → props.textUsername.get,
            "sfPassword" → props.textPassword.get,
            "sfDatabase" → props.database,
            "sfSchema" → props.schemaName,
            "sfWarehouse" → props.warehouse,
            "sfRole" -> sfRole
          )
        case "userP8" =>
          val keyContent = spark.sparkContext
            .wholeTextFiles(props.keyFilepath.get)
            .first()
            ._2
            .replaceAll("(-+.*PRIVATE KEY-+)|\n", "")

          import org.bouncycastle.jce.provider.BouncyCastleProvider
          import org.bouncycastle.util.encoders.Base64

          import java.security.Security

          Security.addProvider(new BouncyCastleProvider())
          val privateKey = if (props.keyPasskey.isEmpty || props.keyPasskey.get.isEmpty) {
            import java.security.spec.PKCS8EncodedKeySpec
            new String(Base64.encode(new PKCS8EncodedKeySpec(Base64.decode(keyContent)).getEncoded))
          } else {
            import org.bouncycastle.openssl.jcajce.{JcaPEMKeyConverter, JceOpenSSLPKCS8DecryptorProviderBuilder}
            import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo
            val pkcs8Prov = new JceOpenSSLPKCS8DecryptorProviderBuilder()
              .build(props.keyPasskey.get.toCharArray)
            val pkInfo = new PKCS8EncryptedPrivateKeyInfo(Base64.decode(keyContent))
            new String(
              Base64.encode(
                new JcaPEMKeyConverter()
                  .setProvider(BouncyCastleProvider.PROVIDER_NAME)
                  .getPrivateKey(pkInfo.decryptPrivateKeyInfo(pkcs8Prov))
                  .getEncoded
              )
            )
          }

          Map(
            "sfUrl" -> props.url,
            "sfUser" -> props.textUsername.get,
            "sfDatabase" -> props.database,
            "sfSchema" -> props.schemaName,
            "sfWarehouse" -> props.warehouse,
            "pem_private_key" -> privateKey,
            "sfRole" -> sfRole
          )
      }

      var writer = in.write
        .format("snowflake")
        .options(options)

      props.dbtable.foreach(dbtablename ⇒ writer = writer.option("dbtable", dbtablename))
      props.writeMode.foreach(mode ⇒ writer = writer.mode(mode))

      if (props.shouldPostSql == Some(true)) {
        Utils.runQuery(options, props.postSql.get)
      }

      writer.save()
    }

  }

  override def deserializeProperty(props: String): PropertiesType = Json.parse(props).as[PropertiesType]

  override def serializeProperty(props: PropertiesType): String = Json.stringify(Json.toJson(props))
}
