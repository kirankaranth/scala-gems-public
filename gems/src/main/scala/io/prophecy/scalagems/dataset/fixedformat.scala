package io.prophecy.scalagems.dataset

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import io.prophecy.gems._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.uiSpec._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.datasetSpec._
import play.api.libs.json.{Format, Json}

class fixedformat extends DatasetSpec {

  val name: String = "fixedformat"
  val datasetType: String = "File"
  val docUrl: String = "https://docs.prophecy.io/low-code-spark/gems/source-target/file/fixed-format"

  type PropertiesType = FixedProperties
  case class FixedProperties(
    @Property("Schema")
    schema: Option[StructType] = None,
    @Property("Description")
    description: Option[String] = Some(""),
    @Property("FixedSchemaString")
    fixedSchemaString: Option[String] = Some(""),
    @Property("FixedSchemaJson")
    fixedSchemaJson: Option[String] = Some(""),
    @Property("Path")
    path: Option[String] = Some(""),
    @Property("Write Mode", """(default: "error") Specifies the behavior when data or table already exists.""")
    writeMode: Option[String] = Some("error"),
    @Property("SkipHeaders", "Skips n lines from top of the file. Currently supports only single files.")
    skipHeaders: Option[String] = None,
    @Property("SkipFooters", "Skips n lines from bottom of the file. Currently supports only single files.")
    skipFooters: Option[String] = None
  ) extends DatasetProperties

  implicit val fixedPropertiesFormat: Format[FixedProperties] = Json.format

  def sourceDialog: DatasetDialog = DatasetDialog("text")
    .addSection("LOCATION", TargetLocation("path"))
    .addSection(
      "PROPERTIES",
      ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
        .addColumn(
          ScrollBox()
            .addElement(
              StackLayout(gap = Some("1rem"))
                .addElement(
                  StackItem(grow = Some(1))
                    .addElement(
                      FieldPicker(height = Some("100%"))
                        .addField(
                          TextArea("Description", 2, placeholder = "Dataset description..."),
                          "description",
                          true
                        )
                        .addField(TextBox("Skip header lines").bindPlaceholder("3"), "skipHeaders")
                        .addField(TextBox("Skip footer lines").bindPlaceholder("3"), "skipFooters")
                    )
                )
                .addElement(
                  StackLayout()
                    .addElement(TitleElement("Fixed Format Schema"))
                    .addElement(Editor(height = Some("70bh")).bindProperty("fixedSchemaString"))
                )
            ),
          "2fr"
        )
        .addColumn(SchemaTable("").bindProperty("schema"), "5fr")
    )
    .addSection(
      "PREVIEW",
      PreviewTable("").bindProperty("schema")
    )

  def targetDialog: DatasetDialog = DatasetDialog("text")
    .addSection("LOCATION", TargetLocation("path"))
    .addSection(
      "PROPERTIES",
      ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
        .addColumn(
          ScrollBox()
            .addElement(
              StackLayout(gap = Some("1rem"))
                .addElement(
                  SelectBox("Write Mode")
                    .addOption("error", "error")
                    .addOption("overwrite", "overwrite")
                    .addOption("append", "append")
                    .addOption("ignore", "ignore")
                    .bindProperty("writeMode")
                )
                .addElement(
                  StackItem(grow = Some(1))
                    .addElement(
                      TextArea("Description", 2, placeholder = "Dataset description...")
                    )
                )
                .addElement(
                  StackLayout()
                    .addElement(TitleElement("Fixed Format Schema"))
                    .addElement(Editor(height = Some("70bh")).bindProperty("fixedSchemaString"))
                )
            ),
          "2fr"
        )
        .addColumn(SchemaTable("").isReadOnly().withoutInferSchema().bindProperty("schema"), "3fr")
    )

  override def validate(component: Component)(implicit context: WorkflowContext): List[Diagnostic] = {
    import scala.collection.mutable.ListBuffer
    val diagnostics = ListBuffer[Diagnostic]()
    diagnostics ++= super.validate(component)

    if (component.properties.path.isEmpty) {
      diagnostics += Diagnostic("properties.path", "path variable cannot be empty [Location]", SeverityLevel.Error)
      diagnostics += Diagnostic(
        "properties.path",
        s"path variable cannot be empty ${component.properties.path}",
        SeverityLevel.Error
      )
    }

    if (component.properties.skipHeaders.isDefined && !component.properties.skipHeaders.get.matches("[0-9]+"))
      diagnostics += Diagnostic(
        "properties.skipHeaders",
        "Skip header lines must be a non-negative number",
        SeverityLevel.Error
      )

    if (component.properties.skipFooters.isDefined && !component.properties.skipFooters.get.matches("[0-9]+"))
      diagnostics += Diagnostic(
        "properties.skipFooters",
        "Skip footer lines must be a non-negative number",
        SeverityLevel.Error
      )

    diagnostics.toList
  }

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = {
    import io.prophecy.abinitio.dml.DMLSchema.parse
    import io.prophecy.libs.FFSchemaRecord
    import io.prophecy.libs.FixedFormatSchemaImplicits._
    import play.api.libs.json.Json

    val newProps = newState.properties
    val oldProps = oldState.properties

    println(s"oldProps => s$oldProps")
    println(s"newProps => s$newProps")

    try {
      // If schema is updated, reparse
      if (oldProps.fixedSchemaString != newProps.fixedSchemaString) {
        val schema = parse(newProps.fixedSchemaString.getOrElse("")).asInstanceOf[FFSchemaRecord]
        val fixedSchema = Json.stringify(Json.toJson(schema))
        newState.copy(properties = newProps.copy(schema = Some(schema.toSpark), fixedSchemaJson = Some(fixedSchema)))
      } else {
        newState
      }
    } catch {
      case e: Throwable =>
        println(s"throwable occurred: ${e}")
        newState
      case e: Error =>
        println(s"Error occurred: ${e}")
        newState
    }
  }

  override def deserializeProperty(props: String): PropertiesType = Json.parse(props).as[PropertiesType]

  override def serializeProperty(props: PropertiesType): String = Json.stringify(Json.toJson(props))

  class FixedFormatCode(props: FixedProperties) extends ComponentCode {

    def sourceApply(spark: SparkSession): DataFrame = {
      import _root_.io.prophecy.abinitio.dml.DMLSchema.parse
      import _root_.io.prophecy.libs.FFSchemaRecord
      import _root_.io.prophecy.libs.FixedFormatSchemaImplicits._
      import play.api.libs.json.Json

      var df: DataFrame = spark.emptyDataFrame
      try {
        val schema = props.fixedSchemaString.map(s ⇒ parse(s).asInstanceOf[FFSchemaRecord])
        val fixedSchema = schema.map(s ⇒ Json.stringify(Json.toJson(s)))
        var reader = spark.read
          .option("schema", fixedSchema.getOrElse(""))
          .format("io.prophecy.libs.FixedFileFormat")

        if (props.skipHeaders.isDefined && props.skipHeaders.get != "0")
          reader = reader.option("skip_header_lines", s"${props.skipHeaders.get}")
        if (props.skipFooters.isDefined && props.skipFooters.get != "0")
          reader = reader.option("skip_footer_lines", s"${props.skipFooters.get}")
        df = reader.load(props.path.getOrElse(""))
      } catch {
        case e: Error =>
          println(s"Error occurred while reading dataframe: ${e}")
          throw new Exception(e.getMessage)
        case e: Throwable =>
          println(s"Throwable occurred while reading dataframe: ${e}")
          throw new Exception(e.getMessage)
      }
      df
    }

    def targetApply(spark: SparkSession, in: DataFrame): Unit = {
      import _root_.io.prophecy.abinitio.dml.DMLSchema.parse
      import _root_.io.prophecy.libs.FFSchemaRecord
      import _root_.io.prophecy.libs.FixedFormatSchemaImplicits._
      import play.api.libs.json.Json

      try {
        val schema = props.fixedSchemaString.map(s ⇒ parse(s).asInstanceOf[FFSchemaRecord])
        val fixedSchema = schema.map(s ⇒ Json.stringify(Json.toJson(s)))

        var writer = in.write
          .format("io.prophecy.libs.FixedFileFormat")
        props.writeMode.foreach { mode ⇒
          writer = writer.mode(mode)
        }
        fixedSchema.foreach { schema ⇒
          writer = writer.option("schema", schema)
        }
        writer.save(props.path.getOrElse(""))
      } catch {
        case e: Error =>
          println(s"Error occurred while writing dataframe: ${e}")
          throw new Exception(e.getMessage)
        case e: Throwable =>
          println(s"Throwable occurred while writing dataframe: ${e}")
          throw new Exception(e.getMessage)
      }
    }
  }

}
