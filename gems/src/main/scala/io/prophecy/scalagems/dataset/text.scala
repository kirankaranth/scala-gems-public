package io.prophecy.scalagems.dataset

import io.prophecy.gems._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.datasetSpec._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.uiSpec._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.{Format, Json}

class text extends DatasetSpec {

  val name: String = "text"
  val datasetType: String = "File"
  val docUrl: String = "https://docs.prophecy.io/low-code-spark/gems/source-target/file/text"

  type PropertiesType = TextProperties
  case class TextProperties(
    @Property("Schema")
    schema: Option[StructType] = None,
    @Property("Description")
    description: Option[String] = Some(""),
    @Property("useSchema")
    useSchema: Option[Boolean] = Some(true),
    @Property("Path")
    path: String = "",
    @Property("wholetext", "(default: false) If true, read a file as a single row and not split by \"\\n\"")
    wholetext: Option[Boolean] = None,
    @Property("Write Mode", """(default: "error") Specifies the behavior when data or table already exists.""")
    writeMode: Option[String] = None,
    @Property("compression", "(default: none) compression codec to use when saving to file.")
    compression: Option[String] = None,
    @Property("partitionColumns", "Partitioning column.")
    partitionColumns: Option[List[String]] = None,
    @Property("lineSep", """(default: "\\n") defines the line separator that should be used for parsing.""")
    lineSep: Option[String] = None,
    @Property("", "recursively scan a directory for files. Using this option disables partition discovery")
    recursiveFileLookup: Option[Boolean] = None
  ) extends DatasetProperties

  implicit val textPropertiesFormat: Format[TextProperties] = Json.format

  def sourceDialog: DatasetDialog = DatasetDialog("text")
    .addSection("LOCATION", TargetLocation("path"))
    .addSection(
      "PROPERTIES",
      ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
        .addColumn(
          ScrollBox().addElement(
            StackLayout(height = Some("100%"))
              .addElement(Checkbox("Enforce schema").bindProperty("useSchema"))
              .addElement(
                StackItem(grow = Some(1))
                  .addElement(
                    FieldPicker(height = Some("100%"))
                      .addField(
                        TextArea("Description", 2, placeholder = "Dataset description..."),
                        "description",
                        true
                      )
                      .addField(Checkbox("Enforce schema"), "useSchema", true)
                      .addField(Checkbox("Read file as single row"), "wholetext")
                      .addField(TextBox("Line Separator").bindPlaceholder("").enableEscapeSequence(), "lineSep")
                      .addField(Checkbox("Recursive File Lookup"), "recursiveFileLookup")
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

  def targetDialog: DatasetDialog = DatasetDialog("text")
    .addSection("LOCATION", TargetLocation("path"))
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
                  .addField(
                    SelectBox("Compression Codec")
                      .addOption("none", "none")
                      .addOption("bzip2", "bzip2")
                      .addOption("gzip", "gzip")
                      .addOption("lz4", "lz4")
                      .addOption("snappy", "snappy")
                      .addOption("deflate", "deflate"),
                    "compression"
                  )
                  .addField(TextBox("Line Separator").bindPlaceholder("").enableEscapeSequence(), "lineSep")
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

    if (component.properties.path.isEmpty) {
      diagnostics += Diagnostic("properties.path", "path variable cannot be empty [Location]", SeverityLevel.Error)
    }
    if (component.properties.lineSep.isDefined && component.properties.lineSep.isEmpty) {
      diagnostics += Diagnostic(
        "properties.lineSep",
        "Line Separator cannot be empty [Properties]",
        SeverityLevel.Error
      )
    }
    if (component.properties.schema.isEmpty) {
      // diagnostics += Diagnostic("properties.schema", "Schema cannot be empty [Properties]", SeverityLevel.Error)
    }

    diagnostics.toList
  }

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = newState

  class TextFormatCode(props: TextProperties) extends ComponentCode {

    def sourceApply(spark: SparkSession): DataFrame = {
      var reader = spark.read
        .format("text")
        .option("wholetext", props.wholetext)
        .option("lineSep", props.lineSep)
        .option("recursiveFileLookup", props.recursiveFileLookup)

      if (props.useSchema.isDefined && props.useSchema.get)
        props.schema.foreach(schema ⇒ reader = reader.schema(schema))

      reader.load(props.path)

    }

    def targetApply(spark: SparkSession, in: DataFrame): Unit = {
      var writer = in.write
        .format("text")

      props.writeMode.foreach { mode ⇒
        writer = writer.mode(mode)
      }

      writer = writer.option("compression", props.compression)
      writer = writer.option("lineSep", props.lineSep)

      writer.save(props.path)
    }

  }

  override def deserializeProperty(props: String): PropertiesType = Json.parse(props).as[PropertiesType]

  override def serializeProperty(props: PropertiesType): String = Json.stringify(Json.toJson(props))
}
