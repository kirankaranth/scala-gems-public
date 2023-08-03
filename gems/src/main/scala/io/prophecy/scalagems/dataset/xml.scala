package io.prophecy.scalagems.dataset

import io.prophecy.gems._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.datasetSpec._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.uiSpec._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.{Format, Json}

class xml extends DatasetSpec {

  val name: String = "xml"
  val datasetType: String = "File"

  type PropertiesType = XMLProperties
  case class XMLProperties(
    @Property("Schema")
    schema: Option[StructType] = None,
    @Property("Description")
    description: Option[String] = Some(""),
    @Property("useSchema")
    useSchema: Option[Boolean] = Some(true),
    @Property("path")
    path: String = "",
    @Property("rowTag")
    rowTag: String = "",
    @Property("rootTag")
    rootTag: Option[String] = None,
    @Property("excludeAttribute")
    excludeAttribute: Option[Boolean] = None,
    @Property("nullValue")
    nullValue: Option[String] = None,
    @Property("mode")
    mode: Option[String] = None,
    @Property("attributePrefix")
    attributePrefix: Option[String] = None,
    @Property("valueTag")
    valueTag: Option[String] = None,
    @Property("ignoreSurroundingSpaces")
    ignoreSurroundingSpaces: Option[Boolean] = None,
    @Property("ignoreNamespace")
    ignoreNamespace: Option[Boolean] = None,
    @Property("timestampFormat")
    timestampFormat: Option[String] = None,
    @Property("dateFormat")
    dateFormat: Option[String] = None,
    @Property("declaration")
    declaration: Option[String] = None,
    @Property("partitionColumns", """(default: "") Partitioning column.""")
    partitionColumns: Option[List[String]] = None,
    @Property("Write Mode", """(default: "error") Specifies the behavior when data or table already exists.""")
    writeMode: Option[String] = None,
    @Property("compression", "(default: none) compression codec to use when saving to file.")
    compression: Option[String] = None
  ) extends DatasetProperties

  implicit val XMLPropertiesFormat: Format[XMLProperties] = Json.format

  def sourceDialog: DatasetDialog = DatasetDialog("text")
    .addSection("LOCATION", TargetLocation("path"))
    .addSection(
      "PROPERTIES",
      ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
        .addColumn(
          ScrollBox().addElement(
            StackLayout(height = Some("100%"))
              .addElement(
                StackItem(grow = Some(1))
                  .addElement(
                    FieldPicker(height = Some(" 100%"))
                      .addField(
                        TextArea("Description", 2, placeholder = "Dataset description..."),
                        "description",
                        true
                      )
                      .addField(Checkbox("Enforce schema"), "useSchema", true)
                      .addField(TextBox("Row Tag", placeholder = "ROW"), "rowTag", true)
                      .addField(Checkbox("Exclude Attributes"), "excludeAttribute")
                      .addField(TextBox("Null Value"), "nullValue")
                      .addField(
                        SelectBox("Parser Mode")
                          .addOption("Permissive", "PERMISSIVE")
                          .addOption("Drop Malformed", "DROPMALFORMED")
                          .addOption("Fail Fast", "FAILFAST"),
                        "mode"
                      )
                      .addField(TextBox("Attribute Prefix"), "attributePrefix")
                      .addField(TextBox("Value Tag"), "valueTag")
                      .addField(Checkbox("Ignore Surrounding Spaces"), "ignoreSurroundingSpaces")
                      .addField(Checkbox("Ignore Namespace"), "ignoreNamespace")
                      .addField(TextBox("Timestamp Format"), "timestampFormat")
                      .addField(TextBox("Date Format"), "dateFormat")
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
                      .addField(TextBox("Row Tag", placeholder = "ROW"), "rowTag", true)
                      .addField(TextBox("Root Tag", placeholder = "ROWS"), "rootTag", true)
                      .addField(
                        SelectBox("Write Mode")
                          .addOption("error", "error")
                          .addOption("overwrite", "overwrite")
                          .addOption("append", "append")
                          .addOption("ignore", "ignore"),
                        "writeMode"
                      )
                      .addField(
                        SchemaColumnsDropdown("Partition Columns")
                          .withMultipleSelection()
                          .bindSchema("schema")
                          .showErrorsFor("partitionColumns"),
                        "partitionColumns"
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
                      .addField(TextBox("Null Value"), "nullValue")
                      .addField(TextBox("Attribute Prefix"), "attributePrefix")
                      .addField(TextBox("Value Tag"), "valueTag")
                      .addField(TextBox("Timestamp Format"), "timestampFormat")
                      .addField(TextBox("Date Format"), "dateFormat")
                      .addField(TextBox("XML Declaration"), "declaration")
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

    if (component.properties.rowTag.isEmpty) {
      diagnostics += Diagnostic("properties.rowTag", "Row tag cannot be empty [Properties]", SeverityLevel.Error)
    }

    // TODO: Is there a better way to determine if we're a Target instead of a Source?
    if (component.ports.inputs.nonEmpty && component.ports.outputs.isEmpty) {
      if (component.properties.rootTag.isEmpty) {
        diagnostics += Diagnostic("properties.rootTag", "Root Tag cannot be empty for Target mode", SeverityLevel.Error)
      }
    }

    diagnostics.toList
  }

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = newState

  class XMLFormatCode(props: XMLProperties) extends ComponentCode {
    def sourceApply(spark: SparkSession): DataFrame = {
      var reader = spark.read
        .format("xml")
        .option("rowTag", props.rowTag)
        .option("rootTag", props.rootTag)
        .option("excludeAttribute", props.excludeAttribute)
        .option("nullValue", props.nullValue)
        .option("mode", props.mode)
        .option("attributePrefix", props.attributePrefix)
        .option("valueTag", props.valueTag)
        .option("ignoreSurroundingSpaces", props.ignoreSurroundingSpaces)
        .option("ignoreNamespace", props.ignoreNamespace)
        .option("timestampFormat", props.timestampFormat)
        .option("dateFormat", props.dateFormat)

      if (props.useSchema.isDefined && props.useSchema.get)
        props.schema.foreach(schema => reader = reader.schema(schema))

      reader.load(props.path)
    }

    def targetApply(spark: SparkSession, in: DataFrame): Unit = {
      var writer = in.write
        .format("xml")

      props.writeMode.foreach { mode =>
        writer = writer.mode(mode)
      }

      writer = writer
        .option("rowTag", props.rowTag)
        .option("rootTag", props.rootTag)
        .option("declaration", props.declaration)
        .option("nullValue", props.nullValue)
        .option("attributePrefix", props.attributePrefix)
        .option("valueTag", props.valueTag)
        .option("timestampFormat", props.timestampFormat)
        .option("compression", props.compression)
        .option("dateFormat", props.dateFormat)

      props.partitionColumns.foreach(pcols =>
        writer = pcols match {
          case nil => writer
          case _   => writer.partitionBy(pcols: _*)
        }
      )

      writer.save(props.path)
    }
  }

  override def deserializeProperty(props: String): PropertiesType = Json.parse(props).as[PropertiesType]

  override def serializeProperty(props: PropertiesType): String = Json.stringify(Json.toJson(props))
}
