package io.prophecy.scalagems.dataset

import io.prophecy.gems._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.datasetSpec._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.uiSpec._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.{Format, Json}

class orc extends DatasetSpec {

  val name: String = "orc"
  val datasetType: String = "File"
  val docUrl: String = "https://docs.prophecy.io/low-code-spark/gems/source-target/file/orc"

  type PropertiesType = OrcProperties
  case class OrcProperties(
                            @Property("Schema")
                            schema: Option[StructType] = None,
                            @Property("Description")
                            description: Option[String] = Some(""),
                            @Property("useSchema")
                            useSchema: Option[Boolean] = Some(false),
                            @Property("Path")
                            path: String = "",
                            @Property("Write Mode", """(default: "error") Specifies the behavior when data or table already exists.""")
                            writeMode: Option[String] = None,
                            @Property(
                              "compression",
                              "(default: snappy) compression codec to use when saving to file. This can be one of the known case-insensitive shorten names(none, snappy, zlib, and lzo). This will override orc.compress."
                            )
                            compression: Option[String] = None,
                            @Property("partitionColumns", "Partitioning column.")
                            partitionColumns: Option[List[String]] = None,
                            @Property("", "recursively scan a directory for files. Using this option disables partition discovery")
                            recursiveFileLookup: Option[Boolean] = None
                          ) extends DatasetProperties

  implicit val orcPropertiesFormat: Format[OrcProperties] = Json.format

  def sourceDialog: DatasetDialog = DatasetDialog("orc")
    .addSection("LOCATION", TargetLocation("path"))
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
                      .addField(Checkbox("Use user-defined schema"), "useSchema", true)
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

  def targetDialog: DatasetDialog = DatasetDialog("orc")
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
                    SchemaColumnsDropdown("Partition Columns")
                      .withMultipleSelection()
                      .bindSchema("schema")
                      .showErrorsFor("partitionColumns"),
                    "partitionColumns"
                  )
                  .addField(
                    SelectBox("Compression Codec")
                      .addOption("none", "none")
                      .addOption("snappy", "snappy")
                      .addOption("zlib", "zlib")
                      .addOption("lzo", "lzo"),
                    "compression"
                  )
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

    println("orc validate component: ", component)

    if (component.properties.path.isEmpty) {
      diagnostics += Diagnostic("properties.path", "path variable cannot be empty [Location]", SeverityLevel.Error)
    }
    if (component.properties.schema.isEmpty) {
      // diagnostics += Diagnostic("properties.schema", "Schema cannot be empty [Properties]", SeverityLevel.Error)
    }

    diagnostics.toList
  }

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = newState

  class OrcFormatCode(props: OrcProperties) extends ComponentCode {

    def sourceApply(spark: SparkSession): DataFrame = {
      var reader = spark.read
        .format("orc")
        .option("recursiveFileLookup", props.recursiveFileLookup)

      if (props.useSchema.isDefined && props.useSchema.get)
        props.schema.foreach(schema ⇒ reader = reader.schema(schema))

      reader.load(props.path)

    }

    def targetApply(spark: SparkSession, in: DataFrame): Unit = {
      var writer = in.write
        .format("orc")

      props.writeMode.foreach { mode ⇒
        writer = writer.mode(mode)
      }

      writer = writer.option("compression", props.compression)

      props.partitionColumns.foreach(pcols ⇒
        writer = pcols match {
          case Nil ⇒ writer
          case _ ⇒ writer.partitionBy(pcols: _*)
        }
      )
      writer.save(props.path)
    }

  }

  override def deserializeProperty(props: String): PropertiesType = Json.parse(props).as[PropertiesType]

  override def serializeProperty(props: PropertiesType): String = Json.stringify(Json.toJson(props))
}
