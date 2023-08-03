package io.prophecy.scalagems.dataset

import io.prophecy.gems._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.datasetSpec._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.uiSpec._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.{Format, Json}

class parquet extends DatasetSpec {

  val name: String = "parquet"
  val datasetType: String = "File"
  val docUrl: String = "https://docs.prophecy.io/low-code-spark/gems/source-target/file/parquet"

  type PropertiesType = ParquetProperties
  case class ParquetProperties(
    @Property("Schema")
    schema: Option[StructType] = None,
    @Property("Description")
    description: Option[String] = Some(""),
    @Property("useSchema")
    useSchema: Option[Boolean] = Some(false),
    @Property("Path")
    path: String = "",
    @Property(
      "",
      "(default is the value specified in spark.sql.parquet.mergeSchema(false)): sets whether we should merge schemas collected from all Parquet part-files. This will override spark.sql.parquet.mergeSchema."
    )
    mergeSchema: Option[Boolean] = None,
    @Property(
      "datetimeRebaseMode",
      "The datetimeRebaseMode option allows to specify the rebasing mode for the values of the DATE, TIMESTAMP_MILLIS, TIMESTAMP_MICROS logical types from the Julian to Proleptic Gregorian calendar."
    )
    datetimeRebaseMode: Option[String] = None,
    @Property(
      "int96RebaseMode",
      "The int96RebaseMode option allows to specify the rebasing mode for INT96 timestamps from the Julian to Proleptic Gregorian calendar."
    )
    int96RebaseMode: Option[String] = None,
    @Property("compression", "(default: none) compression codec to use when saving to file.")
    compression: Option[String] = None,
    @Property("partitionColumns", "Partitioning column.")
    partitionColumns: Option[List[String]] = None,
    @Property("Write Mode", """(default: "error") Specifies the behavior when data or table already exists.""")
    writeMode: Option[String] = None,
    @Property(
      "",
      "an optional glob pattern to only include files with paths matching the pattern. The syntax follows org.apache.hadoop.fs.GlobFilter. It does not change the behavior of partition discovery."
    )
    pathGlobFilter: Option[String] = None,
    @Property(
      "",
      "(batch only): an optional timestamp to only include files with modification times occurring before the specified Time. The provided timestamp must be in the following form: YYYY-MM-DDTHH:mm:ss (e.g. 2020-06-01T13:00:00)"
    )
    modifiedBefore: Option[String] = None,
    @Property(
      "",
      "(batch only): an optional timestamp to only include files with modification times occurring after the specified Time. The provided timestamp must be in the following form: YYYY-MM-DDTHH:mm:ss (e.g. 2020-06-01T13:00:00)"
    )
    modifiedAfter: Option[String] = None,
    @Property("", "recursively scan a directory for files. Using this option disables partition discovery")
    recursiveFileLookup: Option[Boolean] = None
  ) extends DatasetProperties

  implicit val parquetPropertiesFormat: Format[ParquetProperties] = Json.format

  def sourceDialog: DatasetDialog = DatasetDialog("parquet")
    .addSection("LOCATION", TargetLocation("path"))
    .addSection(
      "PROPERTIES",
      ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
        .addColumn(
          ScrollBox().addElement(
            StackLayout(height = Some("100%"))
              .addElement(
                StackItem(grow = Some(1)).addElement(
                  FieldPicker(height = Some("100%"))
                    .addField(
                      TextArea("Description", 2, placeholder = "Dataset description..."),
                      "description",
                      true
                    )
                    .addField(Checkbox("Use user-defined schema"), "useSchema", true)
                    .addField(Checkbox("Merge schema"), "mergeSchema")
                    .addField(
                      SelectBox("Datetime Rebase Mode")
                        .addOption("EXCEPTION", "EXCEPTION")
                        .addOption("CORRECTED", "CORRECTED")
                        .addOption("LEGACY", "LEGACY"),
                      "datetimeRebaseMode"
                    )
                    .addField(
                      SelectBox("Int96 Rebase Mode")
                        .addOption("EXCEPTION", "EXCEPTION")
                        .addOption("CORRECTED", "CORRECTED")
                        .addOption("LEGACY", "LEGACY"),
                      "int96RebaseMode"
                    )
                    .addField(Checkbox("Recursive File Lookup"), "recursiveFileLookup")
                    .addField(TextBox("Path Global Filter").bindPlaceholder(""), "pathGlobFilter")
                    .addField(TextBox("Modified Before").bindPlaceholder(""), "modifiedBefore")
                    .addField(TextBox("Modified After").bindPlaceholder(""), "modifiedAfter")
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

  def targetDialog: DatasetDialog = DatasetDialog("parquet")
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
                      .addOption("uncompressed", "uncompressed")
                      .addOption("gzip", "gzip")
                      .addOption("lz4", "lz4")
                      .addOption("snappy", "snappy")
                      .addOption("lzo", "lzo")
                      .addOption("brotli", "brotli")
                      .addOption("zstd", "zstd"),
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

    if (component.properties.path.isEmpty) {
      diagnostics += Diagnostic("properties.path", "path variable cannot be empty [Location]", SeverityLevel.Error)
    }
    if (component.properties.schema.isEmpty) {
      // diagnostics += Diagnostic("properties.schema", "Schema cannot be empty [Properties]", SeverityLevel.Error)
    }

    diagnostics.toList
  }

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = newState

  class ParquetFormatCode(props: ParquetProperties) extends ComponentCode {

    def sourceApply(spark: SparkSession): DataFrame = {
      var reader = spark.read
        .format("parquet")
        .option("mergeSchema", props.mergeSchema)
        .option("datetimeRebaseMode", props.datetimeRebaseMode)
        .option("int96RebaseMode", props.int96RebaseMode)
        .option("modifiedBefore", props.modifiedBefore)
        .option("modifiedAfter", props.modifiedAfter)
        .option("recursiveFileLookup", props.recursiveFileLookup)
        .option("pathGlobFilter", props.pathGlobFilter)

      if (props.useSchema.isDefined && props.useSchema.get)
        props.schema.foreach(schema ⇒ reader = reader.schema(schema))

      reader.load(props.path)
    }

    def targetApply(spark: SparkSession, in: DataFrame): Unit = {
      var writer = in.write
        .format("parquet")
        .option("compression", props.compression)

      props.writeMode.foreach { mode ⇒
        writer = writer.mode(mode)
      }
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
