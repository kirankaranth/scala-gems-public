package io.prophecy.scalagems.dataset

import io.prophecy.gems._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.datasetSpec._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.uiSpec._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.{Format, Json}

class avro extends DatasetSpec {

  val name: String = "avro"
  val datasetType: String = "File"
  val docUrl: String = "https://docs.prophecy.io/low-code-spark/gems/source-target/file/avro"

  type PropertiesType = AvroProperties
  case class AvroProperties(
    @Property("Schema")
    schema: Option[StructType] = None,
    @Property("Description")
    description: Option[String] = Some(""),
    @Property("useSchema")
    useSchema: Option[Boolean] = Some(false),
    @Property("Path")
    path: String = "",
    @Property("ignoreExtension", "default: false, Ignore files without .avro extension")
    ignoreExtension: Option[Boolean] = None,
    @Property("avroschema", "Optional schema provided by a user in JSON format. ")
    avroschema: Option[String] = None,
    @Property("Write Mode", """default: "error", Specifies the behavior when data or table already exists.""")
    writeMode: Option[String] = None,
    @Property("compression", """default: "none", compression codec to use when saving to file.""")
    compression: Option[String] = None,
    @Property("partitionColumns", "Partitioning column.")
    partitionColumns: Option[List[String]] = None,
    @Property("recordName", "Top level record name in write result, which is required in Avro spec.")
    recordName: Option[String] = None,
    @Property("recordNamespace", "Record namespace in write result.")
    recordNamespace: Option[String] = None,
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

  implicit val avroPropertiesFormat: Format[AvroProperties] = Json.format
  def sourceDialog: DatasetDialog = DatasetDialog("avro")
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
                    .addField(Checkbox("Ignore files without .avro extension while reading"), "ignoreExtension")
                    .addField(Checkbox("Recursive File Lookup"), "recursiveFileLookup")
                    .addField(TextBox("Path Global Filter").bindPlaceholder(""), "pathGlobFilter")
                    .addField(TextBox("Modified Before").bindPlaceholder(""), "modifiedBefore")
                    .addField(TextBox("Modified After").bindPlaceholder(""), "modifiedAfter")
                    .addField(TextBox("Avro Schema").bindPlaceholder("json schema"), "avroschema")
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

  def targetDialog: DatasetDialog = DatasetDialog("avro")
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
                    SelectBox("Compression")
                      .addOption("uncompressed", "uncompressed")
                      .addOption("snappy", "snappy")
                      .addOption("deflate", "deflate")
                      .addOption("bzip2", "bzip2")
                      .addOption("xz", "xz"),
                    "compression"
                  )
                  .addField(
                    SchemaColumnsDropdown("Partition Columns")
                      .withMultipleSelection()
                      .bindSchema("schema")
                      .showErrorsFor("partitionColumns"),
                    "partitionColumns"
                  )
                  .addField(TextBox("Record Name").bindPlaceholder(""), "recordName")
                  .addField(TextBox("Record Namespace").bindPlaceholder(""), "recordNamespace")
                  .addField(TextBox("Avro Schema").bindPlaceholder("json schema"), "avroschema")
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

    println("avro validate component: ", component)

    if (component.properties.path.isEmpty) {
      diagnostics += Diagnostic("properties.path", "path variable cannot be empty [Location]", SeverityLevel.Error)
    }
    if (component.properties.schema.isEmpty) {
      // diagnostics += Diagnostic("properties.schema", "Schema cannot be empty [Properties]", SeverityLevel.Error)
    }
    diagnostics.toList
  }

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = newState

  class AvroFormatCode(props: AvroProperties) extends ComponentCode {

    def sourceApply(spark: SparkSession): DataFrame = {
      import org.apache.avro.Schema
      var reader = spark.read
        .format("avro")
        .option("ignoreExtension", props.ignoreExtension)
        .option("modifiedBefore", props.modifiedBefore)
        .option("modifiedAfter", props.modifiedAfter)
        .option("recursiveFileLookup", props.recursiveFileLookup)
        .option("pathGlobFilter", props.pathGlobFilter)

      reader = props.avroschema match {
        case None ⇒ reader
        case Some(value) ⇒
          val avroSchema = new Schema.Parser().parse(value.stripMargin).toString
          reader.option("avroSchema", avroSchema)
      }

      if (props.useSchema.isDefined && props.useSchema.get)
        props.schema.foreach(schema ⇒ reader = reader.schema(schema))

      reader.load(props.path)

    }

    def targetApply(spark: SparkSession, in: DataFrame): Unit = {
      import org.apache.avro.Schema
      var writer = in.write
        .format("avro")
        .option("recordNamespace", props.recordNamespace)
        .option("compression", props.compression)
        .option("recordName", props.recordName)

      writer = props.avroschema match {
        case None ⇒ writer
        case Some(value) ⇒
          val avroSchema = new Schema.Parser().parse(value.stripMargin).toString
          writer.option("avroSchema", avroSchema)
      }

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
