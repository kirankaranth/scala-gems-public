package io.prophecy.scalagems.dataset

import io.prophecy.gems._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.datasetSpec._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.uiSpec._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json._

class json extends DatasetSpec {

  val name: String = "json"
  val datasetType: String = "File"
  val docUrl: String = "https://docs.prophecy.io/low-code-spark/gems/source-target/file/json"

  type PropertiesType = JsonProperties
  case class JsonProperties(
    @Property("Schema")
    schema: Option[StructType] = None,
    @Property("Description")
    description: Option[String] = Some(""),
    @Property("useSchema")
    useSchema: Option[Boolean] = Some(true),
    @Property("Path")
    path: String = "",
    @Property("primitivesAsString", "(default: false) infers all primitive values as a string type")
    primitivesAsString: Option[Boolean] = None,
    @Property(
      "prefersDecimal",
      "(default: false) infers all floating-point values as a decimal type. If the values do not fit in decimal, then it infers them as doubles."
    )
    prefersDecimal: Option[Boolean] = None,
    @Property("allowComments", "(default: false) ignores Java/C++ style comment in JSON records")
    allowComments: Option[Boolean] = None,
    @Property("allowUnquotedFieldNames", "(default: false) allows unquoted JSON field names")
    allowUnquotedFieldNames: Option[Boolean] = None,
    @Property("allowSingleQuotes", "(default: true) allows single quotes in addition to double quotes")
    allowSingleQuotes: Option[Boolean] = None,
    @Property("allowNumericLeadingZeros", "(default: false) allows leading zeros in numbers (e.g. 00012)")
    allowNumericLeadingZeros: Option[Boolean] = None,
    @Property(
      "allowBackslashEscapingAnyCharacter",
      "(default: false) allows accepting quoting of all character using backslash quoting mechanism"
    )
    allowBackslashEscapingAnyCharacter: Option[Boolean] = None,
    @Property(
      "allowUnquotedControlChars",
      "(default: false) allows JSON Strings to contain unquoted control characters (ASCII characters with value less than 32, including tab and line feed characters) or not."
    )
    allowUnquotedControlChars: Option[Boolean] = None,
    @Property("mode", """(default: "PERMISSIVE") allows a mode for dealing with corrupt records during parsing.""")
    mode: Option[String] = None,
    @Property(
      "columnNameOfCorruptRecord",
      """(default: "NA") allows renaming the new field having malformed string created by PERMISSIVE mode. This overrides spark.sql.columnNameOfCorruptRecord"""
    )
    columnNameOfCorruptRecord: Option[String] = None,
    @Property(
      "dateFormat",
      "(default: yyyy-MM-dd) sets the string that indicates a date format. Custom date formats follow the formats at java.text.SimpleDateFormat. This applies to date type"
    )
    dateFormat: Option[String] = None,
    @Property(
      "timestampFormat",
      "(default: yyyy-MM-dd'T'HH:mm:ss.SSSXXX) sets the string that indicates a timestamp format. Custom date formats follow the formats at java.text.SimpleDateFormat."
    )
    timestampFormat: Option[String] = None,
    @Property("multiLine", "(default: false) parse one record, which may span multiple lines, per file")
    multiLine: Option[Boolean] = None,
    @Property("lineSep", """(default: "\\n") defines the line separator that should be used for parsing.""")
    lineSep: Option[String] = None,
    @Property("samplingRatio", """(default: "1.0") defines fraction of input JSON objects used for schema inferring""")
    samplingRatio: Option[String] = None,
    @Property(
      "dropFieldIfAllNull",
      "(default: false) whether to ignore column of all null values or empty array/struct during schema inference"
    )
    dropFieldIfAllNull: Option[Boolean] = None,
    @Property("Write Mode", """(default: "error") Specifies the behavior when data or table already exists.""")
    writeMode: Option[String] = None,
    @Property("compression", "compression codec to use when saving to file.")
    compression: Option[String] = None,
    @Property("partitionColumns", """(default: "") Partitioning column.""")
    partitionColumns: Option[List[String]] = None,
    @Property(
      "encoding",
      "specifies encoding (charset) of saved json files. If it is not set, the UTF-8 charset will be used."
    )
    encoding: Option[String] = None,
    @Property("ignoreNullFields", "(default: true) Whether to ignore null fields when generating JSON objects.")
    ignoreNullFields: Option[Boolean] = None,
    @Property("", "recursively scan a directory for files. Using this option disables partition discovery")
    recursiveFileLookup: Option[Boolean] = None
  ) extends DatasetProperties

  implicit val jsonPropertiesFormat: Format[JsonProperties] = new Format[JsonProperties] {
    override def reads(json: JsValue): JsResult[JsonProperties] = {
      for {
        schema <- (json \ "schema").validateOpt[StructType]
        description <- (json \ "description").validateOpt[String]
        useSchema <- (json \ "useSchema") match {
          case x @ JsDefined(_) => x.validateOpt[Boolean]
          case _: JsUndefined   => JsSuccess(Some(true))
        }
        path <- (json \ "path").validate[String]
        primitivesAsString <- (json \ "primitivesAsString").validateOpt[Boolean]
        prefersDecimal <- (json \ "prefersDecimal").validateOpt[Boolean]
        allowComments <- (json \ "allowComments").validateOpt[Boolean]
        allowUnquotedFieldNames <- (json \ "allowUnquotedFieldNames").validateOpt[Boolean]
        allowSingleQuotes <- (json \ "allowSingleQuotes").validateOpt[Boolean]
        allowNumericLeadingZeros <- (json \ "allowNumericLeadingZeros").validateOpt[Boolean]
        allowBackslashEscapingAnyCharacter <- (json \ "allowBackslashEscapingAnyCharacter").validateOpt[Boolean]
        allowUnquotedControlChars <- (json \ "allowUnquotedControlChars").validateOpt[Boolean]
        mode <- (json \ "mode").validateOpt[String]
        columnNameOfCorruptRecord <- (json \ "columnNameOfCorruptRecord").validateOpt[String]
        dateFormat <- (json \ "dateFormat").validateOpt[String]
        timestampFormat <- (json \ "timestampFormat").validateOpt[String]
        multiLine <- (json \ "multiLine").validateOpt[Boolean]
        lineSep <- (json \ "lineSep").validateOpt[String]
        samplingRatio <- (json \ "samplingRatio").validateOpt[String]
        dropFieldIfAllNull <- (json \ "dropFieldIfAllNull").validateOpt[Boolean]
        writeMode <- (json \ "writeMode").validateOpt[String]
        compression <- (json \ "compression").validateOpt[String]
        partitionColumns <- (json \ "partitionColumns").validateOpt[List[String]]
        encoding <- (json \ "encoding").validateOpt[String]
        ignoreNullFields <- (json \ "ignoreNullFields").validateOpt[Boolean]
        recursiveFileLookup <- (json \ "recursiveFileLookup").validateOpt[Boolean]
      } yield {
        JsonProperties(
          schema,
          description,
          useSchema,
          path,
          primitivesAsString,
          prefersDecimal,
          allowComments,
          allowUnquotedFieldNames,
          allowSingleQuotes,
          allowNumericLeadingZeros,
          allowBackslashEscapingAnyCharacter,
          allowUnquotedControlChars,
          mode,
          columnNameOfCorruptRecord,
          dateFormat,
          timestampFormat,
          multiLine,
          lineSep,
          samplingRatio,
          dropFieldIfAllNull,
          writeMode,
          compression,
          partitionColumns,
          encoding,
          ignoreNullFields,
          recursiveFileLookup
        )
      }
    }

    override def writes(o: JsonProperties): JsValue = {
      Json.obj(
        "schema" -> o.schema,
        "description" -> o.description,
        "useSchema" -> o.useSchema,
        "path" -> o.path,
        "primitivesAsString" -> o.primitivesAsString,
        "prefersDecimal" -> o.prefersDecimal,
        "allowComments" -> o.allowComments,
        "allowUnquotedFieldNames" -> o.allowUnquotedFieldNames,
        "allowSingleQuotes" -> o.allowSingleQuotes,
        "allowNumericLeadingZeros" -> o.allowNumericLeadingZeros,
        "allowBackslashEscapingAnyCharacter" -> o.allowBackslashEscapingAnyCharacter,
        "allowUnquotedControlChars" -> o.allowUnquotedControlChars,
        "mode" -> o.mode,
        "columnNameOfCorruptRecord" -> o.columnNameOfCorruptRecord,
        "dateFormat" -> o.dateFormat,
        "timestampFormat" -> o.timestampFormat,
        "multiLine" -> o.multiLine,
        "lineSep" -> o.lineSep,
        "samplingRatio" -> o.samplingRatio,
        "dropFieldIfAllNull" -> o.dropFieldIfAllNull,
        "writeMode" -> o.writeMode,
        "compression" -> o.compression,
        "partitionColumns" -> o.partitionColumns,
        "encoding" -> o.encoding,
        "ignoreNullFields" -> o.ignoreNullFields,
        "recursiveFileLookup" -> o.recursiveFileLookup
      )
    }
  }

  def sourceDialog: DatasetDialog = DatasetDialog("json")
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
                    .addField(Checkbox("Parse Multi-line records"), "multiLine")
                    .addField(TextBox("New line separator").bindPlaceholder(""), "lineSep")
                    .addField(Checkbox("Infer primitive values as string type"), "primitivesAsString")
                    .addField(Checkbox("Infer floating-point values as decimal or double type"), "prefersDecimal")
                    .addField(Checkbox("Ignore Java/C++ style comment in Json records"), "allowComments")
                    .addField(Checkbox("Allow unquoted field names"), "allowUnquotedFieldNames")
                    .addField(Checkbox("Allow single quotes"), "allowSingleQuotes")
                    .addField(Checkbox("Allow leading zero in numbers"), "allowNumericLeadingZeros")
                    .addField(Checkbox("Allow Backslash escaping"), "allowBackslashEscapingAnyCharacter")
                    .addField(Checkbox("Allow unquoted control characters in JSON string"), "allowUnquotedControlChars")
                    .addField(
                      SelectBox("Mode to deal with corrupt records")
                        .addOption("PERMISSIVE", "PERMISSIVE")
                        .addOption("DROPMALFORMED", "DROPMALFORMED")
                        .addOption("FAILFAST", "FAILFAST"),
                      "mode"
                    )
                    .addField(
                      TextBox("Column name of a corrupt record")
                        .bindPlaceholder(""),
                      "columnNameOfCorruptRecord"
                    )
                    .addField(TextBox("Date Format String").bindPlaceholder(""), "dateFormat")
                    .addField(TextBox("Timestamp Format String").bindPlaceholder(""), "timestampFormat")
                    .addField(
                      TextBox("Sampling ratio for schema inferring")
                        .bindPlaceholder(""),
                      "samplingRatio"
                    )
                    .addField(
                      Checkbox("Ignore column with all null values during schema inferring"),
                      "dropFieldIfAllNull"
                    )
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

  def targetDialog: DatasetDialog = DatasetDialog("json")
    .addSection("LOCATION", TargetLocation("path"))
    .addSection(
      "PROPERTIES",
      ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
        .addColumn(
          ScrollBox().addElement(
            StackLayout(height = Some("100%")).addElement(
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
                    .addOption("bzip2", "bzip2")
                    .addOption("gzip", "gzip")
                    .addOption("lz4", "lz4")
                    .addOption("snappy", "snappy")
                    .addOption("deflate", "deflate"),
                  "compression"
                )
                .addField(TextBox("Date Format String").bindPlaceholder(""), "dateFormat")
                .addField(TextBox("Timestamp Format String").bindPlaceholder(""), "timestampFormat")
                .addField(TextBox("Encoding").bindPlaceholder(""), "encoding")
                .addField(TextBox("Line Separator").bindPlaceholder(""), "lineSep")
                .addField(Checkbox("Ignore null fields"), "ignoreNullFields")
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

    println("json validate component: ", component)

    if (component.properties.path.isEmpty) {
      diagnostics += Diagnostic("properties.path", "path variable cannot be empty [Location]", SeverityLevel.Error)
    }
    if (component.properties.schema.isEmpty) {
      // diagnostics += Diagnostic("properties.schema", "Schema cannot be empty [Properties]", SeverityLevel.Error)
    }

    def getDoubleOption(s: String): Option[Double] = s.trim.isEmpty match {
      case true ⇒ None
      case false ⇒
        try Some(s.trim.toDouble)
        catch { case _ ⇒ None }

    }

    val invalidSamplingRatio = component.properties.samplingRatio match {
      case None ⇒ None
      case Some(value: String) ⇒
        if (value.isEmpty)
          None
        else
          getDoubleOption(value) match {
            case None ⇒
              Some(Diagnostic("properties.samplingRatio", "Invalid sampling ratio [Properties]", SeverityLevel.Error))
            case Some(legitValue) ⇒
              (0.0 < legitValue) && (legitValue <= 1.0) match {
                case true ⇒ None
                case false ⇒
                  Some(
                    Diagnostic(
                      "properties.samplingRatio",
                      "Sampling Ratio has to be between (0.0, 1.0] [Properties]",
                      SeverityLevel.Error
                    )
                  )
              }
          }

    }
    if (invalidSamplingRatio.isDefined)
      diagnostics += invalidSamplingRatio.get

    diagnostics.toList
  }

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = newState

  override def deserializeProperty(props: String): PropertiesType = Json.parse(props).as[PropertiesType]

  override def serializeProperty(props: PropertiesType): String = Json.stringify(Json.toJson(props))

  class JsonFormatCode(props: JsonProperties) extends ComponentCode {

    def sourceApply(spark: SparkSession): DataFrame = {
      var reader = spark.read
        .format("json")
        .option("multiLine", props.multiLine)
        .option("lineSep", props.lineSep)
        .option("primitivesAsString", props.primitivesAsString)
        .option("prefersDecimal", props.prefersDecimal)
        .option("allowComments", props.allowComments)
        .option("allowUnquotedFieldNames", props.allowUnquotedFieldNames)
        .option("allowSingleQuotes", props.allowSingleQuotes)
        .option("allowNumericLeadingZeros", props.allowNumericLeadingZeros)
        .option("allowBackslashEscapingAnyCharacter", props.allowBackslashEscapingAnyCharacter)
        .option("allowUnquotedControlChars", props.allowUnquotedControlChars)
        .option("mode", props.mode)
        .option("columnNameOfCorruptRecord", props.columnNameOfCorruptRecord)
        .option("dateFormat", props.dateFormat)
        .option("timestampFormat", props.timestampFormat)
        .option("samplingRatio", props.samplingRatio)
        .option("dropFieldIfAllNull", props.dropFieldIfAllNull)
        .option("recursiveFileLookup", props.recursiveFileLookup)

      if (props.useSchema.isDefined && props.useSchema.get)
        props.schema.foreach(schema ⇒ reader = reader.schema(schema))

      reader.load(props.path)

    }

    def targetApply(spark: SparkSession, in: DataFrame): Unit = {
      var writer = in.write
        .format("json")

      props.writeMode.foreach { mode ⇒
        writer = writer.mode(mode)
      }

      writer = writer
        .option("compression", props.compression)
        .option("dateFormat", props.dateFormat)
        .option("timestampFormat", props.timestampFormat)
        .option("encoding", props.encoding)
        .option("lineSep", props.lineSep)
        .option("ignoreNullFields", props.ignoreNullFields)

      props.partitionColumns.foreach(pcols ⇒
        writer = pcols match {
          case Nil ⇒ writer
          case _ ⇒ writer.partitionBy(pcols: _*)
        }
      )
      writer.save(props.path)
    }

  }

}
