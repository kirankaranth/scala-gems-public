package io.prophecy.scalagems.dataset

import io.prophecy.gems._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.datasetSpec._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.uiSpec._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.functional.syntax._
import play.api.libs.json.{Format, JsResult, JsValue, Json}
import ai.x.play.json.Encoders.encoder
import ai.x.play.json.Jsonx

class csv extends DatasetSpec {

  val name: String = "csv"
  val datasetType: String = "File"
  val docUrl: String = "https://docs.prophecy.io/low-code-spark/gems/source-target/file/csv"

  type PropertiesType = CsvProperties
  case class CsvProperties(
    @Property("Schema")
    schema: Option[StructType] = None,
    @Property("Description")
    description: Option[String] = Some(""),
    @Property("useSchema")
    useSchema: Option[Boolean] = Some(true),
    // common properties
    @Property("Path")
    path: String = "",
    // COMMON PROPS
    @Property("Separator", "The column delimiter. By default ',' but can be set to any character")
    separator: Option[SString] = Some(SString(",")),
    @Property("encoding", """(default: "UTF-8"). Encoding type used to decode the given CSV file""")
    encoding: Option[String] = None,
    @Property(
      "quote",
      """(default: "\") Sets a single character used for escaping quoted values where the separator can be part of the value. If you would like to turn off quotations, you need to set not null but an empty string. This behaviour is different from com.databricks.spark.csv."""
    )
    quote: Option[String] = None,
    @Property(
      "escape",
      """(default: "\\") Sets the single character used for escaping quoted values where the separator can be part of the value. If you would like to turn off quotations, you need to set not null but an empty string."""
    )
    escape: Option[String] = None,
    @Property(
      "charToEscapeQuoteEscaping",
      """(default: "\"", escape or \0) Sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters are different, \\0 otherwise."""
    )
    charToEscapeQuoteEscaping: Option[String] = None,
    @Property("header", "(default: false): uses the first line as names of columns.")
    header: Option[Boolean] = Some(true),
    @Property(
      "ignoreLeadingWhiteSpaceReading",
      "(default: false): a flag indicating whether or not leading whitespaces from values being read should be skipped."
    )
    ignoreLeadingWhiteSpaceReading: Option[Boolean] = None,
    @Property(
      "ignoreTrailingWhiteSpaceReading",
      "(default: false): a flag indicating whether or not trailing whitespaces from values being read should be skipped."
    )
    ignoreTrailingWhiteSpaceReading: Option[Boolean] = None,
    @Property(
      "ignoreLeadingWhiteSpaceWriting",
      "(default: false): a flag indicating whether or not leading whitespaces from values being read should be skipped."
    )
    ignoreLeadingWhiteSpaceWriting: Option[Boolean] = None,
    @Property(
      "ignoreTrailingWhiteSpaceWriting",
      "(default: false): a flag indicating whether or not trailing whitespaces from values being read should be skipped."
    )
    ignoreTrailingWhiteSpaceWriting: Option[Boolean] = None,
    @Property(
      "nullValue",
      """(default: "") sets the string representation of a null value. Since 2.0.1, this applies to all supported types including the string type."""
    )
    nullValue: Option[String] = None,
    @Property("emptyValue", """(default: "") sets the string representation of an empty value.""")
    emptyValue: Option[String] = None,
    @Property(
      "dateFormat",
      """(default: "yyyy-MM-dd") sets the string that indicates a date format. Custom date formats follow the formats at Datetime Patterns. This applies to date type."""
    ) dateFormat: Option[String] = None,
    @Property(
      "timestampFormat",
      """(default: "yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]") sets the string that indicates a timestamp format. Custom date formats follow the formats at Datetime Patterns. This applies to timestamp type."""
    )
    timestampFormat: Option[String] = None,
    // SOURCE_ONLY PROPS
    @Property(
      "comment",
      """(default: "") sets a single character used for skipping lines beginning with this character. By default, it is disabled."""
    )
    comment: Option[String] = None,
    @Property(
      "enforceSchema",
      "(default: true) If it is set to false, the specified or inferred schema will be forcibly applied to datasource files, and headers in CSV files will be ignored. If the option is set to false, the schema will be validated against all headers in CSV files in the case when the header option is set to true. Field names in the schema and column names in CSV headers are checked by their positions taking into account spark.sql.caseSensitive. Though the default value is true, it is recommended to disable the enforceSchema option to avoid incorrect results"
    )
    enforceSchema: Option[Boolean] = None,
    @Property(
      "inferSchema",
      "infers the input schema automatically from data. It requires one extra pass over the data."
    )
    inferSchema: Option[Boolean] = None,
    @Property("samplingRatio", "(default: 1.0) defines fraction of rows used for schema inferring.")
    samplingRatio: Option[SDouble] = None,
    @Property("nanValue", """(default: "NaN") sets the string representation of a non-number value.""")
    nanValue: Option[String] = None,
    @Property("positiveInf", """(default: "Inf") sets the string representation of a positive infinity value.""")
    positiveInf: Option[String] = None,
    @Property("negativeInf", """(default: "-Inf") sets the string representation of a negative infinity value.""")
    negativeInf: Option[String] = None,
    @Property("maxColumns", "(default: 20480) defines a hard limit of how many columns a record can have.")
    maxColumns: Option[String] = None,
    @Property(
      "maxCharsPerColumn",
      """(default: -1) defines the maximum number of characters allowed for any given value being read. By default, it is -1 meaning unlimited length"""
    )
    maxCharsPerColumn: Option[String] = None,
    @Property("unescapedQuoteHandling", "defines how the CsvParser will handle values with unescaped quotes.")
    unescapedQuoteHandling: Option[String] = None,
    @Property(
      "mode",
      """(default: "PERMISSIVE") allows a mode for dealing with corrupt records during parsing. It supports the following case-insensitive modes. Note that Spark tries to parse only required columns in CSV under column pruning. Therefore, corrupt records can be different based on required set of fields. This behavior can be controlled by spark.sql.csv.parser.columnPruning.enabled (enabled by default)."""
    )
    mode: Option[String] = None,
    @Property(
      "columnNameOfCorruptRecord",
      """(default: "") allows renaming the new field having malformed string created by PERMISSIVE mode. This overrides spark.sql.columnNameOfCorruptRecord."""
    )
    columnNameOfCorruptRecord: Option[String] = None,
    @Property("multiLine", "(default: false) parse one record, which may span multiple lines.")
    multiLine: Option[Boolean] = None,
    @Property(
      "escapeQuotes",
      "(default: true) a flag indicating whether values containing quotes should always be enclosed in quotes. Default is to escape all values containing a quote character."
    )
    escapeQuotes: Option[Boolean] = None,
    @Property(
      "quoteAll",
      "(default: false) a flag indicating whether all values should always be enclosed in quotes. Default is to only escape values containing a quote character."
    )
    quoteAll: Option[Boolean] = None,
    @Property("compression", """(default: "none") compression codec to use when saving to file.""")
    compression: Option[String] = None,
    @Property("partitionColumns", "Partitioning column.")
    partitionColumns: Option[List[String]] = None,
    @Property("Write Mode", """(default: "error") Specifies the behavior when data or table already exists.""")
    writeMode: Option[String] = Some("error"),
    @Property(
      "locale",
      "sets a locale as language tag in IETF BCP 47 format. For instance, this is used while parsing dates and timestamps."
    )
    locale: Option[String] = None,
    @Property(
      "lineSep",
      "Default covers all \r, \r\n and \n. defines the line separator that should be used for parsing. Maximum length is 1 character."
    )
    lineSep: Option[String] = None,
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
    recursiveFileLookup: Option[Boolean] = None,
    @Property("", "create a single named csv file as output instead of part files")
    createSingleOutputFile: Option[Boolean] = None,
    @Property("", "Skips n lines from top of the file. Currently supports only single files.")
    skipHeaders: Option[String] = None,
    @Property("", "Skips n lines from bottom of the file. Currently supports only single files.")
    skipFooters: Option[String] = None
  ) extends DatasetProperties

//  implicit val csvPropertiesFormat: Format[CsvProperties] = new Format[CsvProperties] {
//    override def reads(json: JsValue): JsResult[CsvProperties] = {
//      for {
//        schema <- (json \ "schema").validateOpt[StructType]
//        description <- (json \ "description").validateOpt[String]
//        useSchema <- (json \ "useSchema").validateOpt[Boolean]
//        path <- (json \ "path").validate[String]
//        separator <- (json \ "separator").validateOpt[SString]
//        encoding <- (json \ "encoding").validateOpt[String]
//        quote <- (json \ "quote").validateOpt[String]
//        escape <- (json \ "escape").validateOpt[String]
//        charToEscapeQuoteEscaping <- (json \ "charToEscapeQuoteEscaping").validateOpt[String]
//        header <- (json \ "header").validateOpt[Boolean]
//        ignoreLeadingWhiteSpaceReading <- (json \ "ignoreLeadingWhiteSpaceReading").validateOpt[Boolean]
//        ignoreTrailingWhiteSpaceReading <- (json \ "ignoreTrailingWhiteSpaceReading").validateOpt[Boolean]
//        ignoreLeadingWhiteSpaceWriting <- (json \ "ignoreLeadingWhiteSpaceWriting").validateOpt[Boolean]
//        ignoreTrailingWhiteSpaceWriting <- (json \ "ignoreTrailingWhiteSpaceWriting").validateOpt[Boolean]
//        nullValue <- (json \ "nullValue").validateOpt[String]
//        emptyValue <- (json \ "emptyValue").validateOpt[String]
//        dateFormat <- (json \ "dateFormat").validateOpt[String]
//        timestampFormat <- (json \ "timestampFormat").validateOpt[String]
//        comment <- (json \ "comment").validateOpt[String]
//        enforceSchema <- (json \ "enforceSchema").validateOpt[Boolean]
//        inferSchema <- (json \ "inferSchema").validateOpt[Boolean]
//        samplingRatio <- (json \ "samplingRatio").validateOpt[SDouble]
//        nanValue <- (json \ "nanValue").validateOpt[String]
//        positiveInf <- (json \ "positiveInf").validateOpt[String]
//        negativeInf <- (json \ "negativeInf").validateOpt[String]
//        maxColumns <- (json \ "maxColumns").validateOpt[String]
//        maxCharsPerColumn <- (json \ "maxCharsPerColumn").validateOpt[String]
//        unescapedQuoteHandling <- (json \ "unescapedQuoteHandling").validateOpt[String]
//        mode <- (json \ "mode").validateOpt[String]
//        columnNameOfCorruptRecord <- (json \ "columnNameOfCorruptRecord").validateOpt[String]
//        multiLine <- (json \ "multiLine").validateOpt[Boolean]
//        escapeQuotes <- (json \ "escapeQuotes").validateOpt[Boolean]
//        quoteAll <- (json \ "quoteAll").validateOpt[Boolean]
//        compression <- (json \ "compression").validateOpt[String]
//        partitionColumns <- (json \ "partitionColumns").validateOpt[List[String]]
//        writeMode <- (json \ "writeMode").validateOpt[String]
//        locale <- (json \ "locale").validateOpt[String]
//        lineSep <- (json \ "lineSep").validateOpt[String]
//        pathGlobFilter <- (json \ "pathGlobFilter").validateOpt[String]
//        modifiedBefore <- (json \ "modifiedBefore").validateOpt[String]
//        modifiedAfter <- (json \ "modifiedAfter").validateOpt[String]
//        recursiveFileLookup <- (json \ "recursiveFileLookup").validateOpt[Boolean]
//        createSingleOutputFile <- (json \ "createSingleOutputFile").validateOpt[Boolean]
//        skipHeaders <- (json \ "skipHeaders").validateOpt[String]
//        skipFooters <- (json \ "skipFooters").validateOpt[String]
//      } yield CsvProperties(
//        schema,
//        description,
//        useSchema.orElse(Some(false)),
//        path,
//        separator,
//        encoding,
//        quote,
//        escape,
//        charToEscapeQuoteEscaping,
//        header,
//        ignoreLeadingWhiteSpaceReading,
//        ignoreTrailingWhiteSpaceReading,
//        ignoreLeadingWhiteSpaceWriting,
//        ignoreTrailingWhiteSpaceWriting,
//        nullValue,
//        emptyValue,
//        dateFormat,
//        timestampFormat,
//        comment,
//        enforceSchema,
//        inferSchema,
//        samplingRatio,
//        nanValue,
//        positiveInf,
//        negativeInf,
//        maxColumns,
//        maxCharsPerColumn,
//        unescapedQuoteHandling,
//        mode,
//        columnNameOfCorruptRecord,
//        multiLine,
//        escapeQuotes,
//        quoteAll,
//        compression,
//        partitionColumns,
//        writeMode,
//        locale,
//        lineSep,
//        pathGlobFilter,
//        modifiedBefore,
//        modifiedAfter,
//        recursiveFileLookup,
//        createSingleOutputFile,
//        skipHeaders,
//        skipFooters
//      )
//    }
//
//    override def writes(props: CsvProperties): JsValue = {
//      Json.obj(
//        "schema" -> props.schema,
//        "description" -> props.description,
//        "useSchema" -> props.useSchema,
//        "path" -> props.path,
//        "separator" -> props.separator,
//        "encoding" -> props.encoding,
//        "quote" -> props.quote,
//        "escape" -> props.escape,
//        "charToEscapeQuoteEscaping" -> props.charToEscapeQuoteEscaping,
//        "header" -> props.header,
//        "ignoreLeadingWhiteSpaceReading" -> props.ignoreLeadingWhiteSpaceReading,
//        "ignoreTrailingWhiteSpaceReading" -> props.ignoreTrailingWhiteSpaceReading,
//        "ignoreLeadingWhiteSpaceWriting" -> props.ignoreLeadingWhiteSpaceWriting,
//        "ignoreTrailingWhiteSpaceWriting" -> props.ignoreTrailingWhiteSpaceWriting,
//        "nullValue" -> props.nullValue,
//        "emptyValue" -> props.emptyValue,
//        "dateFormat" -> props.dateFormat,
//        "timestampFormat" -> props.timestampFormat,
//        "comment" -> props.comment,
//        "enforceSchema" -> props.enforceSchema,
//        "inferSchema" -> props.inferSchema,
//        "samplingRatio" -> props.samplingRatio,
//        "nanValue" -> props.nanValue,
//        "positiveInf" -> props.positiveInf,
//        "negativeInf" -> props.negativeInf,
//        "maxColumns" -> props.maxColumns,
//        "maxCharsPerColumn" -> props.maxCharsPerColumn,
//        "unescapedQuoteHandling" -> props.unescapedQuoteHandling,
//        "mode" -> props.mode,
//        "columnNameOfCorruptRecord" -> props.columnNameOfCorruptRecord,
//        "multiLine" -> props.multiLine,
//        "escapeQuotes" -> props.escapeQuotes,
//        "quoteAll" -> props.quoteAll,
//        "compression" -> props.compression,
//        "partitionColumns" -> props.partitionColumns,
//        "writeMode" -> props.writeMode,
//        "locale" -> props.locale,
//        "lineSep" -> props.lineSep,
//        "pathGlobFilter" -> props.pathGlobFilter,
//        "modifiedBefore" -> props.modifiedBefore,
//        "modifiedAfter" -> props.modifiedAfter,
//        "recursiveFileLookup" -> props.recursiveFileLookup,
//        "createSingleOutputFile" -> props.createSingleOutputFile,
//        "skipHeaders" -> props.skipHeaders,
//        "skipFooters" -> props.skipFooters
//      )
//    }
//  }
  implicit val csvPropertiesFormat: Format[CsvProperties] = Jsonx.formatCaseClass[CsvProperties]

  def sourceDialog: DatasetDialog = DatasetDialog("csv")
    .addSection("LOCATION", TargetLocation("path"))
    .addSection(
      "PROPERTIES",
      ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
        .addColumn(
          ScrollBox()
            .addElement(
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
                        .addField(Checkbox("Use user-defined schema"), "useSchema", true)
                        .addField(TextBox("Column delimiter").bindPlaceholder("").enableEscapeSequence(), "separator")
                        .addField(Checkbox("First row is header"), "header")
                        .addField(Checkbox("Infer column types from data"), "inferSchema")
                        .addField(Checkbox("Parse Multi-line records"), "multiLine")
                        .addField(TextBox("Encoding Type").bindPlaceholder(""), "encoding")
                        .addField(TextBox("Quote character").bindPlaceholder(""), "quote")
                        .addField(TextBox("Escape character").bindPlaceholder(""), "escape")
                        .addField(
                          TextBox("Escape char for quote escaping char").bindPlaceholder(""),
                          "charToEscapeQuoteEscaping"
                        )
                        .addField(TextBox("Skip line beginning with character").bindPlaceholder(""), "comment")
                        .addField(Checkbox("Enforce specified or inferred schema"), "enforceSchema")
                        .addField(TextBox("Sampling Ratio").bindPlaceholder(""), "samplingRatio")
                        .addField(Checkbox("Ignore leading white spaces from values"), "ignoreLeadingWhiteSpaceReading")
                        .addField(
                          Checkbox("Ignore trailing white spaces from values"),
                          "ignoreTrailingWhiteSpaceReading"
                        )
                        .addField(TextBox("Null Value").bindPlaceholder(""), "nullValue")
                        .addField(TextBox("Empty Value").bindPlaceholder(""), "emptyValue")
                        .addField(TextBox("String representation for non-number value").bindPlaceholder(""), "nanValue")
                        .addField(TextBox("Positive infinity value").bindPlaceholder(""), "positiveInf")
                        .addField(TextBox("Negative infinity value").bindPlaceholder(""), "negativeInf")
                        .addField(TextBox("Date format string").bindPlaceholder(""), "dateFormat")
                        .addField(TextBox("Timestamp format string").bindPlaceholder(""), "timestampFormat")
                        .addField(TextBox("Max number of columns per record").bindPlaceholder(""), "maxColumns")
                        .addField(
                          TextBox("Allowed maximum characters per column").bindPlaceholder(""),
                          "maxCharsPerColumn"
                        )
                        .addField(
                          SelectBox("Corrupt record handling")
                            .addOption("PERMISSIVE", "permissive")
                            .addOption("DROPMALFORMED", "dropmalformed")
                            .addOption("FAILFAST", "failfast"),
                          "mode"
                        )
                        .addField(
                          TextBox("Column name of a corrupt record").bindPlaceholder("_corrupt_record"),
                          "columnNameOfCorruptRecord"
                        )
                        .addField(TextBox("Line Sep").bindPlaceholder(""), "lineSep")
                        .addField(TextBox("Locale").bindPlaceholder(""), "locale")
                        .addField(
                          SelectBox("Unescaped Quote Handling")
                            .addOption("STOP_AT_CLOSING_QUOTE", "STOP_AT_CLOSING_QUOTE")
                            .addOption("BACK_TO_DELIMITER", "BACK_TO_DELIMITER")
                            .addOption("STOP_AT_DELIMITER", "STOP_AT_DELIMITER")
                            .addOption("SKIP_VALUE", "SKIP_VALUE")
                            .addOption("RAISE_ERROR", "RAISE_ERROR"),
                          "unescapedQuoteHandling"
                        )
                        .addField(Checkbox("Recursive File Lookup"), "recursiveFileLookup")
                        .addField(TextBox("Path Global Filter").bindPlaceholder(""), "pathGlobFilter")
                        .addField(TextBox("Modified Before").bindPlaceholder(""), "modifiedBefore")
                        .addField(TextBox("Modified After").bindPlaceholder(""), "modifiedAfter")
                        .addField(TextBox("Skip header lines").bindPlaceholder("5"), "skipHeaders")
                        .addField(TextBox("Skip footer lines").bindPlaceholder("5"), "skipFooters")
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

  def targetDialog: DatasetDialog = DatasetDialog("csv")
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
                      .bindProperty("partitionColumns"),
                    "partitionColumns"
                  )
                  .addField(TextBox("Column delimiter").bindPlaceholder("").enableEscapeSequence(), "separator")
                  .addField(Checkbox("First row is header"), "header")
                  .addField(TextBox("Encoding Type").bindPlaceholder(""), "encoding")
                  .addField(TextBox("Quote character").bindPlaceholder(""), "quote")
                  .addField(TextBox("Escape character").bindPlaceholder(""), "escape")
                  .addField(
                    TextBox("Escape char for quote escaping char").bindPlaceholder(""),
                    "charToEscapeQuoteEscaping"
                  )
                  .addField(TextBox("Null Value").bindPlaceholder(""), "nullValue")
                  .addField(TextBox("Empty Value").bindPlaceholder(""), "emptyValue")
                  .addField(
                    SelectBox("Compression")
                      .addOption("none", "none")
                      .addOption("bzip2", "bzip2")
                      .addOption("gzip", "gzip")
                      .addOption("lz4", "lz4")
                      .addOption("snappy", "snappy")
                      .addOption("deflate", "deflate"),
                    "compression"
                  )
                  .addField(Checkbox("Escape quotes"), "escapeQuotes")
                  .addField(Checkbox("Quote All"), "quoteAll")
                  .addField(TextBox("Date format string").bindPlaceholder(""), "dateFormat")
                  .addField(TextBox("Timestamp format string").bindPlaceholder(""), "timestampFormat")
                  .addField(Checkbox("Ignore leading white spaces from values"), "ignoreLeadingWhiteSpaceWriting")
                  .addField(Checkbox("Ignore trailing white spaces from values"), "ignoreTrailingWhiteSpaceWriting")
                  .addField(TextBox("Line Sep").bindPlaceholder(""), "lineSep")
                  .addField(Checkbox("Create single CSV file"), "createSingleOutputFile")
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

    // println("csv validate component: ", component)

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

    component.properties.samplingRatio match {
      case None ⇒ Unit
      case Some(value: SDouble) ⇒
        val (diag, sampleRatio) = (value.diagnostics, value.value)
        diagnostics ++= diag
        sampleRatio match {
          case None ⇒ Unit
          case Some(value) ⇒
            (0.0 < value) && (value <= 1.0) match {
              case true ⇒ Unit
              case false ⇒
                diagnostics += Diagnostic(
                  "properties.samplingRatio",
                  "Sampling Ratio has to be between (0.0, 1.0] [Properties]",
                  SeverityLevel.Error
                )
            }
        }
    }

    if (component.properties.columnNameOfCorruptRecord.isDefined) {
      if (component.properties.schema.isEmpty) {
        diagnostics += Diagnostic(
          "properties.columnNameOfCorruptRecord",
          "ColumnNameOfCorruptRecord will only work with a user-specified schema",
          SeverityLevel.Error
        )
      }
      if (
        component.properties.useSchema.isEmpty || (component.properties.useSchema.isDefined && !component.properties.useSchema.get)
      ) {
        diagnostics += Diagnostic(
          "properties.columnNameOfCorruptRecord",
          "ColumnNameOfCorruptRecord will only work if 'Use Schema' is enabled",
          SeverityLevel.Error
        )
      }

      if (component.properties.partitionColumns.isDefined && !component.properties.createSingleOutputFile.get) {
        diagnostics += Diagnostic(
          "properties.createSingleOutputFile",
          "Creation of single output file and partition columns cannot be used together.",
          SeverityLevel.Error
        )
      }
    }
    diagnostics.toList
  }

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = {
    if (newState.properties.schema.isDefined) {
      val newSchema = newState.properties.schema.get
      val schema =
        (oldState.properties.columnNameOfCorruptRecord, newState.properties.columnNameOfCorruptRecord) match {
          case (Some(oldCR), None) ⇒ StructType.apply(newSchema.fields.filter(f ⇒ f.name != oldCR))
          case (None, Some(newCR)) ⇒
            if (!newSchema.fieldNames.contains(newCR)) {
              newSchema.add(newCR, StringType, true)
            } else {
              newSchema
            }
          case (Some(oldCR), Some(newCR)) ⇒
            StructType.apply(newSchema.fields.filter(f ⇒ f.name != oldCR)).add(newCR, StringType, true)
          case (None, None) ⇒ newSchema
        }
      newState.copy(properties = newState.properties.copy(schema = Some(schema)))
    } else {
      newState
    }
  }

  override def deserializeProperty(props: String): PropertiesType = Json.parse(props).as[PropertiesType]

  override def serializeProperty(props: PropertiesType): String = Json.stringify(Json.toJson(props))

  class CsvFormatCode(props: CsvProperties) extends ComponentCode {

    def sourceApply(spark: SparkSession): DataFrame = {
      if (
        (props.skipHeaders.isDefined && props.skipHeaders.get != "0") ||
        (props.skipFooters.isDefined && props.skipFooters.get != "0")
      ) {
        import org.apache.spark.sql.Row
        val skipHeaderLines = props.skipHeaders.getOrElse("0").toInt

        val linesRDD = spark.sparkContext.textFile(props.path) // Read file as RDD of lines
        val totalCount = linesRDD.count()
        var skipFooterLines = 0
        if (totalCount > 0) {
          skipFooterLines = (totalCount - 1 - props.skipFooters.getOrElse("0").toInt).toInt
        }

        val csvRDD = linesRDD.zipWithIndex // Add line number to each line
          .filter {
            case (line, idx) => (idx >= skipHeaderLines) & (idx <= skipFooterLines)
          } // Filter out first n lines

        if (props.header.isDefined && props.header.get) {
          val dfSchema = StructType(
            csvRDD
              .take(1)
              .map { case (line, idx) => line.split(props.separator.get) } // Split each line into array of values
              .map(arr => Row.fromSeq(arr.toList))
              .take(1)(0)
              .toSeq
              .map(x => StructField(x.toString, StringType))
          )
          val finalRDD = csvRDD
            .filter {
              case (line, idx) => (idx >= skipHeaderLines + 1)
            }
            .map { case (line, idx) => line.split(props.separator.get) } // Split each line into array of values
            .map(arr => Row.fromSeq(arr.toList)) // Convert each array to a Row with a serializable collection
          if (props.useSchema.isDefined && props.useSchema.get) {
            val df = spark.createDataFrame(finalRDD, dfSchema)
            var userSchema = props.schema.get.toArray
            val selectCommands = df.columns.zipWithIndex.map {
              case (c, idx) => s"CAST(`$c` AS ${userSchema(idx).dataType.sql}) as `${userSchema(idx).name}`"
            }
            // Apply select command to DataFrame
            df.selectExpr(selectCommands: _*)
          } else
            spark.createDataFrame(finalRDD, dfSchema)
        } else {
          val finalRDD = csvRDD
            .map {
              case (line, idx) =>
                if (props.useSchema.isDefined && props.useSchema.get)
                  line.split(props.separator.get).padTo(props.schema.get.toArray.length, null)
                else line.split(props.separator.get)
            } // Split each line into array of values
            .map(arr => Row.fromSeq(arr.toList)) // Convert each array to a Row with a serializable collection
          val newSchema = StructType(
            Array.tabulate(finalRDD.first.size)(i => StructField(s"col$i", StringType))
          ) // Infer schema based on first row
          if (props.useSchema.isDefined && props.useSchema.get) {
            val df = spark.createDataFrame(finalRDD, newSchema)
            var userSchema = props.schema.get.toArray
            val selectCommands = df.columns.zipWithIndex.map {
              case (c, idx) => s"CAST(`col$idx` AS ${userSchema(idx).dataType.sql}) as `${userSchema(idx).name}`"
            }
            // Apply select command to DataFrame
            df.selectExpr(selectCommands: _*)
          } else
            spark.createDataFrame(finalRDD, newSchema)
        }
      } else {
        var reader = spark.read
          .format("csv")
          .option("negativeInf", props.negativeInf)
          .option("maxCharsPerColumn", props.maxCharsPerColumn)
          .option("columnNameOfCorruptRecord", props.columnNameOfCorruptRecord)
          .option("header", props.header)
          .option("inferSchema", props.inferSchema)
          .option("mode", props.mode)
          .option("dateFormat", props.dateFormat)
          .option("samplingRatio", props.samplingRatio)
          .option("positiveInf", props.positiveInf)
          .option("escape", props.escape)
          .option("emptyValue", props.emptyValue)
          .option("timestampFormat", props.timestampFormat)
          .option("quote", props.quote)
          .option("sep", props.separator)
          .option("enforceSchema", props.enforceSchema)
          .option("encoding", props.encoding)
          .option("comment", props.comment)
          .option("locale", props.locale)
          .option("lineSep", props.lineSep)
          .option("unescapedQuoteHandling", props.unescapedQuoteHandling)
          .option("charToEscapeQuoteEscaping", props.charToEscapeQuoteEscaping)
          .option("nanValue", props.nanValue)
          .option("ignoreLeadingWhiteSpace", props.ignoreLeadingWhiteSpaceReading)
          .option("ignoreTrailingWhiteSpace", props.ignoreTrailingWhiteSpaceReading)
          .option("nullValue", props.nullValue)
          .option("maxColumns", props.maxColumns)
          .option("multiLine", props.multiLine)
          .option("modifiedBefore", props.modifiedBefore)
          .option("modifiedAfter", props.modifiedAfter)
          .option("recursiveFileLookup", props.recursiveFileLookup)
          .option("pathGlobFilter", props.pathGlobFilter)
        if (props.useSchema.isDefined && props.useSchema.get) {
          props.schema.foreach(schema ⇒ reader = reader.schema(schema))
        }
        reader.load(props.path)
      }

    }

    def targetApply(spark: SparkSession, in: DataFrame): Unit = {
      var writer = in.write
        .format("csv")
        .option("header", props.header)
        .option("dateFormat", props.dateFormat)
        .option("escape", props.escape)
        .option("emptyValue", props.emptyValue)
        .option("timestampFormat", props.timestampFormat)
        .option("quote", props.quote)
        .option("sep", props.separator)
        .option("quoteAll", props.quoteAll)
        .option("encoding", props.encoding)
        .option("charToEscapeQuoteEscaping", props.charToEscapeQuoteEscaping)
        .option("escapeQuotes", props.escapeQuotes)
        .option("ignoreLeadingWhiteSpace", props.ignoreLeadingWhiteSpaceWriting)
        .option("ignoreTrailingWhiteSpace", props.ignoreTrailingWhiteSpaceWriting)
        .option("nullValue", props.nullValue)
        .option("compression", props.compression)
        .option("lineSep", props.lineSep)
      props.writeMode.foreach { mode ⇒
        writer = writer.mode(mode)
      }
      props.partitionColumns.foreach(pcols ⇒
        writer = pcols match {
          case Nil ⇒ writer
          case _ ⇒ writer.partitionBy(pcols: _*)
        }
      )
      if (props.createSingleOutputFile.isDefined) {
        writer.save(props.path + "_temp")
      } else {
        writer.save(props.path)
      }
      if (props.createSingleOutputFile.isDefined) {
        if (props.createSingleOutputFile.get) {
          concatenateFiles(spark, ".csv", props.writeMode.get, props.path + "_temp", props.path, true, true)
        }
      }
    }
  }
}
