package io.prophecy.scalagems.dataset

import ai.x.play.json.Encoders.encoder
import ai.x.play.json.Jsonx
import io.prophecy.gems._
import io.prophecy.gems.datasetSpec.{DatasetProperties, DatasetSpec}
import io.prophecy.gems.diagnostics.{Diagnostic, SeverityLevel}
import io.prophecy.gems.uiSpec._
import io.prophecy.libs._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.{Format, Json}

class xlsx extends DatasetSpec {

  val name: String = "xlsx"
  val datasetType: String = "File"

  type PropertiesType = XLSXProperties
  case class XLSXProperties(
    @Property("Schema")
    schema: Option[StructType] = None,
    @Property("Description")
    description: Option[String] = Some(""),
    @Property("useSchema")
    useSchema: Option[Boolean] = Some(false),
    @Property("path")
    path: String = "",
    @Property("columnNameOfCorruptRecord")
    columnNameOfCorruptRecord: Option[String] = None,
    @Property("columnNameOfRowNumber")
    columnNameOfRowNumber: Option[String] = None,
    @Property("dataAddress")
    dataAddress: String = "",
    @Property("dateFormat")
    dateFormat: Option[String] = None,
    @Property("excerptSize")
    excerptSize: Option[String] = None,
    @Property("fileExtension")
    fileExtension: Option[String] = None,
    @Property("header")
    header: Boolean = true,
    @Property("ignoreAfterHeader")
    ignoreAfterHeader: Option[Boolean] = None,
    @Property("ignoreLeadingWhiteSpace")
    ignoreLeadingWhiteSpace: Option[Boolean] = None,
    @Property("ignoreTrailingWhiteSpace")
    ignoreTrailingWhiteSpace: Option[Boolean] = None,
    @Property("inferSchema")
    inferSchema: Option[Boolean] = None,
    @Property("keepUndefinedRows")
    keepUndefinedRows: Option[Boolean] = None,
    @Property("locale")
    locale: Option[String] = None,
    @Property("nanValue")
    nanValue: Option[String] = None,
    @Property("nullValue")
    nullValue: Option[String] = None,
    @Property("parseMode")
    parseMode: Option[String] = None,
    @Property("positiveInf")
    positiveInf: Option[String] = None,
    @Property("samplingRatio")
    samplingRatio: Option[String] = None,
    @Property("timestampFormat")
    timestampFormat: Option[String] = None,
    @Property("useNullForErrorCells")
    useNullForErrorCells: Option[Boolean] = None,
    @Property("usePlainNumberFormat")
    usePlainNumberFormat: Option[Boolean] = None,
    @Property("workbookPassword")
    workbookPassword: Option[String] = None,
    @Property("zoneId")
    zoneId: Option[String] = None,
    @Property("maxRowsInMemory")
    maxRowsInMemory: Option[Int] = None,
    @Property("maxByteArraySize")
    maxByteArraySize: Option[Int] = None,
    @Property("workbookPassword")
    writeMode: Option[String] = Some("overwrite"),
    @Property("partitionColumns")
    partitionColumns: Option[List[String]] = None
  ) extends DatasetProperties

  implicit val XLSXPropertiesFormat: Format[XLSXProperties] = Jsonx.formatCaseClass[XLSXProperties]

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
                      .addField(TextBox("Data Address").bindPlaceholder("'MySheet1'!A3:AB10"), "dataAddress", true)
                      .addField(Checkbox("Enforce schema"), "useSchema", true)
                      .addField(Checkbox("Header"), "header", true)
                      .addField(TextBox("Column Name of Corrupt Record"), "columnNameOfCorruptRecord", false)
                      .addField(TextBox("Column Name of Row Number"), "columnNameOfRowNumber", false)
                      .addField(TextBox("Date Format"), "dateFormat", false)
                      .addField(TextBox("Excerpt Size").bindPlaceholder("10"), "excerptSize", false)
                      .addField(TextBox("File Extension"), "fileExtension", false)
                      .addField(Checkbox("Ignore leading whitespace"), "ignoreLeadingWhiteSpace", false)
                      .addField(Checkbox("Ignore trailing whitespace"), "ignoreTrailingWhiteSpace", false)
                      .addField(Checkbox("Infer Schema"), "inferSchema", false)
                      .addField(TextBox("Locale"), "locale", false)
                      .addField(TextBox("Negative Infinite value"), "negativeInf", false)
                      .addField(TextBox("Null value"), "nullValue", false)
                      .addField(
                        SelectBox("Parser Mode")
                          .addOption("Permissive", "PERMISSIVE")
                          .addOption("Drop Malformed", "DROPMALFORMED")
                          .addOption("Fail Fast", "FAILFAST"),
                        "parseMode"
                      )
                      .addField(TextBox("Positive Infinite value"), "positiveInf", false)
                      .addField(TextBox("Sampling Ratio"), "samplingRatio", false)
                      .addField(TextBox("Timestamp Format"), "timestampFormat", false)
                      .addField(Checkbox("Use Null for Error Cells"), "useNullForErrorCells", false)
                      .addField(TextBox("Workbook Password"), "workbookPassword")
                      .addField(TextBox("Time Zone ID"), "zoneId")
                      .addField(TextBox("Max Rows in Memory").bindPlaceholder("10"), "maxRowsInMemory")
                      .addField(TextBox("Max Byte Array Size").bindPlaceholder("10"), "maxByteArraySize")
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

  def targetDialog: DatasetDialog = DatasetDialog("xlsx")
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
                      .addField(Checkbox("Header"), "header", true)
                      .addField(TextBox("Date Format"), "dateFormat", true)
                      .addField(TextBox("Data Address").bindPlaceholder("'MySheet1'!A3:AB10"), "dataAddress", true)
                      .addField(TextBox("File Extension").bindPlaceholder("xlsx"), "fileExtension")
                      .addField(TextBox("Locale"), "locale")
                      .addField(Checkbox("Use Plain Number Format"), "usePlainNumberFormat")
                      .addField(TextBox("Workbook Password"), "workbookPassword")
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
    diagnostics.toList
  }

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = newState

  class XLSXFormatCode(props: XLSXProperties) extends ComponentCode {
    def sourceApply(spark: SparkSession): DataFrame = {
      var reader = spark.read
        .format("excel")
        .option("header", props.header)
        .option("dataAddress", props.dataAddress)

      if (props.schema.isDefined && props.schema.get.fields.nonEmpty)
        reader = reader.schema(props.schema.get)
      if (props.fileExtension.isDefined)
        reader = reader.option("fileExtension", props.fileExtension.get)
      if (props.ignoreLeadingWhiteSpace.isDefined)
        reader = reader.option("ignoreLeadingWhiteSpace", props.ignoreLeadingWhiteSpace.get)
      if (props.ignoreTrailingWhiteSpace.isDefined)
        reader = reader.option("ignoreTrailingWhiteSpace", props.ignoreTrailingWhiteSpace.get)
      if (props.inferSchema.isDefined)
        reader = reader.option("inferSchema", props.inferSchema.get)
      if (props.keepUndefinedRows.isDefined)
        reader = reader.option("keepUndefinedRows", props.keepUndefinedRows.get)
      if (props.parseMode.isDefined)
        reader = reader.option("parseMode", props.parseMode.get)
      if (props.useNullForErrorCells.isDefined)
        reader = reader.option("useNullForErrorCells", props.useNullForErrorCells.get)
      if (props.columnNameOfCorruptRecord.isDefined)
        reader = reader.option("columnNameOfCorruptRecord", props.columnNameOfCorruptRecord.get)
      if (props.columnNameOfRowNumber.isDefined)
        reader = reader.option("columnNameOfRowNumber", props.columnNameOfRowNumber.get)
      if (props.dateFormat.isDefined)
        reader = reader.option("dateFormat", props.dateFormat.get)
      if (props.excerptSize.isDefined)
        reader = reader.option("excerptSize", props.excerptSize.get)
      if (props.ignoreAfterHeader.isDefined)
        reader = reader.option("ignoreAfterHeader", props.ignoreAfterHeader.get)
      if (props.locale.isDefined)
        reader = reader.option("locale", props.locale.get)
      if (props.nanValue.isDefined)
        reader = reader.option("nanValue", props.nanValue.get)
      if (props.nullValue.isDefined)
        reader = reader.option("nullValue", props.nullValue.get)
      if (props.positiveInf.isDefined)
        reader = reader.option("positiveInf", props.positiveInf.get)
      if (props.samplingRatio.isDefined)
        reader = reader.option("samplingRatio", props.samplingRatio.get)
      if (props.timestampFormat.isDefined)
        reader = reader.option("timestampFormat", props.timestampFormat.get)
      if (props.workbookPassword.isDefined)
        reader = reader.option("workbookPassword", props.workbookPassword.get)
      if (props.zoneId.isDefined)
        reader = reader.option("zoneId", props.zoneId.get)
      if (props.maxRowsInMemory.isDefined)
        reader = reader.option("maxRowsInMemory", props.maxRowsInMemory.get)
      if (props.maxByteArraySize.isDefined)
        reader = reader.option("maxByteArraySize", props.maxByteArraySize.get)

      reader.load(props.path)
    }

    def targetApply(spark: SparkSession, in: DataFrame): Unit = {
      var writer = in.write
        .format("excel")
        .option("header", props.header)

      if (props.fileExtension.isDefined)
        writer = writer.option("dataAddress", props.dataAddress)
      if (props.fileExtension.isDefined)
        writer = writer.option("fileExtension", props.fileExtension.get)
      if (props.locale.isDefined)
        writer = writer.option("locale", props.locale.get)
      if (props.dateFormat.isDefined)
        writer = writer.option("dateFormat", props.dateFormat.get)
      if (props.usePlainNumberFormat.isDefined)
        writer = writer.option("usePlainNumberFormat", props.usePlainNumberFormat.get)
      if (props.workbookPassword.isDefined)
        writer = writer.option("workbookPassword", props.workbookPassword.get)
      if (props.writeMode.isDefined)
        writer = writer.mode(props.writeMode.get)
      props.writeMode.foreach(mode ⇒ writer = writer.mode(mode))
      if (props.partitionColumns.isDefined)
        writer = writer.partitionBy(props.partitionColumns.get: _*)

      writer.save(props.path)
    }
  }

  override def deserializeProperty(props: String): PropertiesType = Json.parse(props).as[PropertiesType]

  override def serializeProperty(props: PropertiesType): String = Json.stringify(Json.toJson(props))
}
