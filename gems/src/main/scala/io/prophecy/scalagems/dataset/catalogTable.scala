package io.prophecy.scalagems.dataset

import ai.x.play.json.Encoders.encoder
import ai.x.play.json.Jsonx
import io.prophecy.gems._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.datasetSpec._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.uiSpec._
import org.apache.spark.sql.functions.{col, concat, lit}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.{Format, Json}

class catalogTable extends DatasetSpec {

  val name: String = "catalogTable"
  val datasetType: String = "Database"
  val docUrl: String = "https://docs.prophecy.io/low-code-spark/gems/source-target/catalog-table/delta"

  type PropertiesType = CatalogTableProperties
  case class CatalogTableProperties(
                                     @Property("Schema")
                                     schema: Option[StructType] = None,
                                     @Property("Description")
                                     description: Option[String] = Some(""),
                                     @Property("Database")
                                     path: String = "",
                                     @Property("Table")
                                     tableName: String = "",
                                     @Property("Write Mode", """(default: "error") Specifies the behavior when data or table already exists.""")
                                     writeMode: Option[String] = Some("error"),
                                     @Property("fileFormat", "Hive specific file format")
                                     fileFormat: Option[String] = Some("parquet"),
                                     @Property("", "table provider")
                                     provider: Option[String] = Some("delta"),
                                     @Property("partitionColumns", "Partitioning column.")
                                     partitionColumns: Option[List[String]] = None,
                                     @Property("insertInto", "Use insert statement")
                                     insertInto: Option[Boolean] = Some(false),
                                     @Property("filterQuery", "Filter query")
                                     filterQuery: Option[String] = Some(""),
                                     @Property("useExternalFilePath", "Use non default file path (dbfs:/user/hive/warehouse/)?")
                                     useExternalFilePath: Option[Boolean] = Some(false),
                                     @Property("externalFilePath", "Custom file path")
                                     externalFilePath: Option[String] = Some(""),
                                     @Property(
                                       "timestampAsOf",
                                       "read a table snapshot as of given timestamp, for timestamp only data or timestamp strings are accepted ex. \"2019-01-01\" and \"2019-01-01T00:00:00.000Z\" "
                                     )
                                     timestampAsOf: Option[String] = None,
                                     @Property("versionAsOf", "select a table snapshot of given version")
                                     versionAsOf: Option[String] = None,
                                     @Property(
                                       "replaceWhere",
                                       """(default: "") specifies predicate over partition column which should be used to overwrite"""
                                     )
                                     replaceWhere: Option[String] = None,
                                     @Property("overwriteSchema", "(default: false) overwrite the table schema with dataframe's schema")
                                     overwriteSchema: Option[Boolean] = None,
                                     @Property("mergeSchema", "(default: false) update the table schema from dataframe schema")
                                     mergeSchema: Option[Boolean] = None,
                                     @Property("optimizeWrite", "(default: true)")
                                     optimizeWrite: Option[Boolean] = None,
                                     @Property("", "")
                                     mergeSourceAlias: Option[String] = Some("source"),
                                     @Property("", "")
                                     mergeTargetAlias: Option[String] = Some("target"),
                                     @Property("", "")
                                     mergeCondition: Option[SColumn] = None,
                                     @Property("", "")
                                     activeTab: Option[String] = Some("whenMatched"),
                                     @Property("", "")
                                     matchedAction: Option[String] = Some("update"),
                                     @Property("", "")
                                     matchedActionDelete: Option[String] = Some("ignore"),
                                     @Property("", "")
                                     notMatchedAction: Option[String] = Some("insert"),
                                     @Property("", "")
                                     matchedCondition: Option[SColumn] = None,
                                     @Property("", "")
                                     matchedConditionDelete: Option[SColumn] = None,
                                     @Property("", "")
                                     notMatchedCondition: Option[SColumn] = None,
                                     @Property("", "")
                                     matchedTable: Option[List[SColumnExpression]] = Some(Nil),
                                     @Property("", "")
                                     notMatchedTable: Option[List[SColumnExpression]] = Some(Nil),
                                     @Property("", "")
                                     keyColumns: Option[List[String]] = Some(Nil),
                                     @Property("", "")
                                     historicColumns: Option[List[String]] = Some(Nil),
                                     @Property("", "")
                                     fromTimeCol: Option[String] = None,
                                     @Property("", "")
                                     toTimeCol: Option[String] = None,
                                     @Property("", "")
                                     minFlagCol: Option[String] = None,
                                     @Property("", "")
                                     maxFlagCol: Option[String] = None,
                                     @Property("", "")
                                     flagValue: Option[String] = Some("integer"),
                                     @Property("", "")
                                     isCatalogEnabled: Option[Boolean] = None,
                                     @Property("", "")
                                     catalog: Option[String] = None
                                   ) extends DatasetProperties

  implicit val catalogTablePropertiesFormat: Format[CatalogTableProperties] = Jsonx.formatCaseClass[CatalogTableProperties]

  def sourceDialog: DatasetDialog = {
    val fieldPicker = FieldPicker(height = Some("100%"))
      .addField(
        TextArea("Description", 2, placeholder = "Dataset description..."),
        "description",
        true
      )
      .addField(
        SelectBox("Provider")
          .addOption("delta", "delta")
          .addOption("hive", "hive"),
        "provider",
        true
      )

    DatasetDialog("catalogTable")
      .addSection(
        "LOCATION",
        ColumnsLayout()
          .addColumn(
            StackLayout(direction = Some("vertical"), gap = Some("1rem"))
              .addElement(
                CatalogTableDB("")
                  .bindProperty("path")
                  .bindTableProperty("tableName")
                  .bindIsCatalogEnabledProperty("isCatalogEnabled")
                  .bindCatalogProperty("catalog")
              )
          )
      )
      .addSection(
        "PROPERTIES",
        ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
          .addColumn(
            StackLayout()
              .addElement(
                Condition()
                  .ifEqual(
                    PropExpr("component.properties.provider"),
                    StringExpr("delta")
                  )
                  .then(
                    StackItem(grow = Some(1))
                      .addElement(
                        fieldPicker
                          .addField(TextBox("Read timestamp").bindPlaceholder(""), "timestampAsOf")
                          .addField(TextBox("Read version").bindPlaceholder(""), "versionAsOf")
                      )
                      .addElement(
                        ScrollBox().addElement(
                          StackLayout()
                            .addElement(TitleElement(title = "Filter Predicate"))
                            .addElement(Editor(height = Some("30bh")).bindProperty("filterQuery"))
                        )
                      )
                  )
                  .otherwise(
                    StackItem(grow = Some(1))
                      .addElement(
                        fieldPicker
                      )
                      .addElement(
                        ScrollBox().addElement(
                          StackLayout()
                            .addElement(TitleElement(title = "Filter Predicate"))
                            .addElement(Editor(height = Some("40bh")).bindProperty("filterQuery"))
                        )
                      )
                  )
              ),
            "auto"
          )
          .addColumn(SchemaTable("").isReadOnly().bindProperty("schema"), "5fr")
      )
      .addSection(
        "PREVIEW",
        PreviewTable("").bindProperty("schema")
      )
  }

  def targetDialog: DatasetDialog = {
    val matchedTable = ExpTable("Expressions").bindProperty("matchedTable")

    val notMatchedTable = ExpTable("Expressions").bindProperty("notMatchedTable")

    val whenNotMatchedTabPane = TabPane("When Not Matched", "notMatched")
      .addElement(
        ColumnsLayout(gap = Some("1rem"))
          .addColumn(
            StackLayout(height = Some("100%"))
              .addElement(
                SelectBox("Action")
                  .addOption("insert", "insert")
                  .addOption("ignore", "ignore")
                  .bindProperty("notMatchedAction")
              )
              .addElement(
                Condition()
                  .ifNotEqual(
                    PropExpr("component.properties.notMatchedAction"),
                    StringExpr("ignore")
                  )
                  .then(
                    ColumnsLayout(gap = Some("1rem"))
                      .addColumn(
                        StackLayout()
                          .addElement(
                            NativeText("Only when the additional condition is true")
                          )
                          .addElement(
                            Editor(height = Some("40bh"))
                              .makeFieldOptional()
                              .withSchemaSuggestions()
                              .bindProperty("notMatchedCondition")
                          )
                      )
                      .addColumn(
                        StackLayout(height = Some("100%"))
                          .addElement(
                            NativeText(
                              "Replace default update with these expressions (optional)"
                            )
                          )
                          .addElement(notMatchedTable)
                      )
                  )
              )
          )
      )

    val mergeView = StackLayout()
      .addElement(TitleElement("Merge Condition"))
      .addElement(
        ColumnsLayout(gap = Some("1rem"))
          .addColumn(
            TextBox("Source Alias: for the new data coming in")
              .bindPlaceholder("source alias")
              .bindProperty("mergeSourceAlias")
          )
          .addColumn(
            TextBox("Target Alias: for the new existing data")
              .bindPlaceholder("target alias")
              .bindProperty("mergeTargetAlias")
          )
      )
      .addElement(NativeText("Merge condition: checks if you need to merge this row"))
      .addElement(
        Editor(height = Some("40bh"))
          .makeFieldOptional()
          .withSchemaSuggestions()
          .bindProperty("mergeCondition")
      )
      .addElement(TitleElement("Custom Clauses"))
      .addElement(
        StackLayout(height = Some("100bh"))
          .addElement(
            Tabs()
              .bindProperty("activeTab")
              .addTabPane(
                TabPane("When Matched Update", "whenMatched")
                  .addElement(
                    ColumnsLayout(gap = Some("1rem"))
                      .addColumn(
                        StackLayout(height = Some("100%"))
                          .addElement(
                            SelectBox("Action")
                              .addOption("update", "update")
                              .addOption("ignore", "ignore")
                              .bindProperty("matchedAction")
                          )
                          .addElement(
                            Condition()
                              .ifNotEqual(
                                PropExpr("component.properties.matchedAction"),
                                StringExpr("ignore")
                              )
                              .then(
                                ColumnsLayout(gap = Some("1rem"))
                                  .addColumn(
                                    StackLayout()
                                      .addElement(
                                        NativeText("Only when the additional condition is true")
                                      )
                                      .addElement(
                                        Editor(height = Some("40bh"))
                                          .makeFieldOptional()
                                          .withSchemaSuggestions()
                                          .bindProperty("matchedCondition")
                                      )
                                  )
                                  .addColumn(
                                    Condition()
                                      .ifEqual(
                                        PropExpr("component.properties.matchedAction"),
                                        StringExpr("update")
                                      )
                                      .then(
                                        StackLayout(height = Some("100%"))
                                          .addElement(
                                            NativeText(
                                              "Replace default update with these expressions (optional)"
                                            )
                                          )
                                          .addElement(matchedTable)
                                      )
                                  )
                              )
                          )
                      )
                  )
              )
              .addTabPane(
                TabPane("When Matched Delete", "whenMatchedDelete")
                  .addElement(
                    ColumnsLayout(gap = Some("1rem"))
                      .addColumn(
                        StackLayout(height = Some("100%"))
                          .addElement(
                            SelectBox("Action")
                              .addOption("delete", "delete")
                              .addOption("ignore", "ignore")
                              .bindProperty("matchedActionDelete")
                          )
                          .addElement(
                            Condition()
                              .ifNotEqual(
                                PropExpr("component.properties.matchedActionDelete"),
                                StringExpr("ignore")
                              )
                              .then(
                                ColumnsLayout(gap = Some("1rem"))
                                  .addColumn(
                                    StackLayout()
                                      .addElement(
                                        NativeText("Only when the additional condition is true")
                                      )
                                      .addElement(
                                        Editor(height = Some("40bh"))
                                          .makeFieldOptional()
                                          .withSchemaSuggestions()
                                          .bindProperty("matchedConditionDelete")
                                      )
                                  )
                              )
                          )
                      )
                  )
              )
              .addTabPane(
                whenNotMatchedTabPane
              )
          )
      )

    val scd2View = StackLayout()
      .addElement(TitleElement("Merge Details"))
      .addElement(
        StackLayout(height = Some("100%"))
          .addElement(
            SchemaColumnsDropdown("Key Columns")
              .withMultipleSelection()
              .bindSchema("schema")
              .bindProperty("keyColumns")
              .showErrorsFor("keyColumns")
          )
          .addElement(
            SchemaColumnsDropdown("Historic Columns")
              .withMultipleSelection()
              .bindSchema("schema")
              .bindProperty("historicColumns")
              .showErrorsFor("historicColumns")
          )
      )
      .addElement(TitleElement("Time Columns"))
      .addElement(
        StackLayout(height = Some("100%"))
          .addElement(
            SchemaColumnsDropdown("From time column")
              .bindSchema("schema")
              .bindProperty("fromTimeCol")
              .showErrorsFor("fromTimeCol")
          )
          .addElement(
            SchemaColumnsDropdown("To time column")
              .bindSchema("schema")
              .bindProperty("toTimeCol")
              .showErrorsFor("toTimeCol")
          )
      )
      .addElement(TitleElement("Flags"))
      .addElement(
        StackLayout(height = Some("100%"))
          .addElement(
            ColumnsLayout(gap = Some("1rem"))
              .addColumn(
                TextBox("Name of the column used as min/old-value flag")
                  .bindPlaceholder("Enter the name of the column")
                  .bindProperty("minFlagCol")
              )
              .addColumn(
                TextBox("Name of the column used as max/latest flag")
                  .bindPlaceholder("Enter the name of the column")
                  .bindProperty("maxFlagCol")
              )
          )
          .addElement(
            SelectBox("Flag values")
              .addOption("0/1", "integer")
              .addOption("true/false", "boolean")
              .bindProperty("flagValue")
          )
      )

    val fieldPicker = FieldPicker(height = Some("100%"))
      .addField(
        TextArea("Description", 2, placeholder = "Dataset description..."),
        "description",
        true
      )
      .addField(
        SelectBox("Provider")
          .addOption("delta", "delta")
          .addOption("hive", "hive"),
        "provider",
        true
      )

    val deltaFieldPicker = fieldPicker
      .addField(
        SelectBox("Write Mode")
          .addOption("overwrite", "overwrite")
          .addOption("error", "error")
          .addOption("append", "append")
          .addOption("ignore", "ignore")
          .addOption("merge", "merge")
          .addOption("scd2 merge", "merge_scd2"),
        "writeMode",
        true
      )
      .addField(Checkbox("Use insert into"), "insertInto")
      .addField(Checkbox("Overwrite table schema"), "overwriteSchema")
      .addField(Checkbox("Merge dataframe schema into table schema"), "mergeSchema")
      .addField(
        SchemaColumnsDropdown("Partition Columns")
          .withMultipleSelection()
          .bindSchema("schema")
          .showErrorsFor("partitionColumns"),
        "partitionColumns"
      )
      .addField(TextBox("Overwrite partition predicate").bindPlaceholder(""), "replaceWhere")
      .addField(Checkbox("Optimize write"), "optimizeWrite")

    val hiveFieldPicker = fieldPicker
      .addField(
        SelectBox("Write Mode")
          .addOption("overwrite", "overwrite")
          .addOption("error", "error")
          .addOption("append", "append")
          .addOption("ignore", "ignore"),
        "writeMode",
        true
      )
      .addField(
        SelectBox("File Format")
          .addOption("sequencefile", "sequencefile")
          .addOption("rcfile", "rcfile")
          .addOption("orc", "orc")
          .addOption("parquet", "parquet")
          .addOption("textfile", "textfile")
          .addOption("avro", "avro"),
        "fileFormat",
        true
      )
      .addField(Checkbox("Use insert into"), "insertInto")
      .addField(
        SchemaColumnsDropdown("Partition Columns")
          .withMultipleSelection()
          .bindSchema("schema")
          .showErrorsFor("partitionColumns"),
        "partitionColumns"
      )

    DatasetDialog("catalogTable")
      .addSection(
        "LOCATION",
        StackLayout()
          .addElement(
            CatalogTableDB("")
              .bindProperty("path")
              .bindTableProperty("tableName")
              .bindIsCatalogEnabledProperty("isCatalogEnabled")
              .bindCatalogProperty("catalog")
          )
          .addElement(Checkbox("Use File Path").bindProperty("useExternalFilePath"))
          .addElement(
            Condition()
              .ifEqual(
                PropExpr("component.properties.useExternalFilePath"),
                BooleanExpr(true)
              )
              .then(
                TextBox(
                  "File location",
                  placeholder = "dbfs:/FileStore/delta/tableName"
                ).bindProperty("externalFilePath")
              )
          )
      )
      .addSection(
        "PROPERTIES",
        ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
          .addColumn(
            StackLayout().addElement(
              StackLayout(height = Some("100%"))
                .addElement(
                  StackItem(grow = Some(1)).addElement(
                    Condition()
                      .ifEqual(
                        PropExpr("component.properties.provider"),
                        StringExpr("delta")
                      )
                      .then(
                        deltaFieldPicker
                      )
                      .otherwise(
                        hiveFieldPicker
                      )
                  )
                )
            ),
            "auto"
          )
          .addColumn(
            Condition()
              .ifEqual(PropExpr("component.properties.provider"), StringExpr("hive"))
              .then(SchemaTable("").isReadOnly().withoutInferSchema().bindProperty("schema"))
              .otherwise(
                Condition()
                  .ifEqual(PropExpr("component.properties.writeMode"), StringExpr("merge"))
                  .then(mergeView)
                  .otherwise(
                    Condition()
                      .ifEqual(PropExpr("component.properties.writeMode"), StringExpr("merge_scd2"))
                      .then(scd2View)
                      .otherwise(SchemaTable("").isReadOnly().withoutInferSchema().bindProperty("schema"))
                  )
              ),
            "5fr"
          )
      )
  }

  override def validate(component: Component)(implicit context: WorkflowContext): List[Diagnostic] = {
    import java.util.regex.Pattern
    import scala.collection.mutable.ListBuffer
    val diagnostics = ListBuffer[Diagnostic]()
    diagnostics ++= super.validate(component)
    val props = component.properties

    println("catalogTable validate component: ", component)
    val NAME_PATTERN: Pattern = "^[\\w]+$".r.pattern
    val CONFIG_NAME_PATTERN: Pattern = "^\\$\\{.*}$".r.pattern

    if (component.properties.isCatalogEnabled.isDefined && component.properties.isCatalogEnabled.get) {
      if (component.properties.catalog.isDefined && component.properties.catalog.get.trim.isEmpty) {
        diagnostics += Diagnostic("properties.path", "Catalog Name cannot be empty [Location]", SeverityLevel.Error)
      }
    }

    if (component.properties.path.trim.isEmpty) {
      diagnostics += Diagnostic("properties.path", "Database Name cannot be empty [Location]", SeverityLevel.Error)
    }
    //    else {
    //      if (
    //        !CONFIG_NAME_PATTERN.matcher(component.properties.path).matches() && (!NAME_PATTERN
    //          .matcher(component.properties.path)
    //          .matches())
    //      ) {
    //        diagnostics += Diagnostic(
    //          "properties.newDataset.path",
    //          s"${component.properties.path} is not a valid name for databases. Valid names only contain alphabet characters, numbers and _. [Location]",
    //          SeverityLevel.Error
    //        )
    //      }
    //    }
    if (component.properties.tableName.trim.isEmpty) {
      diagnostics += Diagnostic("properties.tableName", "Table Name cannot be empty [Location]", SeverityLevel.Error)
    }
    //    else {
    //      if (
    //        (!CONFIG_NAME_PATTERN.matcher(component.properties.tableName).matches()) && (!NAME_PATTERN
    //          .matcher(component.properties.tableName)
    //          .matches())
    //      ) {
    //        diagnostics += Diagnostic(
    //          "properties.newDataset.tableName",
    //          s"${component.properties.tableName} is not a valid name for tables. Valid names only contain alphabet characters, numbers and _. [Location]",
    //          SeverityLevel.Error
    //        )
    //      }
    //    }
    if (component.properties.schema.isEmpty) {
      // diagnostics += Diagnostic("properties.schema", "Schema cannot be empty [Properties]", SeverityLevel.Error)
    }
    if (
      component.properties.useExternalFilePath.getOrElse(false)
        && component.properties.externalFilePath.get.trim.isEmpty
    ) {
      diagnostics += Diagnostic("properties.useExternalFilePath", "File Location cannot be empty", SeverityLevel.Error)
    }

    val invalidVersion = props.versionAsOf match {
      case None ⇒ None
      case Some(value: String) ⇒
        if (value.isEmpty) None
        else
          getIntOption(value) match {
            case None ⇒
              Some(Diagnostic("properties.versionAsOF", "Invalid version [Properties]", SeverityLevel.Error))
            case Some(value) ⇒ None
          }
    }
    if (invalidVersion.isDefined) diagnostics += invalidVersion.get

    // validation for when deltaMerge selected
    if (props.writeMode.contains("merge")) {
      // validate merge condition
      if (props.mergeCondition.isEmpty || props.mergeCondition.get.expression.trim.isEmpty) {
        diagnostics += Diagnostic(
          "properties.mergeCondition",
          "Merge condition can not be empty [Properties]",
          SeverityLevel.Error
        )
      } else if (component.properties.mergeCondition.get.expression.isEmpty) {
        diagnostics += Diagnostic(
          "properties.mergeCondition",
          s"Unsupported expression ${component.properties.mergeCondition.get.expression} [Properties]",
          SeverityLevel.Error
        )
      }
      // validate source and target alias presence
      if (props.mergeSourceAlias.getOrElse("").trim.isEmpty) {
        diagnostics += Diagnostic(
          "properties.mergeSourceAlias",
          "Source Alias can not be empty [Properties]",
          SeverityLevel.Error
        )
      }
      if (props.mergeTargetAlias.getOrElse("").trim.isEmpty) {
        diagnostics += Diagnostic(
          "properties.mergeTargetAlias",
          "Target Alias can not be empty [Properties]",
          SeverityLevel.Error
        )
      }

      // Both matched and notmatched action cannot be "ignore" simultaneously
      if (
        props.matchedAction.contains("ignore") && props.notMatchedAction.contains("ignore") && props.matchedActionDelete
          .contains("ignore")
      ) {
        diagnostics += Diagnostic(
          "properties.matchedAction",
          """At least one custom clauses ("When matched" or "When not matched") has to be enabled. [Properties]""",
          SeverityLevel.Error
        )
      }

      // validate matched action tab: exptable and matchedCondition
      if (
        props.matchedAction.contains("update") &&
          (props.matchedCondition.isDefined && component.properties.matchedCondition.get.expression.isEmpty)
      ) {
        diagnostics += Diagnostic(
          "properties.matchedCondition",
          s"Unsupported expression ${component.properties.matchedCondition.get.expression} [Properties]",
          SeverityLevel.Error
        )
      }

      if (
        props.matchedAction.contains("delete") &&
          (props.matchedConditionDelete.isDefined && component.properties.matchedConditionDelete.get.expression.isEmpty)
      ) {
        diagnostics += Diagnostic(
          "properties.matchedConditionDelete",
          s"Unsupported expression ${component.properties.matchedConditionDelete.get.expression} [Properties]",
          SeverityLevel.Error
        )
      }

      if (props.matchedAction.contains("update"))
        diagnostics ++= validateExpTable(props.matchedTable.get, "matchedTable")
          .map(_.appendMessage("[When Matched]"))
      // validate not matched action Tab: exptable and notmatchedCondition
      if (props.notMatchedAction.contains("insert")) {
        if (props.notMatchedCondition.isDefined && component.properties.notMatchedCondition.get.expression.isEmpty) {
          diagnostics += Diagnostic(
            "properties.notMatchedCondition",
            s"Unsupported expression ${component.properties.notMatchedCondition.get.expression} [Properties]",
            SeverityLevel.Error
          )
        }
        diagnostics ++= validateExpTable(
          props.notMatchedTable.get,
          "notMatchedTable"
        ).map(_.appendMessage("[When Not Matched]"))
      }
    }

    // validation for when deltaSCD2 selected
    if (props.writeMode.contains("merge_scd2")) {
      if (props.keyColumns.isEmpty)
        diagnostics += Diagnostic("properties.keyColumns", "Select at least one key column.", SeverityLevel.Error)
      if (props.historicColumns.isEmpty)
        diagnostics += Diagnostic(
          "properties.historicColumns",
          "Historic Columns cannot be empty.",
          SeverityLevel.Error
        )
      if (props.fromTimeCol.isEmpty)
        diagnostics += Diagnostic("properties.fromTimeCol", "fromTimeCol cannot be empty", SeverityLevel.Error)
      if (props.toTimeCol.isEmpty)
        diagnostics += Diagnostic("properties.toTimeCol", "toTimeCol cannot be empty", SeverityLevel.Error)
      if (props.minFlagCol.isEmpty)
        diagnostics += Diagnostic("properties.minFlagCol", "minFlagCol cannot be empty", SeverityLevel.Error)
      if (props.maxFlagCol.isEmpty)
        diagnostics += Diagnostic("properties.maxFlagCol", "maxFlagCol cannot be empty", SeverityLevel.Error)
    }

    diagnostics.toList
  }

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = {
    val newProps = newState.properties
    if (newProps.writeMode.contains("merge")) {
      val (cleanMatchedCondition: Option[SColumn], cleanMatchedTable: Option[List[SColumnExpression]]) =
        if (newProps.matchedAction.contains("update")) {
          (newProps.matchedCondition, newProps.matchedTable)
        } else {
          (None, Some(Nil))
        }

      val cleanMatchedConditionDelete =
        if (newProps.matchedActionDelete.contains("delete")) {
          newProps.matchedConditionDelete
        } else {
          None
        }

      val (cleanNotMatchedCondition: Option[SColumn], cleanNotMatchedTable: Option[List[SColumnExpression]]) =
        if (newProps.notMatchedAction.contains("ignore")) {
          (None, Nil)
        } else {
          (newProps.notMatchedCondition, newProps.notMatchedTable)
        }

      newState.copy(properties =
        newProps.copy(
          matchedCondition = cleanMatchedCondition,
          matchedTable = cleanMatchedTable,
          matchedConditionDelete = cleanMatchedConditionDelete,
          notMatchedCondition = cleanNotMatchedCondition,
          notMatchedTable = cleanNotMatchedTable
        )
      )
    } else
      newState.copy(properties =
        newProps.copy(
          mergeCondition = None,
          matchedCondition = None,
          notMatchedCondition = None,
          matchedTable = Some(Nil),
          matchedConditionDelete = None,
          notMatchedTable = Some(Nil)
        )
      )
  }

  override def deserializeProperty(props: String): PropertiesType = Json.parse(props).as[PropertiesType]

  override def serializeProperty(props: PropertiesType): String = Json.stringify(Json.toJson(props))

  class CatalogTableFormatCode(props: CatalogTableProperties) extends ComponentCode {

    def sourceApply(spark: SparkSession): DataFrame = {

      val tableName = if (props.isCatalogEnabled.isDefined && props.isCatalogEnabled.get) {
        s"`${props.catalog.get}`.`${props.path}`.`${props.tableName}`"
      } else {
        s"`${props.path}`.`${props.tableName}`"
      }

      if (props.provider.nonEmpty) {
        if (props.provider.get == "delta") {
          if (props.filterQuery.nonEmpty) {
            if (props.versionAsOf.nonEmpty) {
              spark.sql(
                "SELECT * FROM " + tableName + s" VERSION AS OF ${props.versionAsOf.get} WHERE ${props.filterQuery.get}"
              )
            } else if (props.timestampAsOf.nonEmpty) {
              spark.sql(
                "SELECT * FROM " + tableName + s" TIMESTAMP AS OF '${props.timestampAsOf.get}' WHERE ${props.filterQuery.get}"
              )
            } else {
              spark.sql("SELECT * FROM " + tableName + s" WHERE ${props.filterQuery.get}")
            }
          } else if (props.versionAsOf.nonEmpty) {
            spark.sql("SELECT * FROM " + tableName + s" VERSION AS OF ${props.versionAsOf.get}")
          } else if (props.timestampAsOf.nonEmpty) {
            spark.sql("SELECT * FROM " + tableName + s" TIMESTAMP AS OF '${props.timestampAsOf.get}'")
          } else {
            spark.read.table(tableName)
          }
        } else if (props.provider.get == "hive") {
          if (props.filterQuery.nonEmpty) {
            spark.sql("SELECT * FROM " + tableName + s" WHERE ${props.filterQuery.get}")
          } else {
            spark.read.table(tableName)
          }
        } else {
          spark.read.table(tableName)
        }
      } else {
        if (props.filterQuery.nonEmpty) {
          spark.sql("SELECT * FROM " + tableName + s" WHERE ${props.filterQuery.get}")
        } else {
          spark.read.table(tableName)
        }
      }

    }

    def targetApply(spark: SparkSession, in: DataFrame): Unit = {

      val tableName = if (props.isCatalogEnabled.isDefined && props.isCatalogEnabled.get) {
        s"`${props.catalog.get}`.`${props.path}`.`${props.tableName}`"
      } else {
        s"`${props.path}`.`${props.tableName}`"
      }

      val tableExists = spark.catalog.tableExists(tableName)

      if (props.writeMode.contains("merge") && tableExists) {
        import _root_.io.delta.tables._
        val dt = DeltaTable
          .forName(tableName)
          .as(props.mergeTargetAlias.get)
          .merge(in.as(props.mergeSourceAlias.get), props.mergeCondition.get.column)

        val resMatchedDelete: DeltaMergeBuilder = props.matchedActionDelete match {
          case None ⇒ dt
          case Some(action) ⇒
            action match {

              case "delete" ⇒
                props.matchedConditionDelete match {
                  case None ⇒ dt.whenMatched().delete()
                  case Some(value) ⇒ dt.whenMatched(value.column).delete()
                }

              case "ignore" ⇒ dt
            }
        }
        val resMatchedUpdate: DeltaMergeBuilder = props.matchedAction match {
          case None ⇒ resMatchedDelete
          case Some(action) ⇒
            action match {

              case "update" ⇒
                val matchedWithCondition = props.matchedCondition match {
                  case None ⇒ resMatchedDelete.whenMatched()
                  case Some(value) ⇒ resMatchedDelete.whenMatched(value.column)
                }

                props.matchedTable.get match {
                  case Nil ⇒ matchedWithCondition.updateAll()
                  case _ ⇒
                    val updateExprs = props.matchedTable.get.map(x ⇒ (x.target → x.unaliasedColumn)).toMap
                    matchedWithCondition.update(updateExprs)
                }

              case "ignore" ⇒ resMatchedDelete
            }
        }
        val res: DeltaMergeBuilder = props.notMatchedAction match {
          case None ⇒ resMatchedUpdate
          case Some(action) ⇒
            action match {
              case "insert" ⇒
                val notMatcheWithCond = props.notMatchedCondition match {
                  case None ⇒ resMatchedUpdate.whenNotMatched()
                  case Some(value) ⇒ resMatchedUpdate.whenNotMatched(value.column)
                }
                props.notMatchedTable.get match {
                  case Nil ⇒ notMatcheWithCond.insertAll()
                  case _ ⇒
                    val insertExprs = props.notMatchedTable.get.map(x ⇒ (x.target → x.unaliasedColumn)).toMap
                    notMatcheWithCond.insert(insertExprs)
                }
              case "ignore" ⇒ resMatchedUpdate
            }
        }
        res.execute()
      } else if (props.writeMode.contains("merge_scd2") && tableExists) {
        import _root_.io.delta.tables._
        val keyColumns = props.keyColumns
        val scdHistoricColumns = props.historicColumns
        val fromTimeColumn = props.fromTimeCol.get
        val toTimeColumn = props.toTimeCol.get
        val minFlagColumn = props.minFlagCol.get
        val maxFlagColumn = props.maxFlagCol.get
        val (flagY, flagN) = props.flagValue.get match {
          case "integer" ⇒ ("1", "0")
          case "boolean" ⇒ ("true", "false")
        }

        val updatesDF = in.withColumn(minFlagColumn, lit(flagY)).withColumn(maxFlagColumn, lit(flagY))
        val updateColumns: Array[String] = updatesDF.columns

        val existingTable: DeltaTable = DeltaTable.forName(tableName)
        val existingDF: DataFrame = existingTable.toDF

        val rowsToUpdate = updatesDF
          .join(existingDF, keyColumns.get)
          .where(
            existingDF.col(maxFlagColumn) === lit(flagY) && (
              scdHistoricColumns.get
                .map(scdCol ⇒ !existingDF.col(scdCol).eqNullSafe(updatesDF.col(scdCol)))
                .reduce((c1, c2) ⇒ c1 || c2)
              )
          )
          .select(updateColumns.map(x ⇒ updatesDF.col(x)): _*)
          .withColumn(minFlagColumn, lit(flagN))

        val stagedUpdatesDF = rowsToUpdate
          .withColumn("mergeKey", lit(null))
          .union(updatesDF.withColumn("mergeKey", concat(keyColumns.get.map(x ⇒ col(x)): _*)))

        existingTable
          .as("existingTable")
          .merge(
            stagedUpdatesDF.as("staged_updates"),
            concat(keyColumns.get.map(x ⇒ existingDF.col(x)): _*).eqNullSafe(stagedUpdatesDF("mergeKey"))
          )
          .whenMatched(
            existingDF.col(maxFlagColumn) === lit(flagY) && (
              scdHistoricColumns.get
                .map(scdCol ⇒ !existingDF.col(scdCol).eqNullSafe(stagedUpdatesDF.col(scdCol)))
                .reduce((c1, c2) ⇒ c1 || c2)
              )
          )
          .updateExpr(
            Map(
              maxFlagColumn → flagN,
              toTimeColumn → ("staged_updates." ++ fromTimeColumn)
            )
          )
          .whenNotMatched()
          .insertAll()
          .execute()
      } else {
        var writer = in.write
          .format(props.provider.get)
          .option("optimizeWrite", props.optimizeWrite)
          .option("mergeSchema", props.mergeSchema)
          .option("replaceWhere", props.replaceWhere)
          .option("overwriteSchema", props.overwriteSchema)
        if (props.provider.contains("hive")) {
          props.fileFormat.foreach(fileFormat ⇒ writer = writer.option("fileFormat", fileFormat))
        }

        props.writeMode.foreach { mode ⇒
          if (props.writeMode.contains("merge") || props.writeMode.contains("merge_scd2")) {
            writer = writer.mode("overwrite")
          } else {
            writer = writer.mode(mode)
          }
        }

        if (props.useExternalFilePath.contains(true)) {
          writer = writer.option("path", props.externalFilePath.get.trim)
        }
        props.partitionColumns.foreach(pcols ⇒
          writer = pcols match {
            case Nil ⇒ writer
            case _ ⇒ writer.partitionBy(pcols: _*)
          }
        )

        if (props.insertInto.contains(true)) {
          writer.insertInto(tableName)
        } else {
          writer.saveAsTable(tableName)
        }
      }

    }

  }

}
