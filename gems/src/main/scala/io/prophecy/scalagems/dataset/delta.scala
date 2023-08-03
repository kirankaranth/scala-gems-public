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

class delta extends DatasetSpec {

  val name: String = "delta"
  val datasetType: String = "File"
  val docUrl: String = "https://docs.prophecy.io/low-code-spark/gems/source-target/file/delta"

  type PropertiesType = DeltaProperties
  case class DeltaProperties(
    @Property("Schema")
    schema: Option[StructType] = None,
    @Property("Description")
    description: Option[String] = Some(""),
    @Property("Expectations")
    expectations: Option[String] = Some("""{"version":"1.0","expectations":[]}"""),
    @Property("Path")
    path: String = "",
    @Property(
      "timestampAsOf",
      "read a table snapshot as of given timestamp, for timestamp only data or timestamp strings are accepted ex. \"2019-01-01\" and \"2019-01-01T00:00:00.000Z\" "
    )
    timestampAsOf: Option[String] = None,
    @Property("versionAsOf", "select a table snapshot of given version")
    versionAsOf: Option[String] = None,
    @Property("Write Mode", """(default: "error") Specifies the behavior when data or table already exists.""")
    writeMode: Option[String] = None,
    @Property("partitionColumns", """(default: "") Partitioning column.""")
    partitionColumns: Option[List[String]] = None,
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
    matchedTable: List[SColumnExpression] = Nil,
    @Property("", "")
    notMatchedTable: List[SColumnExpression] = Nil,
    @Property("", "")
    keyColumns: List[String] = Nil,
    @Property("", "")
    historicColumns: List[String] = Nil,
    @Property("", "")
    fromTimeCol: Option[String] = None,
    @Property("", "")
    toTimeCol: Option[String] = None,
    @Property("", "")
    minFlagCol: Option[String] = None,
    @Property("", "")
    maxFlagCol: Option[String] = None,
    @Property("", "")
    flagValue: Option[String] = Some("integer")
  ) extends DatasetProperties
  
  implicit val deltaPropertiesFormat: Format[DeltaProperties] = Jsonx.formatCaseClass[DeltaProperties]
  def sourceDialog: DatasetDialog = DatasetDialog("delta")
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
                  .addField(TextBox("Read timestamp").bindPlaceholder(""), "timestampAsOf")
                  .addField(TextBox("Read version").bindPlaceholder(""), "versionAsOf")
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

  def targetDialog: DatasetDialog = {
    val matchedTable = ExpTable("Expressions")
      .bindProperty("matchedTable")
    val notMatchedTable = ExpTable("Expressions")
      .bindProperty("notMatchedTable")

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
    DatasetDialog("delta")
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
                    )
                )
            ),
            "auto"
          )
          .addColumn(
            Condition()
              .ifEqual(PropExpr("component.properties.writeMode"), StringExpr("merge"))
              .then(mergeView)
              .otherwise(
                Condition()
                  .ifEqual(PropExpr("component.properties.writeMode"), StringExpr("merge_scd2"))
                  .then(scd2View)
                  .otherwise(SchemaTable("").isReadOnly().withoutInferSchema().bindProperty("schema"))
              ),
            "5fr"
          )
      )
  }

  override def validate(component: Component)(implicit context: WorkflowContext): List[Diagnostic] = {
    import scala.collection.mutable.ListBuffer
    val diagnostics = ListBuffer[Diagnostic]()
    diagnostics ++= super.validate(component)

    println("delta validate component: ", component)

    val props = component.properties

    if (props.path.isEmpty) {
      diagnostics += Diagnostic("properties.path", "path variable cannot be empty [Location]", SeverityLevel.Error)
    }
    if (props.schema.isEmpty) {
      //      diagnostics += Diagnostic("properties.schema", "Schema cannot be empty [Properties]", SeverityLevel.Error)
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
        diagnostics ++= validateExpTable(props.matchedTable, "matchedTable")
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
          props.notMatchedTable,
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
      val (cleanMatchedCondition, cleanMatchedTable) =
        if (newProps.matchedAction.contains("update")) {
          (newProps.matchedCondition, newProps.matchedTable)
        } else {
          (None, Nil)
        }

      val cleanMatchedConditionDelete =
        if (newProps.matchedActionDelete.contains("delete")) {
          newProps.matchedConditionDelete
        } else {
          None
        }

      val (cleanNotMatchedCondition, cleanNotMatchedTable) =
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
          matchedTable = Nil,
          matchedConditionDelete = None,
          notMatchedTable = Nil
        )
      )

  }

  override def deserializeProperty(props: String): PropertiesType = Json.parse(props).as[PropertiesType]

  override def serializeProperty(props: PropertiesType): String = Json.stringify(Json.toJson(props))

  class DeltaFormatCode(props: DeltaProperties) extends ComponentCode {

    def sourceApply(spark: SparkSession): DataFrame = {
      var reader = spark.read
        .format("delta")
        .option("versionAsOf", props.versionAsOf)
        .option("timestampAsOf", props.timestampAsOf)

      reader.load(props.path)

    }

    def targetApply(spark: SparkSession, in: DataFrame): Unit = {
      import _root_.io.delta.tables._
      import org.apache.hadoop.fs.{FileSystem, Path}

      val filePath = new Path(props.path)
      val srcFs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

      val tableExists = DeltaTable.isDeltaTable(spark, props.path) && srcFs.exists(filePath)

      if (props.writeMode.contains("merge") && tableExists) {
        val dt = DeltaTable
          .forPath(spark, props.path)
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

                props.matchedTable match {
                  case Nil ⇒ matchedWithCondition.updateAll()
                  case _ ⇒
                    val updateExprs = props.matchedTable.map(x ⇒ (x.target → x.unaliasedColumn)).toMap
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
                props.notMatchedTable match {
                  case Nil ⇒ notMatcheWithCond.insertAll()
                  case _ ⇒
                    val insertExprs = props.notMatchedTable.map(x ⇒ (x.target → x.unaliasedColumn)).toMap
                    notMatcheWithCond.insert(insertExprs)
                }
              case "ignore" ⇒ resMatchedUpdate
            }
        }
        res.execute()
      } else if (props.writeMode.contains("merge_scd2") && tableExists) {

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

        val existingTable: DeltaTable = DeltaTable.forPath(spark, props.path)
        val existingDF: DataFrame = existingTable.toDF

        val rowsToUpdate = updatesDF
          .join(existingDF, keyColumns)
          .where(
            existingDF.col(maxFlagColumn) === lit(flagY) && (
              scdHistoricColumns
                .map(scdCol ⇒ !existingDF.col(scdCol).eqNullSafe(updatesDF.col(scdCol)))
                .reduce((c1, c2) ⇒ c1 || c2)
            )
          )
          .select(updateColumns.map(x ⇒ updatesDF.col(x)): _*)
          .withColumn(minFlagColumn, lit(flagN))

        val stagedUpdatesDF = rowsToUpdate
          .withColumn("mergeKey", lit(null))
          .union(updatesDF.withColumn("mergeKey", concat(keyColumns.map(x ⇒ col(x)): _*)))

        existingTable
          .as("existingTable")
          .merge(
            stagedUpdatesDF.as("staged_updates"),
            concat(keyColumns.map(x ⇒ existingDF.col(x)): _*).eqNullSafe(stagedUpdatesDF("mergeKey"))
          )
          .whenMatched(
            existingDF.col(maxFlagColumn) === lit(flagY) && (
              scdHistoricColumns
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
          .format("delta")
          .option("optimizeWrite", props.optimizeWrite)
          .option("mergeSchema", props.mergeSchema)
          .option("replaceWhere", props.replaceWhere)
          .option("overwriteSchema", props.overwriteSchema)
        props.writeMode.foreach { mode ⇒
          if (props.writeMode.contains("merge") || props.writeMode.contains("merge_scd2")) {
            writer = writer.mode("overwrite")
          } else {
            writer = writer.mode(mode)
          }
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
  }
}
