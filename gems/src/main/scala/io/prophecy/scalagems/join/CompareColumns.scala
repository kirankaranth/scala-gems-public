package io.prophecy.scalagems.join

import io.prophecy.gems._
import io.prophecy.gems.componentSpec._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.uiSpec._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{coalesce, col, explode_outer, first, lit, map, row_number, sum, when}
import play.api.libs.json.{Format, Json}

class CompareColumns extends ComponentSpec {

  val name: String = "CompareColumns"
  val category: String = "Join/Split"
  type PropertiesType = CompareColumnsProperties

  override def optimizeCode: Boolean = true

  // todo: columnSelector property can be hidden from user
  // and move column select to the ports
  case class CompareColumnsProperties(
    @Property("Columns selector")
    columnsSelector: List[String] = Nil,
    @Property("ID columns", "List of all the selected ID columns")
    idColumns: List[SColumn] = Nil,
    @Property(
      "Output Column Name",
      "Name of output column which will contain the input column names"
    )
    columnName: String = "column_name",
    @Property(
      "Match Count Column Name",
      "Name of output column which will contain the number of matches for a given column"
    )
    matchCount: String = "match_count",
    @Property(
      "Mismatch Count Column Name",
      "Name of output column which will contain the number of mismatches for a given column"
    )
    mismatchCount: String = "mismatch_count",
    @Property(
      "Mismatch Example Left Column Name",
      "Name of output column which will contain an example mismatch from the left input (or null for no example)"
    )
    mismatchExampleLeft: String = "mismatch_example_left",
    @Property(
      "Mismatch Example Right Column Name",
      "Name of output column which will contain an example mismatch from the right input (or null for no example)"
    )
    mismatchExampleRight: String = "mismatch_example_right",
    @Property(
      "Mismatch Example ID Column Prefix",
      "Prefix of id columns containing the ids of example mismatches (or null for no example)"
    )
    mismatchIdColumnPrefix: String = "mismatch_example_",
    @Property(
      "selected tab",
      "selected tab"
    )
    activeTab: Option[String] = Some("id_columns")
  ) extends ComponentProperties
  //  override def validate(component: CompareColumns.Component)(implicit context: WorkflowContext): Diagnostics = Nil

  implicit val compareColumnsFormat: Format[CompareColumnsProperties] = Json.format
  def validate(
    component: Component
  )(implicit context: WorkflowContext): List[Diagnostic] = {
    import scala.collection.mutable.ListBuffer

    val diagnostics = ListBuffer[Diagnostic]()
    if (component.ports.inputs.size < 2) {
      diagnostics += Diagnostic(
        "ports.inputs",
        "Input ports can't be less than two in CompareColumns component",
        SeverityLevel.Error
      )
    }
    if (component.properties.idColumns.isEmpty) {
      diagnostics += Diagnostic(
        s"properties.idColumns",
        "Must specify at least 1 ID column",
        SeverityLevel.Error
      )
    }

    if (component.ports.inputs.size == 2) {

      val leftFields =
        component.ports.inputs.head.schema.toList
          .flatMap(s ⇒ s.fields.map(f ⇒ (f.name, f.dataType)))
          .toSet
      val rightFields =
        component.ports.inputs.tail.head.schema.toList
          .flatMap(s ⇒ s.fields.map(f ⇒ (f.name, f.dataType)))
          .toSet
      if (leftFields != rightFields) {
        diagnostics += Diagnostic(
          s"ports.inputs",
          "Input schema mismatch: Inputs must have the same schema",
          SeverityLevel.Error
        )
      }
    }
    val props = component.properties
    val parameterColumns = Seq(
      (props.columnName, s"properties.columnName"),
      (props.matchCount, s"properties.matchCount"),
      (props.mismatchCount, s"properties.mismatchCount"),
      (props.mismatchExampleLeft, s"properties.mismatchExampleLeft"),
      (props.mismatchExampleRight, s"properties.mismatchExampleRight")
    )
    if (parameterColumns.map(_._1).toSet.size != parameterColumns.size) {
      val dupes = parameterColumns.groupBy(x ⇒ x._1).values.filter(_.size > 1)
      dupes.foreach { dupeSet ⇒
        dupeSet.foreach { dupe ⇒
          val (name, prop) = dupe
          diagnostics += Diagnostic(
            prop,
            s"Duplicate column name parameter: ${name}",
            SeverityLevel.Error
          )
        }
      }
    }
    diagnostics.toList
  }

  override def onChange(
    oldState: Component,
    newState: Component
  )(implicit
    context: WorkflowContext
  ): Component = newState

  //  def onChange(oldState: Component, newState: Component): Component = newState

  def dialog: Dialog = {

    val idColumnTable =
      BasicTable(
        "Test",
        columns = List(
          io.prophecy.gems.uiSpec.Column(
            "ID columns to retain",
            "expression",
            Some(
              TextBox(title = "", ignoreTitle = true)
            )
          )
        )
      ).bindProperty("idColumns")

    Dialog("Compare Columns")
      .addElement(
        ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
          .addColumn(
            PortSchemaTabs(
              selectedFieldsProperty = Some("columnsSelector"),
              singleColumnClickCallback = Some { (portId: String, column: String, state: Any) ⇒
                val st = state.asInstanceOf[Component]
                st.copy(properties =
                  st.properties.copy(
                    columnsSelector = st.properties.columnsSelector ::: List(
                      s"$portId##$column"
                    ),
                    idColumns = st.properties.idColumns ::: SColumn(
                      s"""col("$column")"""
                    ) :: Nil
                  )
                )
              }
            ).importSchema()
          )
          .addColumn(
            Tabs()
              .bindProperty("activeTab")
              .addTabPane(
                TabPane("Select Id Columns", "id_columns")
                  .addElement(
                    idColumnTable
                  )
              )
              .addTabPane(
                TabPane("Select Output Columns", "output_columns").addElement(
                  StackLayout(height = Some("100%"))
                    .addElement(
                      TextBox("Output Column Name")
                        .bindPlaceholder("column_name")
                        .bindProperty("columnName")
                    )
                    .addElement(
                      TextBox("Match Count Column Name")
                        .bindPlaceholder("match_count")
                        .bindProperty("matchCount")
                    )
                    .addElement(
                      TextBox("Mismatch Count Column Name")
                        .bindPlaceholder("mismatch_count")
                        .bindProperty("mismatchCount")
                    )
                    .addElement(
                      TextBox("Mismatch Example Left Column Name")
                        .bindPlaceholder("mismatch_example_left")
                        .bindProperty("mismatchExampleLeft")
                    )
                    .addElement(
                      TextBox("Mismatch Example Right Column Name")
                        .bindPlaceholder("mismatch_example_right")
                        .bindProperty("mismatchExampleRight")
                    )
                    .addElement(
                      TextBox("Mismatch Example ID Column Prefix")
                        .bindPlaceholder("mismatch_example_")
                        .bindProperty("mismatchIdColumnPrefix")
                    )
                )
              ),
            "2fr"
          )
      )
  }

  override def deserializeProperty(props: String): PropertiesType = Json.parse(props).as[PropertiesType]

  override def serializeProperty(props: PropertiesType): String = Json.stringify(Json.toJson(props))

  class CompareColumnsCode(props: PropertiesType)(implicit
    context: WorkflowContext
  ) extends ComponentCode {

    def apply(
      spark: SparkSession,
      in0: DataFrame,
      in1: DataFrame
    ): DataFrame = {
      import org.apache.spark.sql.expressions.Window

      val idCols = props.idColumns.map(x => col((x.column).toString()))
      val idColumns = idCols.map(_.toString())

      val columnName = props.columnName
      val matchCount = props.matchCount
      val mismatchCount = props.mismatchCount
      val mismatchExampleLeft = props.mismatchExampleLeft
      val mismatchExampleRight = props.mismatchExampleRight
      val mismatchExampleIdPrefix = props.mismatchIdColumnPrefix

      val df1 = in0
      val df2 = in1

      val (value, left_value, right_value, row_number_column_name) =
        ("##value##", "##left_value##", "##right_value##", "##row_number###")

      val valueColumns = (df1.columns.toSet -- idColumns.toSet).toSeq

      val valueColumnsMap =
        valueColumns.flatMap(c ⇒ Seq(lit(c), col(c).cast("string")))
      val selectCols = idCols ++ Seq(
        explode_outer(map(valueColumnsMap: _*)).as(Seq(columnName, value))
      )

      val exploded1 = df1.select(selectCols: _*).as("exploded1")
      val exploded2 = df2.select(selectCols: _*).as("exploded2")

      val joinCols = columnName :: idColumns
      val joinOutputCols = joinCols.map(c ⇒ coalesce(col(s"exploded1." + c), col(s"exploded2." + c)).as(c)) ++ Seq(
        col(s"exploded1." + value).as(left_value),
        col(s"exploded2." + value).as(right_value)
      )
      val joined = exploded1
        .join(
          exploded2,
          joinCols.foldLeft(lit(true)) {
            case (predicate, column) ⇒
              predicate.and(
                col(s"exploded1." + column) === col(s"exploded2." + column)
              )
          },
          "full_outer"
        )
        .select(joinOutputCols: _*)
        .withColumn(
          matchCount,
          when(
            coalesce(
              col(left_value) === col(right_value),
              col(left_value).isNull && col(right_value).isNull
            ),
            lit(1)
          )
            .otherwise(lit(0))
        )
        .withColumn(
          mismatchCount,
          when(
            coalesce(
              col(left_value) =!= col(right_value),
              !(col(left_value).isNull && col(right_value).isNull)
            ),
            lit(1)
          )
            .otherwise(lit(0))
        )

      val windowSpec =
        Window.partitionBy(joinCols.map(c ⇒ col(c)): _*).orderBy(idCols: _*)
      val exampleCols = joinCols.map(c ⇒ col(c)) ++ Seq(
        lit(0).as(matchCount),
        lit(0).as(mismatchCount),
        col(left_value).as(mismatchExampleLeft),
        col(right_value).as(mismatchExampleRight)
      )
      val mismatchExamples = joined
        .filter(col(mismatchCount).gt(lit(0)))
        .withColumn(row_number_column_name, row_number.over(windowSpec))
        .filter(col(row_number_column_name) === lit(1))
        .select(
          exampleCols: _*
        )
        .dropDuplicates(columnName)

      val aggColumns = List(
        sum(matchCount).as(matchCount),
        sum(mismatchCount).as(mismatchCount),
        first(col(mismatchExampleLeft), ignoreNulls = true)
          .as(mismatchExampleLeft),
        first(col(mismatchExampleRight), ignoreNulls = true)
          .as(mismatchExampleRight)
      ) ++ idColumns.map(c ⇒
        first(
          when(
            coalesce(
              col(mismatchExampleLeft),
              col(mismatchExampleRight)
            ).isNotNull,
            col(c)
          )
            .otherwise(lit(null)),
          ignoreNulls = true
        ).as(mismatchExampleIdPrefix + c)
      )

      val output = joined
        .drop(left_value)
        .drop(right_value)
        .withColumn(mismatchExampleLeft, lit(null))
        .withColumn(mismatchExampleRight, lit(null))
        .union(mismatchExamples)
        .groupBy(columnName)
        .agg(aggColumns.head, aggColumns.tail: _*)
        .orderBy(col(mismatchCount).desc, col(columnName))

      output
    }
  }

}
