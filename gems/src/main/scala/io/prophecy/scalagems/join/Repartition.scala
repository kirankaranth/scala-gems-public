package io.prophecy.scalagems.join

import io.prophecy.gems._
import io.prophecy.gems.componentSpec._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.uiSpec._
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.{Format, Json}

class Repartition extends ComponentSpec {

  val name: String = "Repartition"
  val category: String = "Join/Split"
  val gemDescription: String =
    "Splits a Dataframe across workers using Repartition or Coalesce."
  val docUrl: String = "https://docs.prophecy.io/low-code-spark/gems/join-split/Repartition"

  type PropertiesType = RepartitionProperties
  override def optimizeCode: Boolean = true

  // todo @ank to enable placeholders, use trait-structure. Current structure will create props which make the state
  //  invalid but cant be detected in validate() method
  //  Eg, for coalesce, it wont check if `rangeExpressions` and `hashExpressions` are empty or not. And the failure
  //  would pop in transpilation, when it tries to convert those empty (and therefore invalid) expressions.
  case class RepartitionProperties(
    @Property("Columns selector")
    columnsSelector: List[String] = Nil,
    @Property("Repartition Type", "")
    repartitionType: String = "coalesce",
    @Property("npartitions", "")
    nPartitions: Option[SInt] = Some(SInt("10")),
    @Property("default partitions", "")
    defaultPartitions: Option[SInt] = None,
    @Property("overwrite", "")
    overwriteDefaultNPartitions: Boolean = false,
    @Property("Order By Rules", "List of conditions to order dataframes on")
    hashExpressions: List[SColumn] = Nil,
    @Property("Order By Rules", "List of conditions to order dataframes on")
    rangeExpressions: List[RangeRepartitionExpression] = Nil
  ) extends ComponentProperties

  @Property("Range Expression Helper")
  case class RangeRepartitionExpression(
    @Property("Full expression") expression: SColumn,
    @Property("Sort") sortType: String
  )

  implicit val rangeRepartitionExpressionFormat: Format[RangeRepartitionExpression] = Json.format
  implicit val repartitionPropertiesFormat: Format[RepartitionProperties] = Json.format

  def dialog: Dialog = {
    val selectBox = RadioGroup("")
      .addOption(
        "Coalesce",
        "coalesce",
        description = Some("Reduces the number of partitions without shuffling the dataset.")
      )
      .addOption(
        "Random Repartitioning",
        "random_repartitioning",
        description = Some("Repartitions without data distribution defined. Reshuffles the dataset.")
      )
      .addOption(
        "Hash Repartitioning",
        "hash_repartitioning",
        description = Some(
          "Repartitions, spreading the data evenly across various partitions based on the key. Reshuffles the dataset."
        )
      )
      .addOption(
        "Range Repartitioning",
        "range_repartitioning",
        description =
          Some("Repartitions, spreading the data with tuples having keys within the same range on the the same worker. Reshuffles the dataset.")
      )
      .setOptionType("button")
      .setVariant("medium")
      .setButtonStyle("solid")
      .bindProperty("repartitionType")

    val sortSelectBox = SelectBox("")
      .addOption("Ascending", "asc")
      .addOption("Descending", "desc")

    val numberOfPartitions = ExpressionBox("Number of Partitions")
      .bindPlaceholder("10")
      .bindProperty("nPartitions")
      .withFrontEndLanguage

    val defaultPartitions = ExpressionBox("Number of Partitions")
      .bindPlaceholder("spark.sql.shuffle.partitions")
      .bindProperty("defaultPartitions")
      .withFrontEndLanguage

    val coalesceView = Condition()
      .ifEqual(PropExpr("component.properties.repartitionType"), StringExpr("coalesce"))
      .then(numberOfPartitions)
    val randomRepartitionView = Condition()
      .ifEqual(PropExpr("component.properties.repartitionType"), StringExpr("random_repartitioning"))
      .then(numberOfPartitions)

    val hashRepartitionView =
      Condition()
        .ifEqual(PropExpr("component.properties.repartitionType"), StringExpr("hash_repartitioning"))
        .then(
          StackLayout()
            .addElement(
              ColumnsLayout()
                .addColumn(
                  Checkbox("Overwrite default number of partitions")
                    .bindProperty("overwriteDefaultNPartitions")
                )
                .addColumn(
                  Condition()
                    .ifEqual(PropExpr("component.properties.overwriteDefaultNPartitions"), BooleanExpr(true))
                    .then(defaultPartitions)
                    .otherwise(defaultPartitions.disabled())
                )
            )
            .addElement(
              BasicTable(
                "Test",
                height = Some("400px"),
                columns = List(
                  Column(
                    "Repartition Expression",
                    "expression",
                    Some(
                      ExpressionBox(ignoreTitle = true)
                        .bindLanguage("${record.format}")
                        .bindPlaceholders()
                        .withSchemaSuggestions()
                    )
                  )
                )
              ).bindProperty("hashExpressions")
            )
        )
    val rangeRepartitionView =
      Condition()
        .ifEqual(
          PropExpr("component.properties.repartitionType"),
          StringExpr("range_repartitioning")
        )
        .then(
          StackLayout()
            .addElement(
              ColumnsLayout()
                .addColumn(
                  Checkbox("Overwrite default number of partitions")
                    .bindProperty("overwriteDefaultNPartitions")
                )
                .addColumn(
                  Condition()
                    .ifEqual(PropExpr("component.properties.overwriteDefaultNPartitions"), BooleanExpr(true))
                    .then(defaultPartitions)
                    .otherwise(defaultPartitions.disabled())
                )
            )
            .addElement(
              BasicTable(
                "Test",
                height = Some("400px"),
                columns = Column(
                  "Repartition Expression",
                  "expression.expression",
                  Some(
                    ExpressionBox(ignoreTitle = true)
                      .bindLanguage("${record.expression.format}")
                      .bindPlaceholders()
                      .withSchemaSuggestions()
                  )
                )
                  :: Column("Sort", "sortType", Some(sortSelectBox), width = "25%") :: Nil
              ).bindProperty("rangeExpressions")
            )
        )

    Dialog("Repartition")
      .addElement(
        ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
          .addColumn(PortSchemaTabs().importSchema())
          .addColumn(
            StackLayout()
              .addElement(selectBox)
              .addElement(coalesceView)
              .addElement(randomRepartitionView)
              .addElement(hashRepartitionView)
              .addElement(rangeRepartitionView),
            "2fr"
          )
      )
  }

  def validate(component: Component)(implicit context: WorkflowContext): List[Diagnostic] = {
    import scala.collection.mutable.ListBuffer
    val diagnostics = ListBuffer[Diagnostic]()

    component.properties.repartitionType match {
      case x if x == "hash_repartitioning" || x == "range_repartitioning" ⇒
        if (component.properties.overwriteDefaultNPartitions) {
          if (component.properties.defaultPartitions.isEmpty)
            diagnostics += Diagnostic(
              "properties.defaultPartitions",
              "An Integer value has to be specified in 'Number of Partitions'",
              SeverityLevel.Error
            )
          else {
            diagnostics ++= component.properties.defaultPartitions.get.diagnostics
          }
        }
        if (x == "hash_repartitioning") {
          if (component.properties.hashExpressions.isEmpty)
            diagnostics += Diagnostic(
              s"properties.hashExpressions",
              "At least one expression has to be specified",
              SeverityLevel.Error
            )
          else {
            component.properties.hashExpressions.zipWithIndex.foreach {
              case (expr, idx) ⇒
                diagnostics ++= validateSColumn(
                  expr,
                  s"hashExpressions[$idx]",
                  component,
                  testColumnPresence = Some(ColumnsUsage.WithoutInputAlias)
                )
            }
          }
        } else {
          if (component.properties.rangeExpressions.isEmpty)
            diagnostics += Diagnostic(
              s"properties.rangeExpressions",
              "At least one expression has to be specified",
              SeverityLevel.Error
            )
          else {
            component.properties.rangeExpressions.zipWithIndex.foreach {
              case (expr, idx) ⇒
                diagnostics ++= validateSColumn(
                  expr.expression,
                  s"rangeExpressions[$idx].expression",
                  component,
                  Some(ColumnsUsage.WithoutInputAlias)
                )
            }
          }
        }
      case _ ⇒
        if (component.properties.nPartitions.isDefined) {
          diagnostics ++= component.properties.nPartitions.get.diagnostics
        }
    }
    diagnostics.toList
  }

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = {
    val newProps = newState.properties
    val usedCols =
      getColumnsToHighlight(newProps.hashExpressions ++ newProps.rangeExpressions.map(_.expression), newState)
    newState.copy(properties = newProps.copy(columnsSelector = usedCols))
  }

  override def deserializeProperty(props: String): PropertiesType = Json.parse(props).as[PropertiesType]

  override def serializeProperty(props: PropertiesType): String = Json.stringify(Json.toJson(props))

  class RepartitionCode(props: PropertiesType) extends ComponentCode {

    def apply(spark: SparkSession, in: DataFrame): DataFrame = {
      val out = props.repartitionType match {
        case "coalesce" ⇒ in.coalesce(props.nPartitions.get)
        case "random_repartitioning" ⇒ in.repartition(props.nPartitions.get)
        case "hash_repartitioning" ⇒
          if (props.overwriteDefaultNPartitions)
            in.repartition(props.defaultPartitions.get.toInt, props.hashExpressions.map(_.column): _*)
          else
            in.repartition(props.hashExpressions.map(_.column): _*)
        case "range_repartitioning" ⇒
          val rangeExpressionRules = props.rangeExpressions.map(x ⇒
            x.sortType match {
              case "asc" ⇒ x.expression.column.asc
              case _ ⇒ x.expression.column.desc
            }
          )
          if (props.overwriteDefaultNPartitions)
            in.repartitionByRange(props.defaultPartitions.get.toInt, rangeExpressionRules: _*)
          else in.repartitionByRange(rangeExpressionRules: _*)
      }
      out
    }
  }
}
