package l_2

import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.ExplainMode
import org.apache.spark.sql.execution.command.ExplainCommand
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
import org.apache.spark.sql.functions.{lit, pmod}
import org.apache.spark.sql.types.IntegerType
import shared.SparkCommons

import java.lang

object Internals_1 extends SparkCommons with App {
  import spark.implicits._

  println(sc.master)
  println(sc.appName)
  println(sc.uiWebUrl.get)
  println(sc.version)

  /** Планы выполнения задач. */
  val df: Dataset[lang.Long] =
    spark
      .range(0, 10)
      .localCheckpoint

  val filteredDf: Dataset[lang.Long] = df.filter($"id" > 0)
  filteredDf.explain(true)
  /*
    == Parsed Logical Plan ==
    'Filter ('id > 0)
    +- LogicalRDD [id#0L], false

    == Analyzed Logical Plan ==
    id: bigint
    Filter (id#0L > cast(0 as bigint))
    +- LogicalRDD [id#0L], false

    == Optimized Logical Plan ==
    Filter (id#0L > 0)
    +- LogicalRDD [id#0L], false

    == Physical Plan ==
    *(1) Filter (id#0L > 0)
    +- *(1) Scan ExistingRDD[id#0L]
   */

  val groupedDf: DataFrame =
    df
      .filter($"id" > 0)
      .select(pmod($"id", lit(2)).as("mod2"))
      .groupBy($"mod2".cast(IntegerType).as("pmod2"))
      .count

  groupedDf.show(20, truncate = false)
  groupedDf.prettyExplain()
  groupedDf.explain(true)
  /*
    == Parsed Logical Plan ==
    'Aggregate [cast('mod2 as int) AS pmod2#11], [cast('mod2 as int) AS pmod2#11, count(1) AS count#14L]
    +- Project [pmod(id#0L, cast(2 as bigint)) AS mod2#9L]
       +- Filter (id#0L > cast(0 as bigint))
          +- LogicalRDD [id#0L], false

    == Analyzed Logical Plan ==
    pmod2: int, count: bigint
    Aggregate [cast(mod2#9L as int)], [cast(mod2#9L as int) AS pmod2#11, count(1) AS count#14L]
    +- Project [pmod(id#0L, cast(2 as bigint)) AS mod2#9L]
       +- Filter (id#0L > cast(0 as bigint))
          +- LogicalRDD [id#0L], false

    == Optimized Logical Plan ==
    Aggregate [_groupingexpression#29], [_groupingexpression#29 AS pmod2#11, count(1) AS count#14L]
    +- Project [cast(pmod(id#0L, 2) as int) AS _groupingexpression#29]
       +- Filter (id#0L > 0)
          +- LogicalRDD [id#0L], false

    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- HashAggregate(keys=[_groupingexpression#29], functions=[count(1)], output=[pmod2#11, count#14L])
       +- Exchange hashpartitioning(_groupingexpression#29, 200), ENSURE_REQUIREMENTS, [id=#83]
          // Агрегация внутри каждой партиции.
          +- HashAggregate(keys=[_groupingexpression#29], functions=[partial_count(1)], output=[_groupingexpression#29, count#25L])
             +- Project [cast(pmod(id#0L, 2) as int) AS _groupingexpression#29]
                +- Filter (id#0L > 0)
                   // сканирование rdd из кэша - т.к. был localCheckpoint
                   +- Scan ExistingRDD[id#0L]
   */

  /** Физические операторы из плана выше реализованы с помощью следующих классов: */
  /** package org.apache.spark.sql.execution */
  /*
    case class FilterExec(condition: Expression, child: SparkPlan)
      extends UnaryExecNode with CodegenSupport with GeneratePredicateHelper

    case class ProjectExec(projectList: Seq[NamedExpression], child: SparkPlan)
      extends UnaryExecNode
        with CodegenSupport
        with AliasAwareOutputPartitioning
        with AliasAwareOutputOrdering
   */

  /** package org.apache.spark.sql.execution.aggregate */
  /*
    case class ObjectHashAggregateExec(
        requiredChildDistributionExpressions: Option[Seq[Expression]],
        groupingExpressions: Seq[NamedExpression],
        aggregateExpressions: Seq[AggregateExpression],
        aggregateAttributes: Seq[Attribute],
        initialInputBufferOffset: Int,
        resultExpressions: Seq[NamedExpression],
        child: SparkPlan)
      extends BaseAggregateExec
   */

  /** package org.apache.spark.sql.execution.exchange */
  /*
    case class ShuffleExchangeExec(
        override val outputPartitioning: Partitioning,
        child: SparkPlan,
        shuffleOrigin: ShuffleOrigin = ENSURE_REQUIREMENTS)
      extends ShuffleExchangeLike
   */

  /**
   * Все они наследуют parkPlan (напрямую, либо через UnaryExecNode/Exchange).
   * package org.apache.spark.sql.execution
   */

  /* abstract class SparkPlan extends QueryPlan[SparkPlan] with Logging with Serializable */
  /**
   * В данном классе имеется метод
   * final def execute(): RDD[InternalRow],
   * который запускает абстрактный метод
   * protected def doExecute(): RDD[InternalRow],
   * который имеет конкретную реализацию в каждом физическом операторе, но в большинстве случаев он выполняет
   * F(child.execute())
   *
   * doExecute() - вычисляет результат работы оператора.
   *
   * Т.е. технически - каждый оператор принимает на вход RDD[InternalRow], вызывая child.execute(),
   * и применяет функцию F, которая выполняет трансформацию RDD[InternalRow] => RDD[InternalRow]
   *
   * child: SparkPlan - ссылка на предыдущий физический оператор.
   *
   * Т.е. идут вложенные вызовы doExecute:
   * HashAggregate
   *   .doExecute
   *   .child.execute  // child == Exchange hashpartitioning
   *     .doExecute
   *     .child.execute // child == HashAggregate
   *     ...
   *
   * Эта цепочка начинает выполняться при применении action (show/collect/write/...).
   * Т.е. план строится с начала, а разворачивается с конца (explain пишется сверху вниз).
   */

  /** FilterExec */
  /*
    // выполняется на драйвере
    protected override def doExecute(): RDD[InternalRow] = {
      val numOutputRows = longMetric("numOutputRows")
      // код ниже сериализуется и передается на воркеры
      child.execute().mapPartitionsWithIndexInternal { (index, iter) =>
        val predicate = Predicate.create(condition, child.output)
        predicate.initialize(0)
        iter.filter { row =>
          val r = predicate.eval(row)
          if (r) numOutputRows += 1
          r
        }
      }
    }
   */

  /** package org.apache.spark.sql.execution */
  /* case class CoalesceExec(numPartitions: Int, child: SparkPlan) extends UnaryExecNode */


  /**
   * Создание DataFrame.
   *
   * Первый оператор, который создает RDD[InternalRow] - это коннектор к источнику данных,
   * реализованный с помощью Datasource API через наследование класса BaseRelation.
   * package org.apache.spark.sql.sources
   */
  /* abstract class BaseRelation */
  /** У него есть методы: */
  /*
    def schema: StructType
    ??? def buildScan(): RDD[Row] - в trait TableScan
   */
  /**
   * Каждый коннектор реализует данные методы.
   *
   * При вызове spark.read.format("xxx").load методы schema и buildScan формируют DataFrame,
   * который под капотом представляет собой RDD[InternalRow] и schema: StructType.
   *
   * При записи DataFrame с помощью df.write.format("xxx").save под капотом обычно выполняется:
   */
  /*
    rdd.foreachPartition { rows =>
      write(rows)
    }
   */
  /**
   * Где
   * rdd - RDD[InternalRow]
   * rows - Iterator[InternalRow]
   * write - функция записи партиции с данными в источник, выполняемая на каждом воркере.
   */


  /**
   * Создание RDD.
   *
   * RDD[InternalRow] возникает при чтении источника и создается на основе класса RDD:
   */
  /*
    abstract class RDD[T: ClassTag](
        @transient private var _sc: SparkContext,
        @transient private var deps: Seq[Dependency[_]]
      ) extends Serializable with Logging {
      ...
      @DeveloperApi
      def compute(split: Partition, context: TaskContext): Iterator[T]

      // разработчик сам определяет начальное количество партиций при реализации getPartitions
      protected def getPartitions: Array[Partition]
      ...
      }
   */
  /*
    trait Partition extends Serializable {
      def index: Int
      ...
    }
   */
  /**
   * Метод getPartitions вызывается на драйвере при создании объекта RDD[T] и возвращает массив Partition:
   * Начальное количество партиций == Array().length (определяется разработчиком)
   *
   * Каждый объет Partition хранит информацию о себе:
   * 1. порядковый номер.
   * 2. соответствие партиции и блока исходных данных из источника (маппинг).
   * Т.е. Partition - не сами данные, а метаинформация о данных.
   *
   * Объекты Partition передаются на воркеры.
   *
   * Другими словами, каждый объект Partition (конкретная его реализация зависит от источника) обладает всей информацией,
   * достаточной для того, чтобы, находясь на воркере, прочитать из заданного источника блок данных.
   *
   * Метод compute вызывается на воркере на каждый объект Partition в момент применения действий (show/count/...)
   * к DataFrame/RDD, возвращая Iterator[T], содержащий данные.
   *
   * В итоге мы получаем rdd, состоящий из определенного количества Iterator[T] (Iterator[T] ==  партиция).
   * Количество партиций в RDD соответствует количеству Partition, который вернул метод getPartitions.
   * Каждая партиция существует на одном из воркеров (алокация партиций по воркерам происходит случайным образом).
   *
   * 1. При создании RDD[T] - получаем Array(Partition).
   * 2. => Partition передаются на воркеры.
   * 3. При вызове action на каждом воркере у партиций вызывается метод compute, возвращающий данные - Iterator[T]
   */

//  spark
//    .range(100000)
//    .repartition(3)
//    .write
//    .mode(SaveMode.Overwrite)
//    .parquet("src/main/resources/l_2/test.parquet")

  val testDf: DataFrame =
    spark
      .read
      .parquet("src/main/resources/l_2/test.parquet")

  /** queryExecution - точка входа во "внутренности" датафрейма */
  val rdd: RDD[InternalRow] =
    testDf
      .queryExecution
      .toRdd

  val partitions: Array[Partition] = rdd.partitions

  /** package org.apache.spark.rdd */
  /*
    ...
    @volatile @transient private var partitions_ : Array[Partition] = _
    ...

    final def partitions: Array[Partition] = {
        checkpointRDD.map(_.partitions).getOrElse {
          if (partitions_ == null) {
            stateLock.synchronized {
              if (partitions_ == null) {
                partitions_ = getPartitions
                partitions_.zipWithIndex.foreach { case (partition, index) =>
                  require(partition.index == index,
                    s"partitions($index).partition == ${partition.index}, but it should equal $index")
                }
              }
            }
          }
          partitions_
        }
      }
   */

  partitions.foreach { partition =>
    val filePartition: FilePartition = partition.asInstanceOf[FilePartition]
    println(s"Partition ${filePartition.index}:\n${filePartition.files.mkString("\n")}")
  }
  /** package org.apache.spark.sql.execution.datasources */
  /*
    case class FilePartition(index: Int, files: Array[PartitionedFile])
      extends Partition with InputPartition
   */
  /*
    case class PartitionedFile(
         partitionValues: InternalRow,
         filePath: String,
         start: Long,
         length: Long,
         @transient locations: Array[String] = Array.empty)
   */

  /**
   * doExecute - альтернатива кодогенерации.
   *
   * doExecute - используется, когда невозможно вызвать кодогенерацию (например ошибка компиляции в сгенерированном java коде).
   * Т.е. кодогенерация - имеет более высокий приоритет.
   *
   * doConsume - потребляет результат кодогенерации предыдущего физического оператора.
   * doProduce - формирует результат кодонегерации для следующего физического оператора.
   * Цепочка этих вызовов == WholeStageCodeGen
   */
  /** package org.apache.spark.sql.execution */
  /*
    protected override def doProduce(ctx: CodegenContext): String = {
      child.asInstanceOf[CodegenSupport].produce(ctx, this)
    }

    override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
      val numOutput = metricTerm(ctx, "numOutputRows")

      val predicateCode = generatePredicateCode(
        ctx, child.output, input, output, notNullPreds, otherPreds, notNullAttributes)

      // Reset the isNull to false for the not-null columns, then the followed operators could
      // generate better code (remove dead branches).
      val resultVars = input.zipWithIndex.map { case (ev, i) =>
        if (notNullAttributes.contains(child.output(i).exprId)) {
          ev.isNull = FalseLiteral
        }
        ev
      }

      // Note: wrap in "do { } while(false);", so the generated checks can jump out with "continue;"
      s"""
         |do {
         |  $predicateCode
         |  $numOutput.add(1);
         |  ${consume(ctx, resultVars)}
         |} while(false);
       """.stripMargin
    }
   */

  def printCodeGen[_](ds: Dataset[_]): Unit = {
    val logicalPlan: LogicalPlan =
      ds
        .queryExecution
        .logical

//    val codeGen: ExplainCommand = ExplainCommand(logicalPlan, extended = true, codegen = true)  // Spark 2.4
    val codeGen: ExplainCommand = ExplainCommand(logicalPlan, ExplainMode.fromString("codegen"))  // Spark 3.2.0

    spark
      .sessionState
      .executePlan(codeGen)
      .executedPlan
      .executeCollect
      .foreach(InternalRow => println(InternalRow.getString(0)))
  }

  val testDf2: Dataset[lang.Long] =
    spark
      .range(1)
      .localCheckpoint

  printCodeGen(testDf2.select($"id" + 9))
  /*
    Found 1 WholeStageCodegen subtrees.
    == Subtree 1 / 1 (maxMethodCodeSize:105; maxConstantPoolSize:99(0.15% used); numInnerClasses:0) ==
    *(1) Project [(id#32L + 9) AS (id + 9)#43L]
    +- *(1) Scan ExistingRDD[id#32L]

    Generated code:
    /* 001 */ public Object generate(Object[] references) {
    /* 002 */   return new GeneratedIteratorForCodegenStage1(references);
    /* 003 */ }
    /* 004 */
    /* 005 */ // codegenStageId=1
    /* 006 */ final class GeneratedIteratorForCodegenStage1 extends org.apache.spark.sql.execution.BufferedRowIterator {
    /* 007 */   private Object[] references;
    /* 008 */   private scala.collection.Iterator[] inputs;
    /* 009 */   private scala.collection.Iterator rdd_input_0;
    /* 010 */   private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[] rdd_mutableStateArray_0 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[2];
    /* 011 */
    /* 012 */   public GeneratedIteratorForCodegenStage1(Object[] references) {
    /* 013 */     this.references = references;
    /* 014 */   }
    /* 015 */
    /* 016 */   public void init(int index, scala.collection.Iterator[] inputs) {
    /* 017 */     partitionIndex = index;
    /* 018 */     this.inputs = inputs;
    /* 019 */     rdd_input_0 = inputs[0];
    /* 020 */     rdd_mutableStateArray_0[0] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(1, 0);
    /* 021 */     rdd_mutableStateArray_0[1] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(1, 0);
    /* 022 */
    /* 023 */   }
    /* 024 */
    /* 025 */   protected void processNext() throws java.io.IOException {
    /* 026 */     while ( rdd_input_0.hasNext()) {
    /* 027 */       InternalRow rdd_row_0 = (InternalRow) rdd_input_0.next();
    /* 028 */       ((org.apache.spark.sql.execution.metric.SQLMetric) references[0] /* numOutputRows */).add(1);
    /* 029 */       // common sub-expressions
    /* 030 */
    /* 031 */       long rdd_value_0 = rdd_row_0.getLong(0);
    /* 032 */
    /* 033 */       long project_value_0 = -1L;
    /* 034 */
    /* 035 */       project_value_0 = rdd_value_0 + 9L;
    /* 036 */       rdd_mutableStateArray_0[1].reset();
    /* 037 */
    /* 038 */       rdd_mutableStateArray_0[1].write(0, project_value_0);
    /* 039 */       append((rdd_mutableStateArray_0[1].getRow()));
    /* 040 */       if (shouldStop()) return;
    /* 041 */     }
    /* 042 */   }
    /* 043 */
    /* 044 */ }
   */


}
