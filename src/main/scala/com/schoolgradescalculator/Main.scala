package com.schoolgradescalculator

import java.io.File

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Sink, Source}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContextExecutor, Future}

/**
 * Created by Nir.
 */
object Main {

  implicit val system: ActorSystem = ActorSystem("School_Grades_Calculator")
  implicit val materializer: Materializer = Materializer(system)
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  private val readFilesParallelism = 2
  private val calcAverageParallelism = 2
  private val PrintAverageParallelism = 3
  private val calcHistogramParallelism = 2
  private val PrintHistogramParallelism = 3
  private val calcMedianParallelism = 2
  private val PrintMedianParallelism = 3

  def main(args: Array[String]): Unit = {
    val directoryPath = args(0)
    Source.fromIterator(() => getListOfFiles(directoryPath))
      .mapAsyncUnordered(readFilesParallelism)(f => Future(convertFileToBucketList(f)))
      .alsoTo(calculationPerClass)
      .alsoTo(calculationPerSchool)
      .run()
  }

  val printAverage: Sink[Seq[Long], NotUsed] = Flow[Seq[Long]]
    .mapAsyncUnordered(calcAverageParallelism)(g => Future(calcAverage(g)))
    .to(Sink.foreachAsync(PrintAverageParallelism)(x => Future(println(s"Average: $x"))))

  val printHistogram: Sink[Seq[Long], NotUsed] = Flow[Seq[Long]]
    .mapAsyncUnordered(calcHistogramParallelism)(g => Future(calcHistogram(g)))
    .to(Sink.foreachAsync(PrintHistogramParallelism)(x => Future(println(s"histogram: $x"))))


  val printMedian: Sink[Seq[Long], NotUsed] = Flow[Seq[Long]]
    .mapAsyncUnordered(calcMedianParallelism)(g => Future(calcMedian(g)))
    .to(Sink.foreachAsync(PrintMedianParallelism)(x => Future(println(s"median: $x"))))


  val calculationPerClass: Sink[Seq[Long], NotUsed] = printAverage

  val calculationPerSchool: Sink[Seq[Long], NotUsed] =
    Flow[Seq[Long]]
      .reduce(reduceLists)
      .alsoTo(printAverage)
      .alsoTo(printMedian)
      .alsoTo(printHistogram)
      .to(Sink.ignore)

  def getListOfFiles(path: String): Iterator[File] =
    new java.io.File(path).listFiles.iterator

  def convertFileToBucketList(file: File): Seq[Long] = {
    val res = ArrayBuffer.fill[Long](100)(0)
    val pathSource = scala.io.Source.fromFile(file)
    pathSource.getLines
      .map(x => x.split(",")(1).toInt)
      .foreach(num => res(num - 1) += 1)
    pathSource.close()
    res.toSeq
  }

  private def reduceLists(acc: Seq[Long], list: Seq[Long]) = {
    acc zip list map { case (a, b) => a + b }
  }

  private def calcAverage(list: Seq[Long]) = {
    val (sum, numOfElements) = list.zipWithIndex
      .map {
        case (numberOfElements, i) =>
          val sumOfElements = numberOfElements * (i + 1)
          (sumOfElements, numberOfElements)
      }
      .reduce[(Long, Long)] {
        case ((sum, generalNumOfElements), (indexSum, numOfElements)) =>
          ((sum + indexSum), generalNumOfElements + numOfElements)
      }
    sum / numOfElements
  }

  private def calcHistogram(list: Seq[Long]) = {
    val histogram = ArrayBuffer.fill[Long](10)(0)
    for (fromIndex <- 0 to 9) {
      histogram(fromIndex) = list
        .slice(fromIndex * 10, (fromIndex * 10) + 10)
        .sum
    }
    histogram
  }

  private def calcMedian(list: Seq[Long]) = {
    var elementsBeforeMedian = list.sum / 2
    var i = 0
    while (elementsBeforeMedian > 0) {
      elementsBeforeMedian -= list(i)
      i += 1
    }
    i
  }

}
