package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

case class MeanScoreForTitle(
                              sum: Float,
                              count: Long,
                              meanScore: Float
                            ) {
  def increment(score: Float) = this.copy(sum = this.sum + score, count = this.count + 1)

  def computeMeanLatency = this.copy(
    meanScore = this.sum / this.count
  )
}


object MeanScoreForTitle {
  implicit val format: OFormat[MeanScoreForTitle] = Json.format[MeanScoreForTitle]

  def empty: MeanScoreForTitle = MeanScoreForTitle(0, 0, 0)
}