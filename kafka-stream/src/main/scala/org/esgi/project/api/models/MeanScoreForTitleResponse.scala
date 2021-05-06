package org.esgi.project.api.models

import play.api.libs.json.{Json, OFormat}

case class MeanScoreForTitleResponse(
                                      title: String,
                                      meanScore: Float
                                    )


object MeanScoreForTitleResponse {
  implicit val format: OFormat[MeanScoreForTitleResponse] = Json.format[MeanScoreForTitleResponse]
}
