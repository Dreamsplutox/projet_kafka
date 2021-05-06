package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

case class ViewWithScore(
                          _id: Int,
                          title: String,
                          view_category: String,
                          score: Float
                        )


object ViewWithScore {
  implicit val format: OFormat[ViewWithScore] = Json.format[ViewWithScore]
}