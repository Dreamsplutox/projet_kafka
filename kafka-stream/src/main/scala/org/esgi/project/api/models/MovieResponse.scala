package org.esgi.project.api.models

import play.api.libs.json.{Json, OFormat}

case class MovieResponse(
                 _id: Int,
                 title: String,
                 view_count: Long,
                 stats: StatMovie
               )

object MovieResponse {
  implicit val format: OFormat[MovieResponse] = Json.format[MovieResponse]
}
