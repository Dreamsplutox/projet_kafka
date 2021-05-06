package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

case class Movie(
                 _id: Int,
                 title: String,
                 view_count: Long,
                 stats: StatMovie
               )

object Movie {
  implicit val format: OFormat[Movie] = Json.format[Movie]
}
