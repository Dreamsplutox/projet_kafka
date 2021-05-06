package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

case class StatMovie(
                 past: Past,
                 last_minute: LastMinute,
                 last_five_minute: LastFiveMinute
               )

object StatMovie {
  implicit val format: OFormat[StatMovie] = Json.format[StatMovie]
}
