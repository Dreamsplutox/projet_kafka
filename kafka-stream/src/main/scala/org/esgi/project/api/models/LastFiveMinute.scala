package org.esgi.project.api.models

import play.api.libs.json.{Json, OFormat}

case class LastFiveMinute(
                       start_only: Long,
                       half: Long,
                       full: Long
                     )

object LastFiveMinute {
  implicit val format: OFormat[LastFiveMinute] = Json.format[LastFiveMinute]
}
