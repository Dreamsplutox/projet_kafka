package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

case class LastMinute(
                 start_only: Long,
                 half: Long,
                 full: Long
               )

object LastMinute {
  implicit val format: OFormat[LastMinute] = Json.format[LastMinute]
}
