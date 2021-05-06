package org.esgi.project.api.models

import play.api.libs.json.{Json, OFormat}

case class Past(
                 start_only: Long,
                 half: Long,
                 full: Long
               )

object Past {
  implicit val format: OFormat[Past] = Json.format[Past]
}
