package org.esgi.project.api.models

import play.api.libs.json.{Json, OFormat}

case class ViewCountResponse(
                               title: String,
                               count: Long
                             )

object ViewCountResponse {
  implicit val format: OFormat[ViewCountResponse] = Json.format[ViewCountResponse]
}
