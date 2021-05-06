package org.esgi.project.streaming.models

import play.api.libs.json.{Json, OFormat}

case class ViewsWithLikes(
                             _id: Int,
                             title: String,
                             score: Float
                           )



object ViewsWithLikes {
  implicit val format: OFormat[ViewsWithLikes] = Json.format[ViewsWithLikes]
}
