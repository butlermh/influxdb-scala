package org.influxdb

import org.json4s.jackson.Serialization
import org.json4s.NoTypeHints
import com.ning.http.client.{ Response, AsyncHttpClient }
import java.util.concurrent.{ Future, TimeUnit }
import org.json4s.jackson.Serialization._
import scala.Some
import java.net.URLEncoder

class Client(host: String = "localhost:8086", var username: String = "root", var password: String = "root", var database: String = "", schema: String = "http") {
  implicit val formats = Serialization.formats(NoTypeHints)
  private val httpClient = new AsyncHttpClient()

  var (timeout, unit) = (3, TimeUnit.SECONDS)

  def close() {
    httpClient.close()
  }

  def ping: error.Error = {
    try {
      val url = getUrl("/ping")
      responseToError(getResponse(httpClient.prepareGet(url).execute()))
    } catch {
      case ex: Exception => Some(ex.getMessage)
    }
  }

  private def postJson(url: String, json: String): error.Error = 
    try {
    responseToError(getResponse(httpClient.preparePost(url).addHeader("Content-Type", "application/json").setBody(json).execute()))
  } catch {
    case ex: Exception => Some(ex.getMessage)
  }

  private def delete(url: String): error.Error = 
    try {
    responseToError(getResponse(httpClient.prepareDelete(url).execute()))
  } catch {
    case ex: Exception => Some(ex.getMessage)
  }

  private def getList[T](url: String)(implicit manifest: Manifest[T]): (List[T], error.Error) = 
    try {
      val r = getResponse(httpClient.prepareGet(url).execute())
      responseToError(r) match {
        case None => (read[List[T]](r.getResponseBody), None)
        case Some(err) => (Nil, Some(err))
      }
    } catch {
      case ex: Exception => (Nil, Some(ex.getMessage))
    }
    
  private def get(url: String): error.Error = 
    try {
      responseToError(getResponse(httpClient.prepareGet(url).execute()))
    } catch {
      case ex: Exception => Some(ex.getMessage)
    }
    
  def createDatabase(name: String, replicationFactor: Int = 1): error.Error =
    postJson(url = getUrl("/db"),
      json = write(Map("name" -> name, "replicationFactor" -> replicationFactor)))

  def deleteDatabase(name: String): error.Error =
    delete(url = getUrl(s"/db/$name"))

  def getDatabaseList: (List[response.Database], error.Error) =
    getList[response.Database](url = getUrl("/db"))

  def createDatabaseUser(database: String, username: String, password: String, readFrom: Option[String] = None, writeTo: Option[String] = None): error.Error =
    postJson(url = getUrl(s"/db/$database/users"),
      json = write(Map("name" -> username, "password" -> password, "readFrom" -> readFrom, "writeTo" -> writeTo)))

  def updateDatabaseUser(database: String, username: String, password: Option[String] = None, isAdmin: Boolean = false, readFrom: Option[String] = None, writeTo: Option[String] = None): error.Error =
    postJson(
      url = getUrl(s"/db/$database/users/$username"),
      json = write(Map("password" -> password, "admin" -> isAdmin, "readFrom" -> readFrom, "writeTo" -> writeTo)))

  def authenticateDatabaseUser(database: String, username: String, password: String): error.Error = 
    get(url = getUrlWithUserAndPass(s"/db/$database/authenticate", username, password))

  def createClusterAdmin(username: String, password: String): error.Error =
    postJson(url = getUrl("/cluster_admins"),
      json = write(Map("name" -> username, "password" -> password)))

  def updateClusterAdmin(username: String, password: String): error.Error =
    postJson(url = getUrl(s"/cluster_admins/$username"),
      json = write(Map("password" -> password)))

  def deleteClusterAdmin(username: String): error.Error =
    delete(url = getUrl(s"/cluster_admins/$username"))

  def getClusterAdminList: (List[response.ClusterAdmin], error.Error) =
    getList[response.ClusterAdmin](url = getUrl(s"/cluster_admins"))

  def authenticateClusterAdmin(username: String, password: String): error.Error = 
    get(url = getUrlWithUserAndPass("/cluster_admins/authenticate", username, password))

  def query(query: String, timePrecision: Option[String] = None, chunked: Boolean = false): (response.Response, error.Error) = {
    try {
      val q = URLEncoder.encode(query, "UTF-8")
      val url = getUrl(s"/db/$database/series") + s"&q=$q&chunked=$chunked" +
        (if (timePrecision.isDefined) s"&time_precision=${timePrecision.get}" else "")

      val r = getResponse(httpClient.prepareGet(url).execute())
      responseToError(r) match {
        case None => (response.Response(r.getResponseBody), None)
        case Some(err) => (null, Some(err))
      }
    } catch {
      case ex: Exception => (null, Some(ex.getMessage))
    }
  }

  def getContinuousQueries: (List[response.ContinuousQuery], error.Error) = 
    getList[response.ContinuousQuery](url = getUrl(s"/db/$database/continuous_queries"))

  def deleteContinuousQueries(id: Int): error.Error =
    delete(url = getUrl(s"/db/$database/continuous_queries/$id"))

  def writeSeries(series: Array[Series]): error.Error = writeSeriesCommon(series, None)

  def writeSeriesWithTimePrecision(series: Array[Series], timePrecision: String): error.Error = {
    writeSeriesCommon(series, Some(Map[String, String]("time_precision" -> timePrecision)))
  }

  private def writeSeriesCommon(series: Array[Series], options: Option[Map[String, String]]): error.Error = 
    postJson(url = getUrl(s"/db/$database/series") + 
        (if (options.isDefined) options.get.map { o => val (k, v) = o; s"$k=$v" }.mkString("&", "&", "") else ""),
      json = write(series))

  private def responseToError(r: Response): error.Error = {
    if (r.getStatusCode >= 200 && r.getStatusCode < 300) {
      return None
    }
    return Some(s"Server returned (${r.getStatusText}): ${r.getResponseBody}")
  }
  private def getResponse(fr: Future[Response]): Response = fr.get(timeout, unit)
  private def getUrlWithUserAndPass(path: String, username: String, password: String): String = s"$schema://$host$path?u=$username&p=$password"
  private def getUrl(path: String) = getUrlWithUserAndPass(path, username, password)
}

import scala.util.parsing.json.JSON

package object response {
  case class Database(name: String, replicationFactor: Int)
  case class ClusterAdmin(username: String)
  case class ContinuousQuery(id: Int, query: String)
  case class Response(json: String) {

    def toSeries: Array[Series] =
      JSON.parseFull(json).get.asInstanceOf[List[Any]].map { ai =>

        // it would be better to define a case class here, deserialize to that
        // to get rid of the need for the asInstanceOf ... 

        val m = ai.asInstanceOf[Map[String, Any]]
        Series(
          name = m("name").asInstanceOf[String],
          columns = m("columns").asInstanceOf[List[String]].toArray,
          points = m("points").asInstanceOf[List[List[Any]]].map(_.toArray).toArray)
      }.toArray

    def toSeriesMap: Array[SeriesMap] =
      toSeries.map {
        case Series(name, columns, points) =>
          SeriesMap(
            name = name,
            objects = columns.zip(points.map(_.toArray)).toMap)
      }.toArray
  }
}

package object error {
  type Error = Option[String]
}

case class Series(name: String, columns: Array[String], points: Array[Array[Any]])
case class SeriesMap(name: String, objects: Map[String, Array[Any]])