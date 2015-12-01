/*
 * Copyright (c) 2014 DigitalGlobe.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis

import geotrellis.raster._
import geotrellis.vector._
import geotrellis.proj4._

import geotrellis.spark.tiling._
import geotrellis.spark.ingest.IngestKey

import org.apache.spark.rdd._

import spire.syntax.cfor._

import monocle.{Lens, PLens}
import monocle.syntax._

import scala.reflect.ClassTag

package object spark {

  type ComponentLens[K, C] = PLens[K, K, C, C]

  type SpatialComponent[K] = KeyComponent[K, SpatialKey]
  type TemporalComponent[K] = KeyComponent[K, TemporalKey]

  implicit class SpatialComponentWrapper[K: SpatialComponent](key: K) {
    val _spatialComponent = implicitly[SpatialComponent[K]]

    def spatialComponent: SpatialKey = key &|-> _spatialComponent.lens get

    def updateSpatialComponent(spatialKey: SpatialKey): K =
      key &|-> _spatialComponent.lens set(spatialKey)
  }

  implicit class TemporalCompenentWrapper[K: TemporalComponent](key: K) {
    val _temporalComponent = implicitly[TemporalComponent[K]]

    def temporalComponent: TemporalKey = key &|-> _temporalComponent.lens get

    def updateTemporalComponent(temporalKey: TemporalKey): K =
      key &|-> _temporalComponent.lens set(temporalKey)
  }

  // implicit class IngestKeyWrapper[K: IngestKey](key: K) {
  //   val _ingestKey = implicitly[IngestKey[K]]

  //   def getExtent(): ProjectedExtent = key &|-> _ingestKey.lens get

  // }
  type TileBounds = GridBounds

  implicit class toPipe[A](x : A) {
    def |> [T](f : A => T) = f(x)
  }

  implicit class toPipe2[A, B](tup : (A, B)) {
    def |> [T](f : (A, B) => T) = f(tup._1, tup._2)
  }

  implicit class toPipe3[A, B, C](tup : (A, B, C)) {
    def |> [T](f : (A, B, C) => T) = f(tup._1, tup._2, tup._3)
  }

  implicit class toPipe4[A, B, C, D](tup : (A, B, C, D)) {
    def |> [T](f : (A, B, C, D) => T) = f(tup._1, tup._2, tup._3, tup._4)
  }

  /** Keeps with the convention while still using simple tups, nice */
  implicit class TileTuple[K](tup: (K, Tile)) {
    def id: K = tup._1
    def tile: Tile = tup._2
  }

  def asRasterRDD[K: ClassTag](metaData: RasterMetaData)(f: =>RDD[(K, Tile)]): RasterRDD[K] =
    new RasterRDD[K](f, metaData)

  implicit class MakeRasterRDD[K: ClassTag](val rdd: RDD[(K, Tile)]) {
    def toRasterRDD(metaData: RasterMetaData) =
      new RasterRDD[K](rdd, metaData)
  }

  implicit class RDDTraversableExtensions[K: ClassTag](rs: Traversable[RasterRDD[K]]) {
    def combinePairs(f: (Traversable[(K, Tile)] => (K, Tile))): RasterRDD[K] =
      rs.head.combinePairs(rs.tail)(f)
  }
}
