package com.unchartedsoftware.mosaic.core.analytic.numeric

import org.scalatest._
import com.unchartedsoftware.mosaic.Spark
import com.unchartedsoftware.mosaic.core.projection.Projection
import com.unchartedsoftware.mosaic.core.projection.numeric._
import com.unchartedsoftware.mosaic.core.generation.Series
import com.unchartedsoftware.mosaic.core.generation.mapreduce.MapReduceTileGenerator
import com.unchartedsoftware.mosaic.core.generation.output._
import com.unchartedsoftware.mosaic.core.generation.request._
import com.unchartedsoftware.mosaic.core.analytic._
import com.unchartedsoftware.mosaic.core.analytic.numeric._
import com.unchartedsoftware.mosaic.core.util.ValueExtractor
import org.apache.spark.sql.Row

//define tests here so that scalatest stuff isn't serialized into spark closures
object MapReduceTileGeneratorSpecClosure {

  def testClosure(
    data: Array[Double],
    series: Seq[Series[_,(Int, Int),_,_,_,_,_,_]],
    request: TileSeqRequest[(Int, Int)]
  ): Seq[Seq[TileData[(Int, Int), _, _]]] = {
    //generate some random data
    var frame = Spark.sc.parallelize(data.map(a => Row(a)))

    //create generator
    val gen = new MapReduceTileGenerator[(Int, Int)](Spark.sc)

    //kickoff generation
    gen.generate(frame, series, request).collect
  }
}

class MapReduceTileGeneratorSpec extends FunSpec {
  describe("MapReduceTileGenerator") {
    describe("#generate()") {
      it("should generate tile level 0, correctly distributing input points into bins") {

        //generate some random data
        val data = Array.fill(10)(0D).map(a => Math.random)

        //manually bin
        val manualBins = data.groupBy(a => a > 0.5).map(a => (a._1, a._2.length))

        //create projection, request, extractors
        val cExtractor = new ValueExtractor[Double] {
          override def rowToValue(r: Row): Option[Double] = {
            return Some(r.getDouble(0))
          }
        }
        val projection = new SeriesProjection(0, 1, 0D, 1D)
        val request = new TileSeqRequest[(Int, Int)](Seq((0,0)), projection)

        //create Series
        val series = Seq(
          new Series(2, cExtractor, projection, None, CountAggregator, Some(MaxMinAggregator))
        )

        val tiles = MapReduceTileGeneratorSpecClosure.testClosure(data, series, request)
        val result = tiles.map(s => {
          s(0).asInstanceOf[TileData[(Int, Int), java.lang.Double, (java.lang.Double, java.lang.Double)]]
        })
        assert(result.length === 1) //did we generate a tile?

        //verify binning
        assert(result(0).bins(0) === manualBins.get(false).getOrElse(0))
        assert(result(0).bins(1) === manualBins.get(true).getOrElse(0))

        //verify max/min tile analytic
        val min = result(0).bins.reduce((a,b) => Math.min(a, b))
        val max = result(0).bins.reduce((a,b) => Math.max(a, b))
        assert(result(0).tileMeta.isDefined)
        assert(result(0).tileMeta.get._1 === min)
        assert(result(0).tileMeta.get._2 === max)

        //verify bins touched
        val binsTouched = manualBins.toSeq.length
        assert(result(0).binsTouched === binsTouched)
      }

      it("should ignore rows which are outside the bounds of the projection") {
        //generate some random data
        val data = Array.fill(10)(0D).map(a => Math.random)

        //manually bin
        val manualBins = data.filter(a => a <= 0.5).groupBy(a => a > 0.25).map(a => (a._1, a._2.length))

        //create projection, request, extractors
        val cExtractor = new ValueExtractor[Double] {
          override def rowToValue(r: Row): Option[Double] = {
            return Some(r.getDouble(0))
          }
        }
        val projection = new SeriesProjection(0, 1, 0D, 0.5D)
        val request = new TileSeqRequest[(Int, Int)](Seq((0,0)), projection)
        val vExtractor = new ValueExtractor[Double] {
          override def rowToValue(r: Row): Option[Double] = {
            return None
          }
        }

        //create Series
        val series = Seq(
          new Series(2, cExtractor, projection, Some(vExtractor), CountAggregator, Some(MaxMinAggregator))
        )

        val tiles = MapReduceTileGeneratorSpecClosure.testClosure(data, series, request)
        val result = tiles.map(s => {
          s(0).asInstanceOf[TileData[(Int, Int), java.lang.Double, (java.lang.Double, java.lang.Double)]]
        })
        assert(result.length === 1) //did we generate a tile?

        //verify binning
        assert(result(0).bins(0) === manualBins.get(false).getOrElse(0))
        assert(result(0).bins(1) === manualBins.get(true).getOrElse(0))

        //verify max/min tile analytic
        val min = result(0).bins.reduce((a,b) => Math.min(a, b))
        val max = result(0).bins.reduce((a,b) => Math.max(a, b))
        assert(result(0).tileMeta.isDefined)
        assert(result(0).tileMeta.get._1 === min)
        assert(result(0).tileMeta.get._2 === max)

        //verify bins touched
        val binsTouched = manualBins.toSeq.length
        assert(result(0).binsTouched === binsTouched)
      }

      it("should generate successive tile levels, correctly distributing input points into bins") {

        //generate some random data
        val data = Array.fill(10)(0D).map(a => Math.random)

        //create projection, request, extractors
        val cExtractor = new ValueExtractor[Double] {
          override def rowToValue(r: Row): Option[Double] = {
            return Some(r.getDouble(0))
          }
        }
        val projection = new SeriesProjection(0, 1, 0D, 1D)
        val request = new TileSeqRequest[(Int, Int)](Seq((0,0), (1,0), (1,1)), projection)
        val vExtractor = new ValueExtractor[Double] {
          override def rowToValue(r: Row): Option[Double] = {
            return None
          }
        }

        //create Series
        val series = Seq(
          new Series(10, cExtractor, projection, Some(vExtractor), CountAggregator, Some(MaxMinAggregator))
        )

        val tiles = MapReduceTileGeneratorSpecClosure.testClosure(data, series, request)
        val result = tiles.map(s => {
          s(0).asInstanceOf[TileData[(Int, Int), java.lang.Double, (java.lang.Double, java.lang.Double)]]
        })
        assert(result.length === 3) //did we generate tiles?

        //map the result so that it's easier to work with
        val tilesMap = result.map(a => (a.coords, a)).toMap

        //verify binning of level 1 by aggregating it into level 0
        val combinedOneBins = tilesMap.get((1,0)).get.bins ++ tilesMap.get((1,1)).get.bins

        //verify tile levels 1 and 0 are consistent
        var j = 0
        for (i <- 0 until 10) {
          val zeroBin = tilesMap.get((0,0)).get.bins(i)
          assert(zeroBin === combinedOneBins(j) + combinedOneBins(j+1))
          j = j + 2
        }
      }

      //test optional tile aggregator
      it("should support optional tile aggregators") {
        //generate some random data
        val data = Array.fill(10)(0D).map(a => Math.random)

        //manually bin
        val manualBins = data.groupBy(a => a > 0.5).map(a => (a._1, a._2.length))

        //create projection, request, extractors
        val cExtractor = new ValueExtractor[Double] {
          override def rowToValue(r: Row): Option[Double] = {
            return Some(r.getDouble(0))
          }
        }
        val projection = new SeriesProjection(0, 1, 0D, 1D)
        val request = new TileSeqRequest[(Int, Int)](Seq((0,0)), projection)
        val vExtractor = new ValueExtractor[Double] {
          override def rowToValue(r: Row): Option[Double] = {
            return None
          }
        }

        //create Series
        val series = Seq(
          new Series(2, cExtractor, projection, Some(vExtractor), CountAggregator, None)
        )

        val tiles = MapReduceTileGeneratorSpecClosure.testClosure(data, series, request)
        val result = tiles.map(s => {
          s(0).asInstanceOf[TileData[(Int, Int), java.lang.Double, (java.lang.Double, java.lang.Double)]]
        })
        assert(result.length === 1) //did we generate a tile?

        //verify binning
        assert(result(0).bins(0) === manualBins.get(false).getOrElse(0))
        assert(result(0).bins(1) === manualBins.get(true).getOrElse(0))

        //verify max/min tile analytic is not present
        assert(!result(0).tileMeta.isDefined)

        //verify bins touched
        val binsTouched = manualBins.toSeq.length
        assert(result(0).binsTouched === binsTouched)
      }

      //TODO test multiple series
    }
  }
}
