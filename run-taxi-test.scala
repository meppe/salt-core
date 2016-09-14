import software.uncharted.salt.core.projection.numeric._
import software.uncharted.salt.core.generation.Series
import software.uncharted.salt.core.generation.TileGenerator
import software.uncharted.salt.core.analytic._
import software.uncharted.salt.core.generation.request._
import software.uncharted.salt.core.analytic.numeric._
import java.sql.Timestamp
import org.apache.spark.sql.Row

// source RDD
// It is STRONGLY recommended that you filter your input RDD
// down to only the columns you need for tiling.
val rdd = sqlContext.read.format("com.databricks.spark.csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("file:///taxi_micro.csv") // be sure to update the file path to reflect
                                  // the download location of taxi_micro.csv
  .select("pickup_lon", "pickup_lat", "passengers")
  .rdd

// cache the RDD to make things a bit faster
rdd.cache

// We use a value extractor function to retrieve data-space coordinates from rows
// In this case, that's column 0 (pickup_time, converted to a double millisecond value) and column 1 (distance)
val cExtractor = (r: Row) => {
  if (r.isNullAt(0) || r.isNullAt(1)) {
    None
  } else {
    Some((r.getDouble(0), r.getDouble(1)))
  }
}

// create a projection from data-space into mercator tile space, which is suitable for
// display on top of a map using a mapping library such as leaflet.js
// we specify the zoom levels we intend to support using a Seq[Int]
val projection = new MercatorProjection(Seq(0,1))

// a value extractor function to grab the number of passengers from a Row
val vExtractor = (r: Row) => {
  if (r.isNullAt(2)) {
    None
  } else {
    Some(r.getInt(2).toDouble)
  }
}

// A series ties the value extractors, projection and bin/tile aggregators together.
// We'll be tiling average passengers per bin, and max/min of the bin averages per tile
// We'll also divide our tiles into 8x8 bins so that the output is readable. We specify
// this using the maximum possible bin index, which is (7,7)
val avgPassengers = new Series((7, 7), cExtractor, projection, vExtractor, MeanAggregator, Some(MinMaxAggregator))

// which tiles are we generating? In this case, we'll use a TileSeqRequest
// which allows us to specify a list of tiles we're interested in, by coordinate.
// these tiles should be within the bounds of the Projection we created earlier
val request = new TileSeqRequest(Seq((0,0,0), (1,0,0)))

// Tile Generator object, which houses the generation logic
@transient val gen = TileGenerator(sc)

// Flip the switch by passing in the series and the request
// Note: Multiple series can be run against the source data
// at the same time, so a Seq[Series] is also supported as
// the second argument to generate()
val result = gen.generate(rdd, avgPassengers, request)

// Try to read some values from bins, from the first (and only) series
println(result.map(t => (avgPassengers(t).get.coords, avgPassengers(t).get.bins)).collect.deep.mkString("\n"))