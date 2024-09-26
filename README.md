# go-writer-geoparquet

GeoParquet support for the [writer/go-writer/v3](https://github.com/whosonfirst/go-writer) `Writer` interface.

## Example

_Error handling removed for the sake of brevity._

```
import (
	"context"
	"os"
	
	"github.com/whosonfirst/go-writer/v3"
	_ "github.com/whosonfirst/go-writer-geoparquet/v3"
)

func main() {

	ctx := context.Background()

	geojson_r, _ := os.Open("/path/to/feature.geojson")
	defer geojson_r.Close()
	
	wr, _ := writer.NewWriter(ctx, "geoparquet:///path/to/database.geoparquet")

	wr.Write(ctx, "key", geojson_r)
	wr.Close(ctx)
}
```

Have a look at [geoparquet_test.go](geoparquet_test.go) for a complete example of how to use the package.

If you are just looking for something to create a GeoParquet file from one or more Who's On First data repositories, take a look at the [whosonfirst/wof-cli `emit` tool](https://github.com/whosonfirst/wof-cli/blob/main/emit/README.md#example-geoparquet).

### How does it work?

First of all, this writer only works with GeoJSON Feature records.

For each Feature record passed to the `Write()` method the code will:

* Derive a `whosonfirst/gpq-fork/not-internal/geo.Feature` instance.
* Derive a `whosonfirst/go-whosonfirst-spr/v2.StandardPlaceResponse` instance and convert it to a `map[string]any`.
* Append any additional properties defined in the constructor URI (details below) to the SPR map.
* Assign the SPR map to the `geo.Feature` instance's `Properties` property.
* Write the the `geo.Feature` instance to the underlying GeoParquet database.

### URIs

`go-writer-geoparquet` URIs take the form of:

```
geoparquet://{PATH}?{PARAMETERS}
```

Where `{PATH}` is a valid path on disk where the final GeoParquet database will be written to. If `{PATH}` is empty then data will be written to `STDOUT`.

Valid paramters are:

| Name | Value | Default | Notes |
| --- | --- | --- | --- |
| min | int | 10 | Minimum number of features to consider when building a (GeoParquet) schema. |
| max | int | 100 | Maximum number of features to consider when building a (GeoParquet) schema.|
| compression | string | zstd | Parquet compression to use.  Possible values: uncompressed, snappy, gzip, brotli, zstd. |
| row-group-length | int | 10 | Maximum number of rows per group when writing Parquet data. |
| append-property | string | | Zero or more relative properties to append to the initial SPR instance (derived from the original GeoJSON Feature) before adding it to the GeoParquet database. |

## Important

This package uses [a hard fork](https://github.com/whosonfirst/gpq-fork) of the [planetlabs/gpq](https://github.com/planetlabs/gpq) package in order to expose functions for writing GeoParquet files.

## See also

* https://github.com/whosonfirst/go-writer
* https://github.com/whosonfirst/go-whosonfirst-spr
* https://github.com/whosonfirst/gpq-fork
* https://github.com/planetlabs/gpq
* https://github.com/cholmes/duckdb-geoparquet-tutorials