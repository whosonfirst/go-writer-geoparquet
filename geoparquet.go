package geoparquet

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"

	"github.com/apache/arrow/go/v16/parquet"
	"github.com/tidwall/gjson"
	"github.com/whosonfirst/go-whosonfirst-spr/v2"
	spr_util "github.com/whosonfirst/go-whosonfirst-spr/v2/util"
	"github.com/whosonfirst/go-writer/v3"
	"github.com/whosonfirst/gpq-fork/not-internal/geo"
	"github.com/whosonfirst/gpq-fork/not-internal/geojson"
	"github.com/whosonfirst/gpq-fork/not-internal/geoparquet"
	"github.com/whosonfirst/gpq-fork/not-internal/pqutil"
)

// GeoParquetWriter implements the `writer.Writer` interface for writing GeoParquet records.
type GeoParquetWriter struct {
	writer.Writer
	convert_options   *geojson.ConvertOptions
	io_writer         io.WriteCloser
	feature_writer    *geoparquet.FeatureWriter
	buffer            []*geo.Feature
	append_properties []string
}

func init() {
	ctx := context.Background()
	err := writer.RegisterWriter(ctx, "geoparquet", NewGeoParquetWriter)

	if err != nil {
		panic(err)
	}
}

// NewGeoParquetWriter returns a new `writer.Writer` instance derived from 'uri'.
func NewGeoParquetWriter(ctx context.Context, uri string) (writer.Writer, error) {

	u, err := url.Parse(uri)

	if err != nil {
		return nil, fmt.Errorf("Failed to create new writer, %w", err)
	}

	q := u.Query()

	var io_writer io.WriteCloser

	if u.Path == "" {
		io_writer = os.Stdout
	} else {
		
		wr, err := os.OpenFile(u.Path, os.O_RDWR|os.O_CREATE, 0644)
		
		if err != nil {
			return nil, fmt.Errorf("Failed to open %s for writing, %w", u.Path, err)
		}

		io_writer = wr
	}
	
	min := 10
	max := 100
	compression := "zstd"
	row_group_length := 10

	if q.Has("min") {

		v, err := strconv.Atoi(q.Get("min"))

		if err != nil {
			return nil, fmt.Errorf("Invalid ?min= property, %w", err)
		}

		min = v
	}

	if q.Has("max") {

		v, err := strconv.Atoi(q.Get("max"))

		if err != nil {
			return nil, fmt.Errorf("Invalid ?max= property, %w", err)
		}

		max = v
	}

	if q.Has("compression") {
		compression = q.Get("compression")
	}

	if q.Has("row-group-length") {

		v, err := strconv.Atoi(q.Get("row-group-length"))

		if err != nil {
			return nil, fmt.Errorf("Invalid ?row-group-length= property, %w", err)
		}

		row_group_length = v
	}

	append_properties := q["append-property"]

	convert_options := &geojson.ConvertOptions{
		MinFeatures:    min,
		MaxFeatures:    max,
		Compression:    compression,
		RowGroupLength: row_group_length,
	}

	buffer := make([]*geo.Feature, 0)

	gpq := &GeoParquetWriter{
		convert_options:   convert_options,
		io_writer:         io_writer,
		buffer:            buffer,
		append_properties: append_properties,
	}

	return gpq, nil
}

// Write derives a `whosonfirst/go-whosonfirst-spr/v2.StandardPlaceResponse` and `whosonfirst/gpq-fork/not-internal/geo.Feature`
// instances from 'r' assigning the former to the latter's `Properties` element and then writes the feature to a GeoParquet database
// defined in the constructor.
func (gpq *GeoParquetWriter) Write(ctx context.Context, key string, r io.ReadSeeker) (int64, error) {

	body, err := io.ReadAll(r)

	if err != nil {
		return 0, fmt.Errorf("Failed to read body for %s, %w", key, err)
	}

	wof_spr, err := spr.WhosOnFirstSPR(body)

	if err != nil {

		alt_spr, err := spr.WhosOnFirstAltSPR(body)

		if err != nil {
			return 0, fmt.Errorf("Failed to derive SPR from %s, %w", key, err)
		}

		wof_spr = alt_spr
	}

	spr_map, err := spr_util.SPRToMap(wof_spr)

	if err != nil {
		return 0, fmt.Errorf("Failed to convert SPR to map for %s, %w", key, err)
	}

	if len(gpq.append_properties) > 0 {

		for _, rel_path := range gpq.append_properties {

			abs_path := fmt.Sprintf("properties.%s", rel_path)
			rsp := gjson.GetBytes(body, abs_path)

			spr_map[rel_path] = rsp.String()
		}
	}

	var f *geo.Feature

	err = json.Unmarshal(body, &f)

	if err != nil {
		return 0, fmt.Errorf("Failed to unmarshal Feature from %s, %w", key, err)
	}

	gpq_props := make(map[string]any)

	for k, v := range spr_map {
		gpq_props[k] = v
	}

	f.Properties = gpq_props

	ready, err := gpq.ensureFeatureWriter(ctx, f)

	if err != nil {
		return 0, fmt.Errorf("Failed to ensure feature writer (%s), %w", key, err)
	}

	if !ready {
		gpq.buffer = append(gpq.buffer, f)
		return -1, nil
	}

	err = gpq.flushBuffer(ctx)

	if err != nil {
		return -1, fmt.Errorf("Failed to flush pending buffer (%s), %w", key, err)
	}

	err = gpq.feature_writer.Write(f)

	if err != nil {
		return 0, fmt.Errorf("Failed to write %s, %w", key, err)
	}

	return -1, nil
}

// Close will close the underlying GeoParquet database.
func (gpq *GeoParquetWriter) Close(ctx context.Context) error {

	err := gpq.flushBuffer(ctx)

	if err != nil {
		return fmt.Errorf("Failed to flush buffer, %w", err)
	}

	if gpq.feature_writer != nil {
		err := gpq.feature_writer.Close()

		if err != nil {
			return fmt.Errorf("Failed to close feature writer, %w", err)
		}
	}

	return nil
}

func (gpq *GeoParquetWriter) ensureFeatureWriter(ctx context.Context, f *geo.Feature) (bool, error) {

	if gpq.feature_writer != nil {
		return true, nil
	}

	builder := pqutil.NewArrowSchemaBuilder()

	builder.Add(f.Properties)

	if !builder.Ready() {
		return false, nil
	}

	sc, err := builder.Schema()

	if err != nil {
		return true, err
	}

	var pqWriterProps *parquet.WriterProperties
	var writerOptions []parquet.WriterProperty

	if gpq.convert_options.Compression != "" {
		compression, err := pqutil.GetCompression(gpq.convert_options.Compression)
		if err != nil {
			return true, err
		}
		writerOptions = append(writerOptions, parquet.WithCompression(compression))
	}
	if gpq.convert_options.RowGroupLength > 0 {
		writerOptions = append(writerOptions, parquet.WithMaxRowGroupLength(int64(gpq.convert_options.RowGroupLength)))
	}
	if len(writerOptions) > 0 {
		pqWriterProps = parquet.NewWriterProperties(writerOptions...)
	}

	gpq_wr, err := geoparquet.NewFeatureWriter(&geoparquet.WriterConfig{
		Writer:             gpq.io_writer,
		ArrowSchema:        sc,
		ParquetWriterProps: pqWriterProps,
	})

	if err != nil {
		return true, err
	}

	gpq.feature_writer = gpq_wr
	return true, nil
}

func (gpq *GeoParquetWriter) flushBuffer(ctx context.Context) error {

	if len(gpq.buffer) == 0 {
		return nil
	}

	if gpq.feature_writer == nil {
		return fmt.Errorf("Unable to flush records, feature writer not instantiated")
	}

	for _, f_buf := range gpq.buffer {

		err := gpq.feature_writer.Write(f_buf)

		if err != nil {
			return fmt.Errorf("Failed to write buffered feature, %w", err)
		}
	}

	gpq.buffer = make([]*geo.Feature, 0)
	return nil
}
