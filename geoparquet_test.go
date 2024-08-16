package geoparquet

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/whosonfirst/go-ioutil"
	"github.com/whosonfirst/go-writer/v3"
	"github.com/whosonfirst/gpq-fork/not-internal/validator"
)

func TestGeoParquetWriter(t *testing.T) {

	ctx := context.Background()

	data_fs := os.DirFS("fixtures/data")

	f, err := os.CreateTemp("", "wof.*.geoparquet")

	if err != nil {
		t.Fatalf("Failed to create temp file, %v", err)
	}

	err = f.Close()

	if err != nil {
		t.Fatalf("Failed to close temp file, %v", err)
	}

	temp_path := f.Name()
	temp_name := filepath.Base(temp_path)

	defer os.Remove(temp_path)

	wr_uri := fmt.Sprintf("geoparquet://%s?append-property=sfomuseum:placetype", temp_path)

	wr, err := writer.NewWriter(ctx, wr_uri)

	if err != nil {
		t.Fatalf("Failed to create new writer for %s, %v", wr_uri, err)
	}

	err = fs.WalkDir(data_fs, ".", func(path string, d fs.DirEntry, err error) error {

		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		if strings.HasSuffix(path, "~") {
			return nil
		}

		r, err := data_fs.Open(path)

		if err != nil {
			return fmt.Errorf("Failed to open %s for reading, %w", path, err)
		}

		rsc, err := ioutil.NewReadSeekCloser(r)

		if err != nil {
			return fmt.Errorf("Failed to create ReadSeekCloser for %s, %w", path, err)
		}

		defer rsc.Close()

		key := filepath.Base(path)

		_, err = wr.Write(ctx, key, rsc)

		if err != nil {
			return fmt.Errorf("Failed to write %s, %w", path, err)
		}

		fmt.Println(path)
		return nil
	})

	if err != nil {
		t.Fatalf("Failed to walk data dir, %v", err)
	}

	err = wr.Close(ctx)

	if err != nil {
		t.Fatalf("Failed to close writer, %v", err)
	}

	r, err := os.Open(temp_path)

	if err != nil {
		t.Fatalf("Failed to open temp path %s for validation, %v", temp_path, err)
	}

	defer r.Close()

	v := validator.New(false)

	_, err = v.Validate(ctx, r, temp_name)

	if err != nil {
		t.Fatalf("Failed to validate %s, %v", temp_path, err)
	}
}
