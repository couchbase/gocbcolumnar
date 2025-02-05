package cbcolumnar

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// QueryMetrics encapsulates various metrics gathered during a queries execution.
type QueryMetrics struct {
	ElapsedTime      time.Duration
	ExecutionTime    time.Duration
	ResultCount      uint64
	ResultSize       uint64
	ProcessedObjects uint64
}

// QueryWarning encapsulates any warnings returned by a query.
type QueryWarning struct {
	Code    uint32
	Message string
}

// QueryMetadata provides access to the meta-data properties of a query result.
type QueryMetadata struct {
	RequestID string
	Metrics   QueryMetrics
	Warnings  []QueryWarning
}

// QueryResult allows access to the results of a query.
type QueryResult struct {
	reader analyticsRowReader

	unmarshaler Unmarshaler
}

func (r *QueryResult) NextRow() *QueryResultRow {
	rowBytes := r.reader.NextRow()
	if rowBytes == nil {
		return nil
	}

	return &QueryResultRow{
		rowBytes:    rowBytes,
		unmarshaler: r.unmarshaler,
	}
}

// // Next assigns the next result from the results into the value pointer, returning whether the read was successful.
// func (r *QueryResultRows) Next() bool {
// 	if r.reader == nil {
// 		return false
// 	}
//
// 	rowBytes := r.reader.NextRow()
// 	if rowBytes == nil {
// 		return false
// 	}
//
// 	r.rowBytes = rowBytes
//
// 	return true
// }
//
// // Row returns the value of the current row.
// func (r *QueryResultRows) Row(valuePtr interface{}) error {
// 	if r.rowBytes == nil {
// 		return maybeEnhanceQueryError(ErrNoResult)
// 	}
//
// 	if bytesPtr, ok := valuePtr.(*json.RawMessage); ok {
// 		*bytesPtr = r.rowBytes
//
// 		return nil
// 	}
//
// 	return json.Unmarshal(r.rowBytes, valuePtr)
// }

// Err returns any errors that have occurred on the stream.
func (r *QueryResult) Err() error {
	if r.reader == nil {
		return ErrClosed
	}

	err := r.reader.Err()
	if err != nil {
		return maybeEnhanceQueryError(err)
	}

	return nil
}

// MetaData returns any meta-data that was available from this query.  Note that
// the meta-data will only be available once the object has been closed (either
// implicitly or explicitly).
func (r *QueryResult) MetaData() (*QueryMetadata, error) {
	return r.reader.MetaData()
}

type QueryResultRow struct {
	rowBytes []byte

	unmarshaler Unmarshaler
}

func (qrr *QueryResultRow) Content(valuePtr any) error {
	err := qrr.unmarshaler.Unmarshal(qrr.rowBytes, &valuePtr)
	if err != nil {
		return err
	}

	return nil
}

func BufferQueryResult(result *QueryResult) ([]QueryResultRow, *QueryMetadata, error) {
	if result == nil {
		return nil, nil, makeError(errors.New("result cannot be nil"), nil)
	}

	var buffered []QueryResultRow

	row := result.NextRow()
	for row != nil {
		buffered = append(buffered, *row)

		row = result.NextRow()
	}

	meta, err := result.MetaData()
	if err != nil {
		return nil, nil, err
	}

	err = result.Err()
	if err != nil {
		return nil, nil, err
	}

	return buffered, meta, nil
}

type RowHandler func(row *QueryResultRow) error

func IterateQueryResult(ctx context.Context, result *QueryResult, handler RowHandler) (*QueryMetadata, error) {
	if result == nil {
		return nil, makeError(errors.New("result cannot be nil"), nil)
	}

	if err := iterateResults(ctx, result, handler); err != nil {
		return nil, err
	}

	if err := result.reader.Err(); err != nil {
		return nil, err
	}

	meta, err := result.reader.MetaData()
	if err != nil {
		return nil, err
	}

	return meta, nil
}

func iterateResults(ctx context.Context, result *QueryResult, handler RowHandler) error {
	rowCh := make(chan json.RawMessage, 1)

	go func() {
		for {
			row := result.reader.NextRow()
			if row == nil {
				break
			}

			rowCh <- row
		}

		close(rowCh)
	}()

	for {
		select {
		case <-ctx.Done():
			result.reader.Close()

			return ctx.Err()
		case row, ok := <-rowCh:
			if len(row) > 0 {
				err := handler(&QueryResultRow{
					rowBytes:    row,
					unmarshaler: result.unmarshaler,
				})
				if err != nil {
					result.reader.Close()

					return err
				}
			}

			if !ok {
				return nil
			}
		}
	}
}

type analyticsRowReader interface {
	NextRow() []byte
	MetaData() (*QueryMetadata, error)
	Close() error
	Err() error
}
