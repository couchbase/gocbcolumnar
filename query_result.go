package cbcolumnar

import (
	"encoding/json"
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

// Err returns any errors that have occurred on the stream.
func (r *QueryResult) Err() error {
	if r.reader == nil {
		return ErrClosed
	}

	err := r.reader.Err()
	if err != nil {
		return err
	}

	return nil
}

// MetaData returns any meta-data that was available from this query.  Note that
// the meta-data will only be available once the object has been closed (either
// implicitly or explicitly).
func (r *QueryResult) MetaData() (*QueryMetadata, error) {
	meta, err := r.reader.MetaData()
	if err != nil {
		return nil, err
	}

	return meta, nil
}

type QueryResultRow struct {
	rowBytes []byte

	unmarshaler Unmarshaler
}

func (qrr *QueryResultRow) ContentAs(valuePtr any) error {
	// We don't need to convert this error, if it's ours then we already have.
	// If it's the users then we don't want to interfere with it.
	return qrr.unmarshaler.Unmarshal(qrr.rowBytes, &valuePtr) // nolint:wrapcheck
}

func BufferQueryResult(result *QueryResult) ([]QueryResultRow, *QueryMetadata, error) {
	if result == nil {
		return nil, nil, invalidArgumentError{
			ArgumentName: "result",
			Reason:       "result cannot be nil",
		}
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

// IterateQueryResult results will iterate over all rows in the result set and call the handler for each row.
// This provides a push based approach to streaming the results.
// Note that the result stream is already bound to context.Context so this function does not take a context.
func IterateQueryResult(result *QueryResult, handler RowHandler) (*QueryMetadata, error) {
	if result == nil {
		return nil, invalidArgumentError{
			ArgumentName: "result",
			Reason:       "result cannot be nil",
		}
	}

	if err := iterateResults(result, handler); err != nil {
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

func iterateResults(result *QueryResult, handler RowHandler) error {
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

	for row := range rowCh {
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
	}

	return nil
}

type analyticsRowReader interface {
	NextRow() []byte
	MetaData() (*QueryMetadata, error)
	Close() error
	Err() error
}
