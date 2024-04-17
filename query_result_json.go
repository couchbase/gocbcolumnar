// nolint
package cbcolumnar

import (
	"encoding/json"
	"time"
)

// Code in this file will be required soon.

type jsonAnalyticsMetrics struct {
	ElapsedTime      string `json:"elapsedTime"`
	ExecutionTime    string `json:"executionTime"`
	ResultCount      uint64 `json:"resultCount"`
	ResultSize       uint64 `json:"resultSize"`
	MutationCount    uint64 `json:"mutationCount,omitempty"`
	SortCount        uint64 `json:"sortCount,omitempty"`
	ErrorCount       uint64 `json:"errorCount,omitempty"`
	WarningCount     uint64 `json:"warningCount,omitempty"`
	ProcessedObjects uint64 `json:"processedObjects,omitempty"`
}

type jsonAnalyticsWarning struct {
	Code    uint32 `json:"code"`
	Message string `json:"msg"`
}

type jsonAnalyticsResponse struct {
	RequestID       string                 `json:"requestID"`
	ClientContextID string                 `json:"clientContextID"`
	Status          string                 `json:"status"`
	Warnings        []jsonAnalyticsWarning `json:"warnings"`
	Metrics         jsonAnalyticsMetrics   `json:"metrics"`
	Signature       interface{}            `json:"signature"`
	Handle          string                 `json:"handle,omitempty"`
}

type jsonAnalyticsError struct {
	Code uint32 `json:"code"`
	Msg  string `json:"msg"`
}

type jsonAnalyticsErrorResponse struct {
	Errors json.RawMessage `json:"errors"`
}

func (meta *QueryMetadata) fromData(data jsonAnalyticsResponse) error {
	metrics := QueryMetrics{}
	if err := metrics.fromData(data.Metrics); err != nil {
		return err
	}

	warnings := make([]QueryWarning, len(data.Warnings))
	for wIdx, jsonWarning := range data.Warnings {
		err := warnings[wIdx].fromData(jsonWarning)
		if err != nil {
			return err
		}
	}

	meta.RequestID = data.RequestID
	meta.Metrics = metrics
	meta.Warnings = warnings

	return nil
}

func (metrics *QueryMetrics) fromData(data jsonAnalyticsMetrics) error {
	elapsedTime, err := time.ParseDuration(data.ElapsedTime)
	if err != nil {
		// logDebugf("Failed to parse query metrics elapsed time: %s", err)
	}

	executionTime, err := time.ParseDuration(data.ExecutionTime)
	if err != nil {
		// logDebugf("Failed to parse query metrics execution time: %s", err)
	}

	metrics.ElapsedTime = elapsedTime
	metrics.ExecutionTime = executionTime
	metrics.ResultCount = data.ResultCount
	metrics.ResultSize = data.ResultSize
	metrics.ProcessedObjects = data.ProcessedObjects

	return nil
}

func (warning *QueryWarning) fromData(data jsonAnalyticsWarning) error {
	warning.Code = data.Code
	warning.Message = data.Message

	return nil
}
