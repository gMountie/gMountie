package utils

import (
	"testing"

	"go.gmountie.dev/gmountie/pkg/utils/log"

	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
)

func Must0(t testing.TB, err error) {
	if err != nil {
		log.Log.Error("Unexpected error", zap.Error(err))
		t.FailNow()
	}
}

func Must1[T any](suite suite.TestingSuite, obj T, err error) T {
	if err != nil {
		log.Log.Error("Unexpected error", zap.Error(err))
		suite.T().FailNow()
	}
	return obj
}
