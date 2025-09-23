package domain_test

import (
	"testing"

	"github.com/comfforts/logger"
	"github.com/hankgalt/batch-orchestra/pkg/domain"
	"github.com/stretchr/testify/require"
)

func TestBatchOffset(t *testing.T) {
	l := logger.GetSlogLogger()

	type User struct {
		domain.WithId
		Name string
	}

	printType := func(v any) {
		switch v := v.(type) {
		case int, int8, int16, int32, int64:
			l.Debug("Integer type", "value", v)
		case uint, uint8, uint16, uint32, uint64:
			l.Debug("Unsigned integer type", "value", v)
		case float32, float64:
			l.Debug("Float type", "value", v)
		case string:
			l.Debug("String type", "value", v)
		case domain.CustomOffset[domain.HasId]:
			l.Debug("CustomOffset[HasId] type", "value", v, "Id", v.Val.GetId())
		default:
			l.Debug("Unknown type", "value", v)
		}
	}

	id := "123"
	u := User{WithId: domain.WithId{Id: id}, Name: "Alice"}
	co := domain.CustomOffset[domain.HasId]{Val: u}
	require.Equal(t, id, co.Val.GetId())
	varInt64 := int64(123)

	printType(varInt64)
	printType(id)
	printType(co)
}
