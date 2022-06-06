//go:build !fast
// +build !fast

package table_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

type issue229Struct struct{}

// UnmarshalJSON implements json.Unmarshaler
func (i *issue229Struct) UnmarshalJSON(_ []byte) error {
	return nil
}

func TestIssue229UnexpectedNullWhileParseNilJsonDocumentValue(t *testing.T) {
	// https://github.com/ydb-platform/ydb-go-sdk/issues/229
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db := connect(t)
	err := db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
		res, err := tx.Execute(ctx, `SELECT Nothing(JsonDocument?) AS r`, nil)
		require.NoError(t, err)
		require.NoError(t, res.NextResultSetErr(ctx))
		require.True(t, res.NextRow())

		var val issue229Struct
		require.NoError(t, res.Scan(&val))
		return nil
	})
	require.NoError(t, err)
}

func connect(t *testing.T) ydb.Connection {
	db, err := ydb.Open(
		context.Background(),
		os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithAccessTokenCredentials(os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS")))
	require.NoError(t, err)
	return db
}

func TestIssue259IntervalFromDuration(t *testing.T) {
	// https://github.com/ydb-platform/ydb-go-sdk/issues/259
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db := connect(t)
	err := db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
		res, err := tx.Execute(ctx, `DECLARE $ts as Interval;
			$ten_micro = CAST(10 as Interval);
			SELECT $ts == $ten_micro, $ten_micro;`, table.NewQueryParameters(
			table.ValueParam(`$ts`, types.IntervalValueFromDuration(10*time.Microsecond)),
		))
		require.NoError(t, err)
		require.NoError(t, res.NextResultSetErr(ctx))
		require.True(t, res.NextRow())

		var (
			val      bool
			tenMicro time.Duration
		)
		require.NoError(t, res.Scan(&val, &tenMicro))
		require.True(t, val)
		require.Equal(t, 10*time.Microsecond, tenMicro)
		return nil
	}, table.WithTxSettings(table.TxSettings(table.WithSerializableReadWrite())))
	require.NoError(t, err)
}
