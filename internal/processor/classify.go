package processor

import (
	"errors"
	"fmt"

	"github.com/lib/pq"
)

// classify decides whether a store error is worth retrying.
//
// Most database errors are about the database: it is down, slow, restarting,
// out of connections, or missing a table after a bad deploy. Those are
// retried in place, because they will stop, and skipping records while they
// last would throw data away.
//
// A few are about the record. Postgres class 22 (data exception: a NUL byte
// in text, a value out of range, a timestamp it can't represent) and class 23
// (integrity constraint violation) mean this record will be rejected however
// many times it is offered. Retrying it forever would stall its partition, so
// it is marked as poison instead.
//
// Class 42 (undefined table or column, syntax error) is deliberately not
// here. It means the schema and the code disagree, which affects every
// record, and the right response is to stop and be noticed, not to
// dead-letter the whole topic.
func classify(err error) error {
	if err == nil || errors.Is(err, errPoison) {
		return err
	}
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		switch pqErr.Code.Class() {
		case "22", "23":
			return fmt.Errorf("%w: database rejected the record (%s %s): %s",
				errPoison, pqErr.Code, pqErr.Code.Name(), pqErr.Message)
		}
	}
	return err
}
