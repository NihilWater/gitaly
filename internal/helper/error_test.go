package helper

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v15/internal/testhelper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// unusedErrorCode is any error code that we don't have any wrapping functions for yet. This is used
// to verify that we correctly wrap errors that already have a different gRPC error code than the
// one under test.
const unusedErrorCode = codes.OutOfRange

func TestError(t *testing.T) {
	errorMessage := "sentinel error"
	input := errors.New(errorMessage)
	inputGRPC := status.Error(unusedErrorCode, errorMessage)
	inputInternalGRPC := ErrAbortedf(errorMessage)

	for _, tc := range []struct {
		desc   string
		errorf func(err error) error
		code   codes.Code
	}{
		{
			desc:   "Canceled",
			errorf: ErrCanceled,
			code:   codes.Canceled,
		},
		{
			desc:   "DeadlineExceeded",
			errorf: ErrDeadlineExceeded,
			code:   codes.DeadlineExceeded,
		},
		{
			desc:   "Internal",
			errorf: ErrInternal,
			code:   codes.Internal,
		},
		{
			desc:   "InvalidArgument",
			errorf: ErrInvalidArgument,
			code:   codes.InvalidArgument,
		},
		{
			desc:   "FailedPrecondition",
			errorf: ErrFailedPrecondition,
			code:   codes.FailedPrecondition,
		},
		{
			desc:   "NotFound",
			errorf: ErrNotFound,
			code:   codes.NotFound,
		},
		{
			desc:   "Unavailable",
			errorf: ErrUnavailable,
			code:   codes.Unavailable,
		},
		{
			desc:   "AlreadyExists",
			errorf: ErrAlreadyExists,
			code:   codes.AlreadyExists,
		},
		{
			desc:   "Aborted",
			errorf: ErrAborted,
			code:   codes.Aborted,
		},
		{
			desc:   "Unauthenticated",
			errorf: ErrUnauthenticated,
			code:   codes.Unauthenticated,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			// tc.code and our canary test code must not
			// clash!
			require.NotEqual(t, tc.code, unusedErrorCode)

			// When not re-throwing an error we get the
			// GRPC error code corresponding to the
			// function's name.
			err := tc.errorf(input)
			require.EqualError(t, err, errorMessage)
			require.False(t, errors.Is(err, inputGRPC))
			require.Equal(t, tc.code, status.Code(err))

			// When re-throwing an error an existing GRPC
			// error code will get preserved, instead of
			// the one corresponding to the function's
			// name.
			err = tc.errorf(inputGRPC)
			require.True(t, errors.Is(err, inputGRPC))
			require.False(t, errors.Is(err, input))
			require.Equal(t, unusedErrorCode, status.Code(err))
			require.NotEqual(t, tc.code, status.Code(inputGRPC))

			// Wrapped gRPC error (internal.status.Error) code will get
			// preserved, instead of the one corresponding to the function's
			// name.
			err = tc.errorf(fmt.Errorf("outer: %w", inputGRPC))
			require.True(t, errors.Is(err, inputGRPC))
			require.False(t, errors.Is(err, input))
			require.Equal(t, unusedErrorCode, status.Code(err))
			require.NotEqual(t, tc.code, status.Code(inputGRPC))

			if tc.code != codes.Aborted {
				// Wrapped gRPC error code constructed with helpers will get
				// preserved, instead of the one corresponding to the function's
				// name.
				err = tc.errorf(fmt.Errorf("outer: %w", inputInternalGRPC))
				require.True(t, errors.Is(err, inputInternalGRPC))
				require.False(t, errors.Is(err, input))
				require.Equal(t, codes.Aborted, status.Code(err))
				require.NotEqual(t, tc.code, status.Code(inputInternalGRPC))
			}
		})
	}
}

func TestErrorf(t *testing.T) {
	for _, tc := range []struct {
		desc         string
		errorf       func(format string, a ...interface{}) error
		expectedCode codes.Code
	}{
		{
			desc:         "Canceledf",
			errorf:       ErrCanceledf,
			expectedCode: codes.Canceled,
		},
		{
			desc:         "DeadlineExceededf",
			errorf:       ErrDeadlineExceededf,
			expectedCode: codes.DeadlineExceeded,
		},
		{
			desc:         "Internalf",
			errorf:       ErrInternalf,
			expectedCode: codes.Internal,
		},
		{
			desc:         "InvalidArgumentf",
			errorf:       ErrInvalidArgumentf,
			expectedCode: codes.InvalidArgument,
		},
		{
			desc:         "FailedPreconditionf",
			errorf:       ErrFailedPreconditionf,
			expectedCode: codes.FailedPrecondition,
		},
		{
			desc:         "NotFoundf",
			errorf:       ErrNotFoundf,
			expectedCode: codes.NotFound,
		},
		{
			desc:         "ErrUnavailablef",
			errorf:       ErrUnavailablef,
			expectedCode: codes.Unavailable,
		},
		{
			desc:         "ErrAbortedf",
			errorf:       ErrAbortedf,
			expectedCode: codes.Aborted,
		},
		{
			desc:         "ErrDataLossf",
			errorf:       ErrDataLossf,
			expectedCode: codes.DataLoss,
		},
		{
			desc:         "ErrUnknownf",
			errorf:       ErrUnknownf,
			expectedCode: codes.Unknown,
		},
		{
			desc:         "ErrUnimplementedf",
			errorf:       ErrUnimplementedf,
			expectedCode: codes.Unimplemented,
		},
		{
			desc:         "ErrUnauthenticatedf",
			errorf:       ErrUnauthenticatedf,
			expectedCode: codes.Unauthenticated,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Run("with non-gRPC error", func(t *testing.T) {
				err := tc.errorf("top-level: %w", errors.New("nested"))
				require.EqualError(t, err, "top-level: nested")
				require.Equal(t, tc.expectedCode, status.Code(err))

				s, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, status.New(tc.expectedCode, "top-level: nested"), s)
			})

			t.Run("with status.Errorf error and %v", func(t *testing.T) {
				require.NotEqual(t, tc.expectedCode, unusedErrorCode)

				err := tc.errorf("top-level: %v", status.Errorf(unusedErrorCode, "deeply: %s", "nested"))
				require.EqualError(t, err, "top-level: deeply: nested")

				// The error code of the nested error should be discarded.
				require.Equal(t, tc.expectedCode, status.Code(err))
				s, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, status.New(tc.expectedCode, "top-level: deeply: nested"), s)
			})

			t.Run("with status.Errorf error and %w", func(t *testing.T) {
				require.NotEqual(t, tc.expectedCode, unusedErrorCode)

				err := tc.errorf("top-level: %w", status.Errorf(unusedErrorCode, "deeply: %s", "nested"))
				require.EqualError(t, err, "top-level: deeply: nested")

				// We should be reporting the error code of the nested error.
				require.Equal(t, unusedErrorCode, status.Code(err))
				s, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, status.New(unusedErrorCode, "top-level: deeply: nested"), s)
			})

			t.Run("with status.Error error and %v", func(t *testing.T) {
				require.NotEqual(t, tc.expectedCode, unusedErrorCode)

				err := tc.errorf("top-level: %v", status.Error(unusedErrorCode, "nested"))
				require.EqualError(t, err, "top-level: nested")

				// The error code of the nested error should be discarded.
				require.Equal(t, tc.expectedCode, status.Code(err))
				s, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, status.New(tc.expectedCode, "top-level: nested"), s)
			})

			t.Run("with status.Error error and %w", func(t *testing.T) {
				require.NotEqual(t, tc.expectedCode, unusedErrorCode)

				err := tc.errorf("top-level: %w", status.Error(unusedErrorCode, "nested"))
				require.EqualError(t, err, "top-level: nested")

				// We should be reporting the error code of the nested error.
				require.Equal(t, unusedErrorCode, status.Code(err))
				s, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, status.New(unusedErrorCode, "top-level: nested"), s)
			})

			t.Run("multi-nesting gRPC errors", func(t *testing.T) {
				require.NotEqual(t, tc.expectedCode, unusedErrorCode)

				err := tc.errorf("first: %w",
					ErrInternalf("second: %w",
						status.Error(unusedErrorCode, "third"),
					),
				)
				require.EqualError(t, err, "first: second: third")

				// We should be reporting the error code of the nested error.
				require.Equal(t, unusedErrorCode, status.Code(err))
				s, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, status.New(unusedErrorCode, "first: second: third"), s)
			})

			t.Run("multi-nesting with standard error wrapping", func(t *testing.T) {
				require.NotEqual(t, tc.expectedCode, unusedErrorCode)

				err := tc.errorf("first: %w",
					fmt.Errorf("second: %w",
						formatError(unusedErrorCode, "third"),
					),
				)
				require.EqualError(t, err, "first: second: third")

				// We should be reporting the error code of the nested error.
				require.Equal(t, unusedErrorCode, status.Code(err))
				s, ok := status.FromError(err)
				require.True(t, ok)
				require.Equal(t, status.New(unusedErrorCode, "first: second: third"), s)
			})
		})
	}
}

func TestErrWithDetails(t *testing.T) {
	for _, tc := range []struct {
		desc            string
		err             error
		details         []proto.Message
		expectedErr     error
		expectedMessage string
		expectedDetails []proto.Message
		expectedCode    codes.Code
	}{
		{
			desc:        "no error",
			expectedErr: errors.New("no error given"),
		},
		{
			desc:        "status with OK code",
			err:         status.Error(codes.OK, "message"),
			expectedErr: errors.New("no error given"),
		},
		{
			desc:        "normal error",
			err:         errors.New("message"),
			expectedErr: errors.New("error is not a gRPC status"),
		},
		{
			desc:            "status",
			err:             status.Error(codes.FailedPrecondition, "message"),
			expectedMessage: "rpc error: code = FailedPrecondition desc = message",
			expectedCode:    codes.FailedPrecondition,
		},
		{
			desc: "status with details",
			err:  status.Error(codes.FailedPrecondition, "message"),
			details: []proto.Message{
				&gitalypb.Repository{},
			},
			expectedMessage: "rpc error: code = FailedPrecondition desc = message",
			expectedCode:    codes.FailedPrecondition,
			expectedDetails: []proto.Message{
				&gitalypb.Repository{},
			},
		},
		{
			desc: "status with multiple details",
			err:  status.Error(codes.FailedPrecondition, "message"),
			details: []proto.Message{
				&gitalypb.Repository{RelativePath: "a"},
				&gitalypb.Repository{RelativePath: "b"},
			},
			expectedMessage: "rpc error: code = FailedPrecondition desc = message",
			expectedCode:    codes.FailedPrecondition,
			expectedDetails: []proto.Message{
				&gitalypb.Repository{RelativePath: "a"},
				&gitalypb.Repository{RelativePath: "b"},
			},
		},
		{
			desc: "status with mixed type details",
			err:  status.Error(codes.FailedPrecondition, "message"),
			details: []proto.Message{
				&gitalypb.Repository{},
				&gitalypb.GitCommit{},
			},
			expectedMessage: "rpc error: code = FailedPrecondition desc = message",
			expectedCode:    codes.FailedPrecondition,
			expectedDetails: []proto.Message{
				&gitalypb.Repository{},
				&gitalypb.GitCommit{},
			},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			detailedErr, err := ErrWithDetails(tc.err, tc.details...)
			require.Equal(t, tc.expectedErr, err)
			if err != nil {
				return
			}

			require.Equal(t, tc.expectedMessage, detailedErr.Error())

			st, ok := status.FromError(detailedErr)
			require.True(t, ok, "error should be a status")
			require.Equal(t, tc.expectedCode, st.Code())

			statusProto := st.Proto()
			require.NotNil(t, statusProto)

			var details []proto.Message
			for _, detail := range statusProto.GetDetails() {
				detailProto, err := detail.UnmarshalNew()
				require.NoError(t, err)
				details = append(details, detailProto)
			}
			testhelper.ProtoEqual(t, tc.expectedDetails, details)
		})
	}
}

func TestGrpcCode(t *testing.T) {
	t.Parallel()
	for desc, tc := range map[string]struct {
		in  error
		exp codes.Code
	}{
		"unwrapped status": {
			in:  status.Error(codes.NotFound, ""),
			exp: codes.NotFound,
		},
		"wrapped status": {
			in:  fmt.Errorf("context: %w", status.Error(codes.NotFound, "")),
			exp: codes.NotFound,
		},
		"unwrapped status created by helpers": {
			in:  ErrNotFoundf(""),
			exp: codes.NotFound,
		},
		"wrapped status created by helpers": {
			in:  fmt.Errorf("context: %w", ErrNotFoundf("")),
			exp: codes.NotFound,
		},
		"double wrapped status created by helpers": {
			in:  fmt.Errorf("outer: %w", fmt.Errorf("context: %w", ErrNotFoundf(""))),
			exp: codes.NotFound,
		},
		"double helper wrapped status": {
			in:  ErrAbortedf("outer: %w", fmt.Errorf("context: %w", ErrNotFoundf(""))),
			exp: codes.NotFound,
		},
		"nil input": {
			in:  nil,
			exp: codes.OK,
		},
		"no code defined": {
			in:  assert.AnError,
			exp: codes.Unknown,
		},
	} {
		t.Run(desc, func(t *testing.T) {
			assert.Equal(t, tc.exp, GrpcCode(tc.in))
		})
	}
}
