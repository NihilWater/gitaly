package helper

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

type statusWrapper struct {
	error
	status *status.Status
}

func (sw statusWrapper) GRPCStatus() *status.Status {
	return sw.status
}

func (sw statusWrapper) Unwrap() error {
	return sw.error
}

// ErrCanceled wraps err with codes.Canceled, unless err is already a gRPC error.
func ErrCanceled(err error) error { return wrapError(codes.Canceled, err) }

// ErrDeadlineExceeded wraps err with codes.DeadlineExceeded, unless err is already a gRPC error.
func ErrDeadlineExceeded(err error) error { return wrapError(codes.DeadlineExceeded, err) }

// ErrInternal wraps err with codes.Internal, unless err is already a gRPC error.
func ErrInternal(err error) error { return wrapError(codes.Internal, err) }

// ErrInvalidArgument wraps err with codes.InvalidArgument, unless err is already a gRPC error.
func ErrInvalidArgument(err error) error { return wrapError(codes.InvalidArgument, err) }

// ErrNotFound wraps error with codes.NotFound, unless err is already a gRPC error.
func ErrNotFound(err error) error { return wrapError(codes.NotFound, err) }

// ErrFailedPrecondition wraps err with codes.FailedPrecondition, unless err is already a gRPC
// error.
func ErrFailedPrecondition(err error) error { return wrapError(codes.FailedPrecondition, err) }

// ErrUnavailable wraps err with codes.Unavailable, unless err is already a gRPC error.
func ErrUnavailable(err error) error { return wrapError(codes.Unavailable, err) }

// ErrPermissionDenied wraps err with codes.PermissionDenied, unless err is already a gRPC error.
func ErrPermissionDenied(err error) error { return wrapError(codes.PermissionDenied, err) }

// ErrAlreadyExists wraps err with codes.AlreadyExists, unless err is already a gRPC error.
func ErrAlreadyExists(err error) error { return wrapError(codes.AlreadyExists, err) }

// ErrAborted wraps err with codes.Aborted, unless err is already a gRPC error.
func ErrAborted(err error) error { return wrapError(codes.Aborted, err) }

// ErrUnauthenticated wraps err with codes.Unauthenticated, unless err is already a gRPC error.
func ErrUnauthenticated(err error) error { return wrapError(codes.Unauthenticated, err) }

// wrapError wraps the given error with the error code unless it's already a gRPC error. If given
// nil it will return nil.
func wrapError(code codes.Code, err error) error {
	if err == nil {
		return nil
	}

	_, ok := status.FromError(err)
	if ok {
		return err
	}

	if foundCode := GrpcCode(err); foundCode != codes.OK && foundCode != codes.Unknown {
		code = foundCode
	}

	return statusWrapper{error: err, status: status.New(code, err.Error())}
}

// ErrCanceledf wraps a formatted error with codes.Canceled, unless the formatted error is a
// wrapped gRPC error.
func ErrCanceledf(format string, a ...interface{}) error {
	return formatError(codes.Canceled, format, a...)
}

// ErrDeadlineExceededf wraps a formatted error with codes.DeadlineExceeded, unless the formatted
// error is a wrapped gRPC error.
func ErrDeadlineExceededf(format string, a ...interface{}) error {
	return formatError(codes.DeadlineExceeded, format, a...)
}

// ErrInternalf wraps a formatted error with codes.Internal, unless the formatted error is a
// wrapped gRPC error.
func ErrInternalf(format string, a ...interface{}) error {
	return formatError(codes.Internal, format, a...)
}

// ErrInvalidArgumentf wraps a formatted error with codes.InvalidArgument, unless the formatted
// error is a wrapped gRPC error.
func ErrInvalidArgumentf(format string, a ...interface{}) error {
	return formatError(codes.InvalidArgument, format, a...)
}

// ErrNotFoundf wraps a formatted error with codes.NotFound, unless the
// formatted error is a wrapped gRPC error.
func ErrNotFoundf(format string, a ...interface{}) error {
	return formatError(codes.NotFound, format, a...)
}

// ErrFailedPreconditionf wraps a formatted error with codes.FailedPrecondition, unless the
// formatted error is a wrapped gRPC error.
func ErrFailedPreconditionf(format string, a ...interface{}) error {
	return formatError(codes.FailedPrecondition, format, a...)
}

// ErrUnavailablef wraps a formatted error with codes.Unavailable, unless the
// formatted error is a wrapped gRPC error.
func ErrUnavailablef(format string, a ...interface{}) error {
	return formatError(codes.Unavailable, format, a...)
}

// ErrPermissionDeniedf wraps a formatted error with codes.PermissionDenied, unless the formatted
// error is a wrapped gRPC error.
func ErrPermissionDeniedf(format string, a ...interface{}) error {
	return formatError(codes.PermissionDenied, format, a...)
}

// ErrAlreadyExistsf wraps a formatted error with codes.AlreadyExists, unless the formatted error is
// a wrapped gRPC error.
func ErrAlreadyExistsf(format string, a ...interface{}) error {
	return formatError(codes.AlreadyExists, format, a...)
}

// ErrAbortedf wraps a formatted error with codes.Aborted, unless the formatted error is a wrapped
// gRPC error.
func ErrAbortedf(format string, a ...interface{}) error {
	return formatError(codes.Aborted, format, a...)
}

// ErrDataLossf wraps a formatted error with codes.DataLoss, unless the formatted error is a wrapped
// gRPC error.
func ErrDataLossf(format string, a ...interface{}) error {
	return formatError(codes.DataLoss, format, a...)
}

// ErrUnknownf wraps a formatted error with codes.Unknown, unless the formatted error is a wrapped
// gRPC error.
func ErrUnknownf(format string, a ...interface{}) error {
	return formatError(codes.Unknown, format, a...)
}

// ErrUnimplementedf wraps a formatted error with codes.Unimplemented, unless the formatted error is a wrapped
// gRPC error.
func ErrUnimplementedf(format string, a ...interface{}) error {
	return formatError(codes.Unimplemented, format, a...)
}

// ErrUnauthenticatedf wraps a formatted error with codes.Unauthenticated, unless the formatted error is a wrapped
// gRPC error.
func ErrUnauthenticatedf(format string, a ...interface{}) error {
	return formatError(codes.Unauthenticated, format, a...)
}

// grpcErrorMessageWrapper is used to wrap a gRPC `status.Status`-style error such that it behaves
// like a `status.Status`, except that it generates a readable error message.
type grpcErrorMessageWrapper struct {
	*status.Status
}

func (e grpcErrorMessageWrapper) GRPCStatus() *status.Status {
	return e.Status
}

func (e grpcErrorMessageWrapper) Error() string {
	return e.Message()
}

func (e grpcErrorMessageWrapper) Unwrap() error {
	return e.Status.Err()
}

// formatError will create a new error from the given format string. If the error string contains a
// %w verb and its corresponding error has a gRPC error code, then the returned error will keep this
// gRPC error code instead of using the one provided as an argument.
func formatError(code codes.Code, format string, a ...interface{}) error {
	args := make([]interface{}, 0, len(a))
	for _, a := range a {
		err, ok := a.(error)
		if !ok {
			args = append(args, a)
			continue
		}

		status, ok := status.FromError(err)
		if !ok {
			args = append(args, a)
			continue
		}

		// Wrap gRPC status errors so that the resulting error message stays readable.
		args = append(args, grpcErrorMessageWrapper{status})
	}

	err := fmt.Errorf(format, args...)

	for current := err; current != nil; current = errors.Unwrap(current) {
		nestedCode := GrpcCode(current)
		if nestedCode != codes.OK && nestedCode != codes.Unknown {
			code = nestedCode
		}
	}

	return statusWrapper{err, status.New(code, err.Error())}
}

// ErrWithDetails adds the given details to the error if it is a gRPC status whose code is not OK.
func ErrWithDetails(err error, details ...proto.Message) (error, error) {
	if GrpcCode(err) == codes.OK {
		return nil, fmt.Errorf("no error given")
	}

	st, ok := status.FromError(err)
	if !ok {
		return nil, fmt.Errorf("error is not a gRPC status")
	}

	proto := st.Proto()
	for _, detail := range details {
		marshaled, err := anypb.New(detail)
		if err != nil {
			return nil, err
		}

		proto.Details = append(proto.Details, marshaled)
	}

	return statusWrapper{err, status.FromProto(proto)}, nil
}

// GrpcCode translates errors into codes.Code values.
// It unwraps the nested errors until it finds the most nested one that returns the codes.Code.
// If err is nil it returns codes.OK.
// If no codes.Code found it returns codes.Unknown.
func GrpcCode(err error) codes.Code {
	if err == nil {
		return codes.OK
	}

	code := codes.Unknown
	for ; err != nil; err = errors.Unwrap(err) {
		st, ok := status.FromError(err)
		if ok {
			code = st.Code()
		}
	}

	return code
}
