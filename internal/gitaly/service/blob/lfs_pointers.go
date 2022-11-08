package blob

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v15/internal/git"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/catfile"
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/gitpipe"
	"gitlab.com/gitlab-org/gitaly/v15/internal/gitaly/service"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper/chunk"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
	"google.golang.org/protobuf/proto"
)

const (
	// lfsPointerMaxSize is the maximum size for an lfs pointer text blob. This limit is used
	// as a heuristic to filter blobs which can't be LFS pointers. The format of these pointers
	// is described in https://github.com/git-lfs/git-lfs/blob/master/docs/spec.md#the-pointer.
	lfsPointerMaxSize = 200
)

// ListLFSPointers finds all LFS pointers which are transitively reachable via a graph walk of the
// given set of revisions.
func (s *server) ListLFSPointers(in *gitalypb.ListLFSPointersRequest, stream gitalypb.BlobService_ListLFSPointersServer) error {
	ctx := stream.Context()

	repository := in.GetRepository()
	if err := service.ValidateRepository(repository); err != nil {
		return err
	}
	if len(in.Revisions) == 0 {
		return helper.ErrInvalidArgumentf("missing revisions")
	}
	for _, revision := range in.Revisions {
		if strings.HasPrefix(revision, "-") && revision != "--all" && revision != "--not" {
			return helper.ErrInvalidArgumentf("invalid revision: %q", revision)
		}
	}

	chunker := chunk.New(&lfsPointerSender{
		send: func(pointers []*gitalypb.LFSPointer) error {
			return stream.Send(&gitalypb.ListLFSPointersResponse{
				LfsPointers: pointers,
			})
		},
	})

	repo := s.localrepo(repository)

	objectReader, cancel, err := s.catfileCache.ObjectReader(ctx, repo)
	if err != nil {
		return helper.ErrInternal(fmt.Errorf("creating object reader: %w", err))
	}
	defer cancel()

	revlistIter := gitpipe.Revlist(ctx, repo, in.GetRevisions(),
		gitpipe.WithObjects(),
		gitpipe.WithBlobLimit(lfsPointerMaxSize),
		gitpipe.WithObjectTypeFilter(gitpipe.ObjectTypeBlob),
	)

	catfileObjectIter, err := gitpipe.CatfileObject(ctx, objectReader, revlistIter)
	if err != nil {
		return helper.ErrInternalf("creating object iterator: %w", err)
	}

	if err := sendLFSPointers(chunker, catfileObjectIter, int(in.Limit)); err != nil {
		return err
	}

	return nil
}

// ListAllLFSPointers finds all LFS pointers which exist in the repository, including those which
// are not reachable via graph walks.
func (s *server) ListAllLFSPointers(in *gitalypb.ListAllLFSPointersRequest, stream gitalypb.BlobService_ListAllLFSPointersServer) error {
	ctx := stream.Context()

	repository := in.GetRepository()
	if err := service.ValidateRepository(repository); err != nil {
		return err
	}

	repo := s.localrepo(repository)

	chunker := chunk.New(&lfsPointerSender{
		send: func(pointers []*gitalypb.LFSPointer) error {
			return stream.Send(&gitalypb.ListAllLFSPointersResponse{
				LfsPointers: pointers,
			})
		},
	})

	objectReader, cancel, err := s.catfileCache.ObjectReader(ctx, repo)
	if err != nil {
		return helper.ErrInternalf("creating object reader: %w", err)
	}
	defer cancel()

	catfileInfoIter := gitpipe.CatfileInfoAllObjects(ctx, repo,
		gitpipe.WithSkipCatfileInfoResult(func(objectInfo *catfile.ObjectInfo) bool {
			return objectInfo.Type != "blob" || objectInfo.Size > lfsPointerMaxSize
		}),
	)

	catfileObjectIter, err := gitpipe.CatfileObject(ctx, objectReader, catfileInfoIter)
	if err != nil {
		return helper.ErrInternalf("creating object iterator: %w", err)
	}

	if err := sendLFSPointers(chunker, catfileObjectIter, int(in.Limit)); err != nil {
		return err
	}

	return nil
}

// GetLFSPointers takes the list of requested blob IDs and filters them down to blobs which are
// valid LFS pointers. It is fine to pass blob IDs which do not point to a valid LFS pointer, but
// passing blob IDs which do not exist results in an error.
func (s *server) GetLFSPointers(req *gitalypb.GetLFSPointersRequest, stream gitalypb.BlobService_GetLFSPointersServer) error {
	ctx := stream.Context()

	if err := validateGetLFSPointersRequest(req); err != nil {
		return helper.ErrInvalidArgument(err)
	}

	repo := s.localrepo(req.GetRepository())

	chunker := chunk.New(&lfsPointerSender{
		send: func(pointers []*gitalypb.LFSPointer) error {
			return stream.Send(&gitalypb.GetLFSPointersResponse{
				LfsPointers: pointers,
			})
		},
	})

	objectInfoReader, cancel, err := s.catfileCache.ObjectInfoReader(ctx, repo)
	if err != nil {
		return helper.ErrInternal(fmt.Errorf("creating object info reader: %w", err))
	}
	defer cancel()

	objectReader, cancel, err := s.catfileCache.ObjectReader(ctx, repo)
	if err != nil {
		return helper.ErrInternal(fmt.Errorf("creating object reader: %w", err))
	}
	defer cancel()

	blobs := make([]gitpipe.RevisionResult, len(req.GetBlobIds()))
	for i, blobID := range req.GetBlobIds() {
		blobs[i] = gitpipe.RevisionResult{OID: git.ObjectID(blobID)}
	}

	catfileInfoIter, err := gitpipe.CatfileInfo(ctx, objectInfoReader, gitpipe.NewRevisionIterator(ctx, blobs),
		gitpipe.WithSkipCatfileInfoResult(func(objectInfo *catfile.ObjectInfo) bool {
			return objectInfo.Type != "blob" || objectInfo.Size > lfsPointerMaxSize
		}),
	)
	if err != nil {
		return helper.ErrInternalf("creating object info iterator: %w", err)
	}
	defer cancel()

	catfileObjectIter, err := gitpipe.CatfileObject(ctx, objectReader, catfileInfoIter)
	if err != nil {
		return helper.ErrInternalf("creating object iterator: %w", err)
	}

	if err := sendLFSPointers(chunker, catfileObjectIter, 0); err != nil {
		return err
	}

	return nil
}

func validateGetLFSPointersRequest(req *gitalypb.GetLFSPointersRequest) error {
	if err := service.ValidateRepository(req.GetRepository()); err != nil {
		return err
	}

	if len(req.GetBlobIds()) == 0 {
		return fmt.Errorf("empty BlobIds")
	}

	return nil
}

type lfsPointerSender struct {
	pointers []*gitalypb.LFSPointer
	send     func([]*gitalypb.LFSPointer) error
}

func (t *lfsPointerSender) Reset() {
	t.pointers = t.pointers[:0]
}

func (t *lfsPointerSender) Append(m proto.Message) {
	t.pointers = append(t.pointers, m.(*gitalypb.LFSPointer))
}

func (t *lfsPointerSender) Send() error {
	return t.send(t.pointers)
}

func sendLFSPointers(chunker *chunk.Chunker, iter gitpipe.CatfileObjectIterator, limit int) error {
	buffer := bytes.NewBuffer(make([]byte, 0, lfsPointerMaxSize))

	var i int
	for iter.Next() {
		lfsPointer := iter.Result()

		// Avoid allocating bytes for an LFS pointer until we know the current blob really
		// is an LFS pointer.
		buffer.Reset()

		// Given that we filter pipeline objects by size, the biggest object we may see here
		// is 200 bytes in size. So it's not much of a problem to read this into memory
		// completely.
		if _, err := io.Copy(buffer, lfsPointer); err != nil {
			return helper.ErrInternal(fmt.Errorf("reading LFS pointer data: %w", err))
		}

		if !git.IsLFSPointer(buffer.Bytes()) {
			continue
		}

		objectData := make([]byte, buffer.Len())
		copy(objectData, buffer.Bytes())

		if err := chunker.Send(&gitalypb.LFSPointer{
			Data: objectData,
			Size: int64(len(objectData)),
			Oid:  lfsPointer.ObjectID().String(),
		}); err != nil {
			return helper.ErrInternal(fmt.Errorf("sending LFS pointer chunk: %w", err))
		}

		i++
		if limit > 0 && i >= limit {
			break
		}
	}

	if err := iter.Err(); err != nil {
		return helper.ErrInternal(err)
	}

	if err := chunker.Flush(); err != nil {
		return helper.ErrInternal(err)
	}

	return nil
}
