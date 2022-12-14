package objectpool

import (
	"gitlab.com/gitlab-org/gitaly/v15/internal/git/objectpool"
	"gitlab.com/gitlab-org/gitaly/v15/internal/helper"
	"gitlab.com/gitlab-org/gitaly/v15/proto/go/gitalypb"
)

var (
	errInvalidPoolDir = helper.ErrInvalidArgument(objectpool.ErrInvalidPoolDir)

	// errMissingPool is returned when the request is missing the object pool.
	errMissingPool = helper.ErrInvalidArgumentf("no object pool repository")
)

// PoolRequest is the interface of a gRPC request that carries an object pool.
type PoolRequest interface {
	GetObjectPool() *gitalypb.ObjectPool
}

// ExtractPool returns the pool repository from the request or an error if the
// request did no contain a pool.
func ExtractPool(req PoolRequest) (*gitalypb.Repository, error) {
	poolRepo := req.GetObjectPool().GetRepository()
	if poolRepo == nil {
		return nil, errMissingPool
	}

	return poolRepo, nil
}

func (s *server) poolForRequest(req PoolRequest) (*objectpool.ObjectPool, error) {
	poolRepo, err := ExtractPool(req)
	if err != nil {
		return nil, err
	}

	pool, err := objectpool.NewObjectPool(s.locator, s.gitCmdFactory, s.catfileCache, s.txManager, s.housekeepingManager, poolRepo.GetStorageName(), poolRepo.GetRelativePath())
	if err != nil {
		if err == objectpool.ErrInvalidPoolDir {
			return nil, errInvalidPoolDir
		}

		return nil, helper.ErrInternal(err)
	}

	return pool, nil
}
