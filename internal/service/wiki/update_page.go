package wiki

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"gitlab.com/gitlab-org/gitaly-proto/go/gitalypb"
	"gitlab.com/gitlab-org/gitaly/internal/rubyserver"
)

func (s *server) WikiUpdatePage(stream gitalypb.WikiService_WikiUpdatePageServer) error {
	firstRequest, err := stream.Recv()
	if err != nil {
		return err
	}

	if err := validateWikiUpdatePageRequest(firstRequest); err != nil {
		return status.Errorf(codes.InvalidArgument, "WikiUpdatePage: %v", err)
	}

	ctx := stream.Context()

	client, err := s.WikiServiceClient(ctx)
	if err != nil {
		return err
	}

	clientCtx, err := rubyserver.SetHeaders(ctx, firstRequest.GetRepository())
	if err != nil {
		return err
	}

	rubyStream, err := client.WikiUpdatePage(clientCtx)
	if err != nil {
		return err
	}

	if err := rubyStream.Send(firstRequest); err != nil {
		return err
	}

	err = rubyserver.Proxy(func() error {
		request, err := stream.Recv()
		if err != nil {
			return err
		}
		return rubyStream.Send(request)
	})

	if err != nil {
		return err
	}

	response, err := rubyStream.CloseAndRecv()
	if err != nil {
		return err
	}

	return stream.SendAndClose(response)
}

func validateWikiUpdatePageRequest(request *gitalypb.WikiUpdatePageRequest) error {
	if len(request.GetPagePath()) == 0 {
		return fmt.Errorf("empty Page Path")
	}

	if len(request.GetTitle()) == 0 {
		return fmt.Errorf("empty Title")
	}

	if len(request.GetFormat()) == 0 {
		return fmt.Errorf("empty Format")
	}

	return validateRequestCommitDetails(request)
}
