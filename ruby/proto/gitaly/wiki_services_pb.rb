# Generated by the protocol buffer compiler.  DO NOT EDIT!
# Source: wiki.proto for package 'gitaly'

require 'grpc'
require 'wiki_pb'

module Gitaly
  module WikiService
    # WikiService is a service that provides Wiki-related functionality. This
    # service is deprecated and should not be used anymore. Instead, all
    # functionality to implement Wikis should use Git-based RPCS.
    class Service

      include ::GRPC::GenericService

      self.marshal_class_method = :encode
      self.unmarshal_class_method = :decode
      self.service_name = 'gitaly.WikiService'

      rpc :WikiWritePage, stream(::Gitaly::WikiWritePageRequest), ::Gitaly::WikiWritePageResponse
      rpc :WikiUpdatePage, stream(::Gitaly::WikiUpdatePageRequest), ::Gitaly::WikiUpdatePageResponse
      # WikiFindPage returns a stream because the page's raw_data field may be arbitrarily large.
      rpc :WikiFindPage, ::Gitaly::WikiFindPageRequest, stream(::Gitaly::WikiFindPageResponse)
      rpc :WikiGetAllPages, ::Gitaly::WikiGetAllPagesRequest, stream(::Gitaly::WikiGetAllPagesResponse)
      rpc :WikiListPages, ::Gitaly::WikiListPagesRequest, stream(::Gitaly::WikiListPagesResponse)
    end

    Stub = Service.rpc_stub_class
  end
end
