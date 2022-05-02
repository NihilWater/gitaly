# Generated by the protocol buffer compiler.  DO NOT EDIT!
# Source: transaction.proto for package 'gitaly'

require 'grpc'
require 'transaction_pb'

module Gitaly
  module RefTransaction
    # RefTransaction is a service which provides RPCs to interact with reference
    # transactions. Reference transactions are used in the context of Gitaly
    # Cluster to ensure that all nodes part of a single transaction perform the
    # same change: given a set of changes, the changes are hashed and the hash is
    # then voted on.
    class Service

      include ::GRPC::GenericService

      self.marshal_class_method = :encode
      self.unmarshal_class_method = :decode
      self.service_name = 'gitaly.RefTransaction'

      rpc :VoteTransaction, ::Gitaly::VoteTransactionRequest, ::Gitaly::VoteTransactionResponse
      rpc :StopTransaction, ::Gitaly::StopTransactionRequest, ::Gitaly::StopTransactionResponse
    end

    Stub = Service.rpc_stub_class
  end
end
