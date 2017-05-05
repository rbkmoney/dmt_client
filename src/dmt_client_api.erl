-module(dmt_client_api).

-export([commit/2]).
-export([checkout/1]).
-export([pull/1]).
-export([checkout_object/2]).

-type ref() :: dmsl_domain_config_thrift:'Reference'().
-type version() :: dmsl_domain_config_thrift:'Version'().
-type snapshot() :: dmsl_domain_config_thrift:'Snapshot'().
-type commit() :: dmsl_domain_config_thrift:'Commit'().
-type object_ref() :: dmsl_domain_thrift:'Reference'().

-spec commit(version(), commit()) -> version().

commit(Version, Commit) ->
    call(repository, 'Commit', [Version, Commit]).

-spec checkout(ref()) -> snapshot().

checkout(Reference) ->
    call(repository, 'Checkout', [Reference]).

-spec pull(version()) -> dmsl_domain_config_thrift:'History'().

pull(Version) ->
    call(repository, 'Pull', [Version]).

-spec checkout_object(ref(), object_ref()) -> dmsl_domain_thrift:'DomainObject'().

checkout_object(Reference, ObjectReference) ->
    call(repository_client, 'checkoutObject', [Reference, ObjectReference]).


call(ServiceName, Function, Args) ->
    Host = application:get_env(dmt, client_host, "dominant"),
    Port = integer_to_list(application:get_env(dmt, client_port, 8022)),
    {Path, Service} = get_handler_spec(ServiceName),
    Call = {Service, Function, Args},
    Opts = #{
        url => list_to_binary(Host ++ ":" ++ Port ++ Path),
        event_handler => {dmt_client_woody_event_handler, undefined}
    },
    Context = woody_context:new(),
    case woody_client:call(Call, Opts, Context) of
        {ok, Response} ->
            Response;
        {exception, Exception} ->
            throw(Exception)
    end.

get_handler_spec(repository) ->
    {"/v1/domain/repository",
        {dmsl_domain_config_thrift, 'Repository'}};
get_handler_spec(repository_client) ->
    {"/v1/domain/repository_client",
        {dmsl_domain_config_thrift, 'RepositoryClient'}}.
