-module(dmt_client_api).

-export([commit/2]).
-export([checkout/1]).
-export([pull/1]).
-export([checkout_object/2]).

-spec commit(dmt_client:version(), dmt_client:commit()) -> dmt_client:version() | no_return().

commit(Version, Commit) ->
    call(repository, 'Commit', [Version, Commit]).

-spec checkout(dmt_client:ref()) -> dmt_client:snapshot() | no_return().

checkout(Reference) ->
    call(repository, 'Checkout', [Reference]).

-spec pull(dmt_client:version()) -> dmt_client:history() | no_return().

pull(Version) ->
    call(repository, 'Pull', [Version]).

-spec checkout_object(dmt_client:ref(), dmt_client:object_ref()) -> dmsl_domain_thrift:'DomainObject'() | no_return().

checkout_object(Reference, ObjectReference) ->
    call(repository_client, 'checkoutObject', [Reference, ObjectReference]).


call(ServiceName, Function, Args) ->
    {Url, Service} = get_handler_spec(ServiceName),
    Call = {Service, Function, Args},
    Opts = #{
        url => Url,
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
    {
        application:get_env(dmt_client, repository_url, <<"dominant:8022/v1/domain/repository">>),
        {dmsl_domain_config_thrift, 'Repository'}
    };
get_handler_spec(repository_client) ->
    {
        application:get_env(dmt_client, repository_client_url, <<"dominant:8022/v1/domain/repository_client">>),
        {dmsl_domain_config_thrift, 'RepositoryClient'}
    }.
