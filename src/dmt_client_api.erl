-module(dmt_client_api).

-export([commit/2]).
-export([checkout/1]).
-export([pull/1]).
-export([checkout_object/2]).

-spec commit(dmt_client:version(), dmt_client:commit()) -> dmt_client:version() | no_return().

commit(Version, Commit) ->
    call('Repository', 'Commit', [Version, Commit]).

-spec checkout(dmt_client:ref()) -> dmt_client:snapshot() | no_return().

checkout(Reference) ->
    call('Repository', 'Checkout', [Reference]).

-spec pull(dmt_client:version()) -> dmt_client:history() | no_return().

pull(Version) ->
    call('Repository', 'Pull', [Version]).

-spec checkout_object(dmt_client:ref(), dmt_client:object_ref()) -> dmsl_domain_thrift:'DomainObject'() | no_return().

checkout_object(Reference, ObjectReference) ->
    call('RepositoryClient', 'checkoutObject', [Reference, ObjectReference]).


call(ServiceName, Function, Args) ->
    Url = get_service_url(ServiceName),
    Service = get_service_modname(ServiceName),
    Call = {Service, Function, Args},
    Opts = #{
        url => Url,
        event_handler => scoper_woody_event_handler,
        transport_opts => get_transport_opts()
    },
    Context = woody_context:new(),
    case woody_client:call(Call, Opts, Context) of
        {ok, Response} ->
            Response;
        {exception, Exception} ->
            throw(Exception)
    end.

get_service_url(ServiceName) ->
    maps:get(ServiceName, genlib_app:env(dmt_client, service_urls)).

get_service_modname(ServiceName) ->
    {get_service_module(ServiceName), ServiceName}.

get_service_module('Repository') ->
    dmsl_domain_config_thrift;
get_service_module('RepositoryClient') ->
    dmsl_domain_config_thrift.

get_transport_opts() ->
    Default = [{recv_timeout, 60000}, {connect_timeout, 1000}],
    genlib_app:env(dmt_client, transport_opts, Default).
