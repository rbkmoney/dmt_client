%%% @doc Public API, supervisor and application startup.
%%% @end

-module(dmt_client).

-behaviour(supervisor).
-behaviour(application).

%% API
-export([checkout/1]).
-export([checkout/2]).
-export([checkout_object/1]).
-export([checkout_object/2]).
-export([checkout_object/3]).
-export([checkout_versioned_object/1]).
-export([checkout_versioned_object/2]).
-export([checkout_versioned_object/3]).
-export([checkout_objects_by_type/1]).
-export([checkout_objects_by_type/2]).
-export([checkout_objects_by_type/3]).
-export([checkout_filter_objects/1]).
-export([checkout_filter_objects/2]).
-export([checkout_filter_objects/3]).
-export([checkout_fold_objects/2]).
-export([checkout_fold_objects/3]).
-export([checkout_fold_objects/4]).
-export([commit/2]).
-export([commit/3]).
-export([get_last_version/0]).
-export([pull_range/2]).
-export([pull_range/3]).

%% Health check API
-export([health_check/0]).

%% Supervisor callbacks
-export([init/1]).

%% Application callbacks
-export([start/2]).
-export([stop/1]).

-export_type([ref/0]).
-export_type([vsn/0]).
-export_type([version/0]).
-export_type([limit/0]).
-export_type([snapshot/0]).
-export_type([commit/0]).
-export_type([object_ref/0]).
-export_type([object_type/0]).
-export_type([object_filter/0]).
-export_type([object_folder/1]).
-export_type([domain_object/0]).
-export_type([domain/0]).
-export_type([history/0]).
-export_type([transport_opts/0]).

-include_lib("damsel/include/dmsl_domain_config_thrift.hrl").

-type ref() :: dmsl_domain_config_thrift:'Reference'().
-type vsn() :: dmsl_domain_config_thrift:'Version'().
-type version() :: vsn() | latest.
-type limit() :: dmsl_domain_config_thrift:'Limit'().
-type snapshot() :: dmsl_domain_config_thrift:'Snapshot'().
-type commit() :: dmsl_domain_config_thrift:'Commit'().
-type object_ref() :: dmsl_domain_thrift:'Reference'().
-type object_type() :: atom().
-type object_filter() :: fun((object_type(), domain_object()) -> boolean()).
-type object_folder(Acc) :: fun((object_type(), domain_object(), Acc) -> Acc).
-type domain_object() :: dmsl_domain_thrift:'DomainObject'().
-type versioned_object() :: dmsl_domain_config_thrift:'VersionedObject'().
-type domain() :: dmsl_domain_thrift:'Domain'().
-type history() :: dmsl_domain_config_thrift:'History'().
-type transport_opts() :: woody_client_thrift_http_transport:transport_options() | undefined.

%%% API

-spec checkout(version()) -> snapshot() | no_return().
checkout(Reference) ->
    checkout(Reference, undefined).

-spec checkout(version(), transport_opts()) -> snapshot() | no_return().
checkout(Reference, Opts) ->
    Version = ref_to_version(Reference),
    case dmt_client_cache:get(Version, Opts) of
        {ok, Snapshot} ->
            Snapshot;
        {error, Error} ->
            erlang:error(Error)
    end.

-spec checkout_object(object_ref()) -> domain_object() | no_return().
checkout_object(ObjectReference) ->
    checkout_object(latest, ObjectReference).

-spec checkout_object(version(), object_ref()) -> domain_object() | no_return().
checkout_object(Reference, ObjectReference) ->
    checkout_object(Reference, ObjectReference, undefined).

-spec checkout_object(version(), object_ref(), transport_opts()) -> domain_object() | no_return().
checkout_object(Reference, ObjectReference, Opts) ->
    Version = ref_to_version(Reference),
    unwrap(dmt_client_cache:get_object(Version, ObjectReference, Opts)).

-spec checkout_versioned_object(object_ref()) -> versioned_object() | no_return().
checkout_versioned_object(ObjectReference) ->
    checkout_versioned_object(latest, ObjectReference).

-spec checkout_versioned_object(version(), object_ref()) -> versioned_object() | no_return().
checkout_versioned_object(Reference, ObjectReference) ->
    checkout_versioned_object(Reference, ObjectReference, undefined).

-spec checkout_versioned_object(version(), object_ref(), transport_opts()) -> versioned_object() | no_return().
checkout_versioned_object(Reference, ObjectReference, Opts) ->
    Version = ref_to_version(Reference),
    #'VersionedObject'{version = Version, object = checkout_object(Reference, ObjectReference, Opts)}.

-spec checkout_objects_by_type(object_type()) -> [domain_object()] | no_return().
checkout_objects_by_type(ObjectType) ->
    checkout_objects_by_type(latest, ObjectType).

-spec checkout_objects_by_type(version(), object_type()) -> [domain_object()] | no_return().
checkout_objects_by_type(Reference, ObjectType) ->
    checkout_objects_by_type(Reference, ObjectType, undefined).

-spec checkout_objects_by_type(version(), object_type(), transport_opts()) -> [domain_object()] | no_return().
checkout_objects_by_type(Reference, ObjectType, Opts) ->
    Version = ref_to_version(Reference),
    unwrap(dmt_client_cache:get_objects_by_type(Version, ObjectType, Opts)).

-spec checkout_filter_objects(object_filter()) -> [{object_type(), domain_object()}] | no_return().
checkout_filter_objects(Filter) ->
    checkout_filter_objects(latest, Filter).

-spec checkout_filter_objects(version(), object_filter()) -> [{object_type(), domain_object()}] | no_return().
checkout_filter_objects(Reference, Filter) ->
    checkout_filter_objects(Reference, Filter, undefined).

-spec checkout_filter_objects(version(), object_filter(), transport_opts()) ->
    [{object_type(), domain_object()}] | no_return().
checkout_filter_objects(Reference, Filter, Opts) ->
    Folder = fun(Type, Object, Acc) ->
        case Filter(Type, Object) of
            true -> [{Type, Object} | Acc];
            false -> Acc
        end
    end,
    checkout_fold_objects(Reference, Folder, [], Opts).

-spec checkout_fold_objects(object_folder(Acc), Acc) -> Acc | no_return().
checkout_fold_objects(Folder, Acc) ->
    checkout_fold_objects(latest, Folder, Acc).

-spec checkout_fold_objects(version(), object_folder(Acc), Acc) -> Acc | no_return().
checkout_fold_objects(Reference, Folder, Acc) ->
    checkout_fold_objects(Reference, Folder, Acc, undefined).

-spec checkout_fold_objects(version(), object_folder(Acc), Acc, transport_opts()) -> Acc | no_return().
checkout_fold_objects(Reference, Folder, Acc, Opts) ->
    Version = ref_to_version(Reference),
    unwrap(dmt_client_cache:fold_objects(Version, Folder, Acc, Opts)).

-spec commit(version(), commit()) -> version() | no_return().
commit(Version, Commit) ->
    commit(Version, Commit, undefined).

-spec commit(version(), commit(), transport_opts()) -> version() | no_return().
commit(Version, Commit, Opts) ->
    dmt_client_backend:commit(Version, Commit, Opts).

-spec get_last_version() -> version().
get_last_version() ->
    dmt_client_cache:get_last_version().

-spec pull_range(version(), limit()) -> history() | no_return().
pull_range(Version, Limit) ->
    pull_range(Version, Limit, undefined).

-spec pull_range(version(), limit(), transport_opts()) -> history() | no_return().
pull_range(Version, Limit, Opts) ->
    dmt_client_backend:pull_range(Version, Limit, Opts).

%% Health check API

-spec health_check() -> erl_health:result().
health_check() ->
    try
        _ = dmt_client_cache:get_last_version(),
        {passing, #{}}
    catch
        _Class:_Error ->
            {critical, #{last_version => not_found}}
    end.

%%% Supervisor callbacks

-spec init([]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([]) ->
    Cache = #{id => dmt_client_cache, start => {dmt_client_cache, start_link, []}, restart => permanent},
    {ok, {#{strategy => one_for_one, intensity => 10, period => 60}, [Cache]}}.

%%% Application callbacks

-spec start(normal, any()) -> {ok, pid()} | {error, any()}.
start(_StartType, _StartArgs) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec stop(any()) -> ok.
stop(_State) ->
    ok.

%%% Internal functions

unwrap({ok, Acc}) -> Acc;
unwrap({error, {woody_error, _} = Error}) -> erlang:error(Error);
%% DISCUSS: shouldn't version_not_found be handled some other way?
unwrap({error, _}) -> erlang:throw(#'ObjectNotFound'{}).

-spec ref_to_version(version()) -> vsn().
ref_to_version(Version) when is_integer(Version) ->
    Version;
ref_to_version(latest) ->
    dmt_client_cache:get_last_version().
