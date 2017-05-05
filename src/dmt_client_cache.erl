-module(dmt_client_cache).
-behaviour(gen_server).

%%

-export([start_link/0]).

-export([put/1]).
-export([get/1]).
-export([get_closest/1]).
-export([get_min/0]).
-export([get_max/0]).

%%

-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-define(TABLE, ?MODULE).
-define(SERVER, ?MODULE).
-define(DEFAULT_INTERVAL, 5000).

-include_lib("dmsl/include/dmsl_domain_config_thrift.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

%%

-type version() :: dmsl_domain_config_thrift:'Version'().
-type snapshot() :: dmsl_domain_config_thrift:'Snapshot'().

-spec start_link() -> {ok, pid()} | {error, term()}. % FIXME

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec put(snapshot()) -> snapshot().

put(Snapshot) ->
    ok = gen_server:call(?SERVER, {put, Snapshot}),
    Snapshot.

-spec get(version()) -> {ok, snapshot()} | {error, version_not_found}.

get(Version) ->
    gen_server:call(?SERVER, {get, Version}).

-spec get_closest(version()) -> {ok, snapshot()} | {error, version_not_found}.

get_closest(Version) ->
    gen_server:call(?SERVER, {get_closest, Version}).

-spec get_min() -> {ok, snapshot()} | {error, version_not_found}.

get_min() ->
    gen_server:call(?SERVER, get_min).

-spec get_max() -> {ok, snapshot()} | {error, version_not_found}.

get_max() ->
    gen_server:call(?SERVER, get_max).

%%

-record(state, {
    timer = undefined :: undefined | reference()
}).

-type state() :: #state{}.

-spec init(_) -> {ok, state(), 0}.

init(_) ->
    EtsOpts = [
        named_table,
        ordered_set,
        protected,
        {read_concurrency, true},
        {keypos, #'Snapshot'.version}
    ],
    ?TABLE = ets:new(?TABLE, EtsOpts),
    {ok, #state{}, 0}.

-spec handle_call(term(), {pid(), term()}, state()) -> {reply, term(), state()}.

handle_call({put, Snapshot}, _From, State) ->
    {reply, put_snapshot(Snapshot), State};

handle_call({get, Version}, _From, State) ->
    {reply, get_snapshot(Version), State};

handle_call({get_closest, Version}, _From, State) ->
    {reply, closest_snapshot(Version), State};

handle_call(get_min, _From, State) ->
    {reply, min_snapshot(), State};

handle_call(get_max, _From, State) ->
    {reply, max_snapshot(), State};

handle_call(_Msg, _From, State) ->
    {noreply, State}.

-spec handle_cast(term(), state()) -> {noreply, state()}.

handle_cast(_Msg, State) ->
    {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.

handle_info(timeout, State) ->
    _Result = update_cache(),
    {noreply, restart_timer(State)};

handle_info(_Msg, State) ->
    {noreply, State}.

-spec terminate(term(), state()) -> ok.
terminate(_Reason, _State) ->
    ok.

-spec code_change(term(), state(), term()) -> {error, noimpl}.
code_change(_OldVsn, _State, _Extra) ->
    {error, noimpl}.

%% internal

-spec put_snapshot(snapshot()) -> ok.

put_snapshot(Snapshot) ->
    true = ets:insert(?TABLE, Snapshot),
    ok.

-spec get_snapshot(version()) -> {ok, snapshot()} | {error, version_not_found}.

get_snapshot(Version) ->
    case ets:lookup(?TABLE, Version) of
        [Snapshot] ->
            {ok, Snapshot};
        [] ->
            {error, version_not_found}
    end.

-spec min_snapshot() -> {ok, snapshot()} | {error, version_not_found}.

min_snapshot() ->
    case ets:first(?TABLE) of
        '$end_of_table' ->
            {error, version_not_found};
        Version ->
            get_snapshot(Version)
    end.

-spec max_snapshot() -> {ok, snapshot()} | {error, version_not_found}.

max_snapshot() ->
    case ets:last(?TABLE) of
        '$end_of_table' ->
            {error, version_not_found};
        Version ->
            get_snapshot(Version)
    end.

-spec closest_snapshot(version()) -> {ok, snapshot()} | {error, version_not_found}.

closest_snapshot(Version) ->
    CachedVersions = ets:select(?TABLE, ets:fun2ms(fun (#'Snapshot'{version = V}) -> V end)),
    Closest = lists:foldl(fun (V, Acc) ->
        case abs(V - Version) =< abs(Acc - Version) of
            true ->
                V;
            false ->
                Acc
        end
    end, 0, CachedVersions),
    case Closest of
        0 ->
            {error, version_not_found};
        Closest ->
            get_snapshot(Closest)
    end.

-spec restart_timer(state()) -> state().

restart_timer(State = #state{timer = undefined}) ->
    start_timer(State);

restart_timer(State = #state{timer = TimerRef}) ->
    _ = erlang:cancel_timer(TimerRef),
    start_timer(State#state{timer = undefined}).

-spec start_timer(state()) -> state().

start_timer(State = #state{timer = undefined}) ->
    Interval = genlib_app:env(dmt_client, poll_interval, ?DEFAULT_INTERVAL),
    State#state{timer = erlang:send_after(Interval, self(), timeout)}.

-spec update_cache() -> {ok, version()} | {error, term()}.

update_cache() ->
    OldHead = case max_snapshot() of
        {error, version_not_found} ->
            #'Snapshot'{version = 0, domain = dmt_domain:new()};
        {ok, Snapshot} ->
            Snapshot
    end,
    try
        FreshHistory = dmt_client_api:pull(OldHead#'Snapshot'.version),
        NewHead = dmt_history:head(FreshHistory, OldHead),
        ok = put_snapshot(NewHead),
        {ok, NewHead#'Snapshot'.version}
    catch
        error:{woody_error, {_Source, Class, _Details}} = Error when
            Class == resource_unavailable;
            Class == result_unknown
        ->
            {error, Error}
    end.

