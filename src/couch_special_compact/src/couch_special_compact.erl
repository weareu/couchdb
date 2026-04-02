%% -*- erlang -*-
%%! -escript main -pa $ERL_LIBS/couch/ebin
%% Licensed under the Apache License, Version 2.0
%% See the LICENSE file in the project root for more information.

-module(couch_special_compact).

-export([main/1]).

-include_lib("kernel/include/file.hrl").
-include_lib("../../config/src/config.hrl").
-include_lib("../../couch/include/couch_db.hrl").
-include_lib("../../couch_log/include/couch_log.hrl").
-include("../../couch/src/couch_bt_engine.hrl").

main(Args) ->
    case parse_args(Args) of
        {ok, Options} ->
            run_compactor(Options);
        {error, Reason} ->
            io:format("Error: ~s~n", [Reason]),
            usage(),
            halt(1)
    end.

parse_args(Args) ->
    case Args of
        [ShardFilePath, IniFilesDirs] ->
            {ok, #{shard_file => ShardFilePath, ini_files_dirs => IniFilesDirs}};
        _ ->
            {error, "Invalid arguments. Usage: <ShardFilePath> [IniFilesDirs]"}
    end.

initialize_runtime(IniFilesDirs) ->
    io:format("Initializing minimal CouchDB runtime with ini files directories: ~p~n", [IniFilesDirs]),
    config:init([IniFilesDirs]),
    io:format("Initialization complete.~n"),
    ok.

run_compactor(Options) ->
    IniFilesDirs = maps:get(ini_files_dirs, Options),
    initialize_runtime(IniFilesDirs),
    ShardFile = maps:get(shard_file, Options),
    io:format("ShardFile: ~s~n", [ShardFile]),
    case initialize_state(ShardFile) of
        {ok, St, DbName, ShortName, Db} ->
            Parent = self(),
            CompactorOptions = [], % Add any specific compactor options here
            io:format("Starting compaction for shard file: ~s - ~s : ~s~n", [ShardFile, DbName, ShortName]),
            Pid = spawn_link(fun() ->
                couchextractcompact:start(St, DbName, CompactorOptions, Parent)
            end),
            receive
                {compact_done, couch_bt_engine, NewFilePath} ->
                    io:format("Compaction completed. New file: ~s~n", [NewFilePath]),
                    halt(0);
                {'EXIT', Pid, Reason} ->
                    io:format("Compaction failed: ~p~n", [Reason]),
                    halt(1)
            end;
        {error, Reason} ->
            io:format("Failed to initialize compaction state: ~p~n", [Reason]),
            halt(1)
    end.

initialize_state(ShardFile) ->
    %% Check if the shard file exists
    case filelib:is_file(ShardFile) of
        true ->
            DbName = ?l2b(extract_dbname(ShardFile)),
            ShortName = mem3:dbname(DbName),
            io:format("DbName0 (list): ~p, DbName (binary): ~p~n", [DbName, ShortName]),
            %% Initialize the #st{} record
            case initialize_st(ShardFile, DbName, ShortName) of
                {ok, St, Db} ->
                    {ok, St, DbName, ShortName, Db};
                {error, Reason} ->
                    {error, Reason}
            end;
        false ->
            {error, "Shard file does not exist"}
    end.

extract_dbname(FilePath) ->
    io:format("Extracting database name from file path: ~s~n", [FilePath]),
    %% Find "/data/" in the file path
    case string:split(FilePath, "/data/", all) of
        [_Prefix, Suffix] ->
            %% Remove the ".couch" extension
            DbName = filename:rootname(Suffix),
            io:format("Extracted database name: ~s~n", [DbName]),
            DbName;
        _ ->
            %% If "/data/" is not found, log an error and fail
            io:format("Error: '/data/' not found in file path: ~s~n", [FilePath]),
            throw({invalid_filepath, FilePath})
    end.

test_snappy() ->
    %% Test compression and decompression
    InputData = <<"Hello, Snappy!">>,

    %% Test compression
    case catch couch_compress:compress(InputData, snappy) of
        Compressed when is_binary(Compressed) ->
            io:format("Compression successful. Compressed data: ~p~n", [Compressed]),
            
            %% Test decompression
            case catch couch_compress:decompress(Compressed) of
                Decompressed when is_binary(Decompressed) ->
                    io:format("Decompression successful. Original data: ~p~n", [Decompressed]),
                    
                    %% Verify data integrity
                    case Decompressed =:= InputData of
                        true ->
                            io:format("Data integrity verified!~n"),
                            {ok, Compressed, Decompressed};
                        false ->
                            io:format("Data mismatch after decompression!~n"),
                            {error, data_mismatch}
                    end;
                Error ->
                    io:format("Decompression failed. Error: ~p~n", [Error]),
                    {error, decompression_failed, Error}
            end;
        Error ->
            io:format("Compression failed. Error: ~p~n", [Error]),
            {error, compression_failed, Error}
    end.

initialize_st(ShardFile, DbName, ShortName) ->
    io:format("Initializing state for shard file: ~s ~s~n", [ShardFile, ShortName]),
    try
        %% Open the shard file
        case couch_file:open(ShardFile, [?ADMIN_CTX]) of
            {ok, Fd} ->
                io:format("Successfully opened shard file: ~s~nFile Descriptor: ~p~n", [ShardFile, Fd]),
                %% Read the header from the shard file
                case couch_file:read_header(Fd) of
                    {ok, Header} ->
                        io:format("Successfully read header for shard file: ~s~n", [ShardFile]),
                        Engine = couch_bt_engine,
                        application:start(couch_epi),
                        application:start(couch_stats),
                        %% Get the engine path
                        {ok, Filepath} = couch_server:get_engine_path(DbName, Engine),
                        case couch_db:validate_dbname(DbName) of
                            ok ->
                                ok;
                            {error, E} ->
                                throw({target_create_error, DbName, E})
                        end,
                        case couch_server:lock(DbName, <<"shard attachment extraction">>) of
                            ok ->
                                ok;
                            {error, Err} ->
                                throw({target_create_error, DbName, Err})
                        end,
                        Opts = [create, ?ADMIN_CTX] ++ [], %never partitioned
                        case couch_db:start_link(Engine, DbName, Filepath, Opts) of
                            {ok, Db} ->
                                io:format("Started database process for: ~p~n", [DbName]),
                                %% Create the #st{} record
                                St = #st{
                                    filepath = ShardFile,
                                    header = Header,
                                    fd = Fd
                                },
                                {ok, St, Db};
                            {error, Er} ->
                                throw({target_create_error, DbName, Er })
                        end;
                    {err, ValidateReason} ->
                        io:format("Failed to read header for shard file: ~s, Reason: ~p~n", [ShardFile, ValidateReason]),
                        throw({header_read_failed, ValidateReason})
                end;
            {err, ValidateReason} ->
                io:format("Failed to open shard file: ~s, Reason: ~p~n", [ShardFile, ValidateReason]),
                throw({file_open_failed, ValidateReason})
        end
    catch
        error:Error:StackTrace ->
            io:format("Unexpected error during state initialization for shard file: ~s~nError: ~p~nStackTrace: ~p~n", 
                    [ShardFile, Error, StackTrace]),
            {error, {unexpected_error, Error, StackTrace}};
        throw:Thrown:StackTrace ->
            io:format("Unexpected throw during state initialization for shard file: ~s~nThrow: ~p~nStackTrace: ~p~n", 
                    [ShardFile, Thrown, StackTrace]),
            {error, {unexpected_throw, Thrown, StackTrace}};
        exit:ExitReason:StackTrace ->
            io:format("Unexpected exit during state initialization for shard file: ~s~nExit Reason: ~p~nStackTrace: ~p~n", 
                    [ShardFile, ExitReason, StackTrace]),
            {error, {unexpected_exit, ExitReason, StackTrace}}
    end.
    

usage() ->
    io:format("Usage: couchextractcompact <path_to_shard_file> <path_to_config_ini_path>~n").