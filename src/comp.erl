-module(comp).

-export([comp/1, comp/2 , decomp/1, decomp/2, comp_proc/2, comp_proc/3, decomp_proc/2, decomp_proc/3, 
		sort_loop/1, sort_loop/2, comp_loop_con/3, createProcesses/3, createdecompProcs/3, decomp_loop_con/3]).

-define(DEFAULT_CHUNK_SIZE, 1024*1024).

% File Compression

comp(File) -> % Compress file to <name of the file>.ch
    comp(File, ?DEFAULT_CHUNK_SIZE).

comp(File, Chunk_Size) ->  % Starts a reader and a writer which run in separate processes
    case file_service:start_file_reader(File, Chunk_Size) of
        {ok, Reader} -> 
            case archive:start_archive_writer(File++".ch") of
                {ok, Writer} ->
                    comp_loop(Reader, Writer);
                {error, Reason} ->
                    io:format("Could not open output file: ~w~n", [Reason])
            end;
        {error, Reason} ->
            io:format("Could not open input file: ~w~n", [Reason])
    end.
    
% Exercise 1 (Write a concurrent compression function)
% Compression uses a single process running in comp_loop. This function may be called from comp(File), 
% and comp(File, Chunk_Size). Write two concurrent versions comp_proc(File, Procs) and comp_proc(File,
% Chunk_Size, Procs) that compress File using Procs processes. Make sure that all the processes
% created during the compression process stop when the compression ends.    
    
% Compression of a file with concurrent processes
    
comp_proc(File, Procs) -> %% Compress file to <name of the file>.ch
    comp_proc(File, ?DEFAULT_CHUNK_SIZE, Procs).   
    
comp_proc(File, Chunk_Size, Procs) ->  %% Starts a reader and a writer which run in separate processes
		case file_service:start_file_reader(File, Chunk_Size) of
			{ok, Reader} -> 
				case archive:start_archive_writer(File++".ch") of
					{ok, Writer} ->
						createProcesses(Procs, Reader, Writer),
						checkProcs(Procs),
						Reader ! stop,
						Writer ! stop;
					{error, Reason} ->
						io:format("Could not open output file: ~w~n", [Reason])
				end;
			{error, Reason} ->
				io:format("Could not open input file: ~w~n", [Reason])
		end. 
    
% Functions to create n procs to do compression

createProcesses(0, _, _) -> true;

createProcesses(N, Reader, Writer) -> spawn(comp, comp_loop_con, [Reader, Writer, self()]),
				createProcesses(N-1, Reader, Writer).

% Function to check that the previously created procs have finished (reaching eof)

checkProcs(0) -> ok;
				
checkProcs(Procs) -> 
	receive
		eof -> checkProcs(Procs-1)
				end.
	
comp_loop_con(Reader, Writer, From) ->  % Compression loop => get a chunk, compress it, send to writer
    Reader ! {get_chunk, self()},  % request a chunk from the file reader
    receive
        {chunk, Num, Data} ->   % once you got one 
            Comp_Data = compress:compress(Data), % compress
            Writer ! {add_chunk, Num, Comp_Data}, % send to writer
            comp_loop_con(Reader, Writer, From); % and keep going
        eof ->  % once it's eof, stop reader and writer
            From ! eof;
        {error, Reason} -> % otherwise, there was an error so we get this message
            io:format("Error reading input file: ~w~n",[Reason]),
            Reader ! stop,
            Writer ! abort,
            From ! eof
    end.	

comp_loop(Reader, Writer) ->  % Compression loop => get a chunk, compress it, send to writer
    Reader ! {get_chunk, self()}, % request a chunk from the file reader
    receive
        {chunk, Num, Data} ->   % once you got one
            Comp_Data = compress:compress(Data), % compress
            Writer ! {add_chunk, Num, Comp_Data}, % send to writer
            comp_loop(Reader, Writer); % keep going
        eof ->  % end of file, stop reader and writer
            Reader ! stop,
            Writer ! stop;
        {error, Reason} -> % otherwise, there was an error so we get this message
            io:format("Error reading input file: ~w~n",[Reason]),
            Reader ! stop,
            Writer ! abort
    end.

% File Decompression

% Exercise 2 (Write a concurrent decompression function) Decompression also takes place in 
% a single process in decomp_loop. This function is called from decomp(Archive) and 
% decomp(Archive, Output_File). Write two concurrent versions decomp_proc(Archive, Procs) and 
% decomp_proc(Archive, Output_file, Procs) that decompress Archive using Proc processes. 
% All the processes started should stop when the decompression ends.

decomp(Archive) ->  
    decomp(Archive, string:replace(Archive, ".ch", "", trailing)).

decomp(Archive, Output_File) ->
    case archive:start_archive_reader(Archive) of
        {ok, Reader} ->
            case file_service:start_file_writer(Output_File) of
                {ok, Writer} ->
                    decomp_loop(Reader, Writer);
                {error, Reason} ->
                    io:format("Could not open output file: ~w~n", [Reason])
            end;
        {error, Reason} -> % otherwise, there was an error so we get this message
            io:format("Could not open input file: ~w~n", [Reason])
    end.
    
% Decompresion en Processes procesos    
    
decomp_proc(Archive, Procs) ->  
    decomp_proc(Archive, string:replace(Archive, ".ch", "", trailing), Procs).

decomp_proc(Archive, Output_File, Procs) ->
    case archive:start_archive_reader(Archive) of
        {ok, Reader} ->
            case file_service:start_file_writer(Output_File) of
                {ok, Writer} ->
					Sorted = spawn(comp, sort_loop, [Writer]),
                    createdecompProcs(Procs, Reader, Sorted),
					checkProcs(Procs),
					Reader ! stop,
					Sorted ! stop;
                {error, Reason} -> % otherwise, there was an error so we get this message
                    io:format("Could not open output file: ~w~n", [Reason])
            end;
        {error, Reason} -> % otherwise, there was an error so we get this message
            io:format("Could not open input file: ~w~n", [Reason])
    end.

% Functions to create n procs to do decompression

createdecompProcs(0, _ , _) -> true;

createdecompProcs(N, Reader, Writer) -> spawn(comp, decomp_loop_con, [Reader, Writer, self()]),
				createdecompProcs(N-1, Reader, Writer).

decomp_loop_con(Reader, Writer, From) ->
    Reader ! {get_chunk, self()},  %% request a chunk from the reader
    receive 
        {chunk, Num, Comp_Data} ->  %% got one
            Data = compress:decompress(Comp_Data),
            Writer ! {add_chunk, Num, Data},
            decomp_loop_con(Reader, Writer, From);
        eof -> % eof (exit decomp)
            From ! eof;
        {error, Reason} -> % otherwise, there was an error so we get this message
            io:format("Error reading input file: ~w~n", [Reason]),
            Writer ! abort,
            Reader ! stop
    end.

decomp_loop(Reader, Writer) ->
    Reader ! {get_chunk, self()},  % request a chunk from the reader
    receive 
        {chunk, _Num, Comp_Data} ->  % got one
            Data = compress:decompress(Comp_Data), % take the data
            Writer ! {write_chunk, Data}, % write it for the output
            decomp_loop(Reader, Writer);
        eof -> % eof (exit decomp)
            Reader ! stop,
            Writer ! stop;
        {error, Reason} -> 
            io:format("Error reading input file: ~w~n", [Reason]),
            Writer ! abort,
            Reader ! stop
    end.

%% Loop function for chunk sorting after decomp

sort_loop(File_Writer, Chunk) ->
	receive
		{add_chunk, Chunk, Data} -> %% matching forces Chunk to be the one we want
			File_Writer ! {write_chunk, Data},
			sort_loop(File_Writer, Chunk+1);
		stop -> File_Writer ! stop
	end.
		
sort_loop(File_Writer) ->
	sort_loop(File_Writer, 0).