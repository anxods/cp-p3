-module(comp).

-export([comp/1, comp/2 , decomp/1, decomp/2, comp_proc/2, comp_proc/3, decomp_proc/2, decomp_proc/3, 
		sort_loop/1, sort_loop/2, comp_loop_con/3, createProcs/3, crear_dcprocs/3, decomp_loop_con/3]).

-define(DEFAULT_CHUNK_SIZE, 1024*1024).

%%% File Compression

comp(File) -> %% Compress file to file.ch
    comp(File, ?DEFAULT_CHUNK_SIZE).

comp(File, Chunk_Size) ->  %% Starts a reader and a writer which run in separate processes
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
    
    
    
%%% Compresion en Procs procesos
    
comp_proc(File, Procs) -> %% Compress file to file.ch
    comp_proc(File, ?DEFAULT_CHUNK_SIZE, Procs).   
    
comp_proc(File, Chunk_Size, Procs) ->  %% Starts a reader and a writer which run in separate processes
		case file_service:start_file_reader(File, Chunk_Size) of
			{ok, Reader} -> 
				case archive:start_archive_writer(File++".ch") of
					{ok, Writer} ->
						createProcs(Procs, Reader, Writer),
						comprobar_procs(Procs),
						Reader ! stop,
						Writer ! stop;
					{error, Reason} ->
						io:format("Could not open output file: ~w~n", [Reason])
				end;
			{error, Reason} ->
				io:format("Could not open input file: ~w~n", [Reason])
		end.
    
    
    
%%Creacion de Proc procesos para comprimir

createProcs(0, _, _) -> true;

createProcs(N, Reader, Writer) -> spawn(comp, comp_loop_con, [Reader, Writer, self()]),
				createProcs(N-1, Reader, Writer).


%%Comprobar que ya han terminado (eof) o no los Procs creados

comprobar_procs(0) -> ok;
				
comprobar_procs(Procs) -> 
	receive
		eof -> comprobar_procs(Procs-1)
				end.
		
	
comp_loop_con(Reader, Writer, From) ->  %% Compression loop => get a chunk, compress it, send to writer
    Reader ! {get_chunk, self()},  %% request a chunk from the file reader
    receive
        {chunk, Num, Data} ->   %% got one, compress and send to writer
            Comp_Data = compress:compress(Data),
            Writer ! {add_chunk, Num, Comp_Data},
            comp_loop_con(Reader, Writer, From);
        eof ->  %% end of file, stop reader and writer
            From ! eof;
        {error, Reason} ->
            io:format("Error reading input file: ~w~n",[Reason]),
            Reader ! stop,
            Writer ! abort,
            From ! eof
         
    end.	
	
	

comp_loop(Reader, Writer) ->  %% Compression loop => get a chunk, compress it, send to writer
    Reader ! {get_chunk, self()},  %% request a chunk from the file reader
    receive
        {chunk, Num, Data} ->   %% got one, compress and send to writer
            Comp_Data = compress:compress(Data),
            Writer ! {add_chunk, Num, Comp_Data},
            comp_loop(Reader, Writer);
        eof ->  %% end of file, stop reader and writer
            Reader ! stop,
            Writer ! stop;
        {error, Reason} ->
            io:format("Error reading input file: ~w~n",[Reason]),
            Reader ! stop,
            Writer ! abort
    end.

%% File Decompression

decomp(Archive) ->  
    decomp(Archive, string:replace(Archive, ".ch", "", trailing)).    
    %%decomp(Archive, string:substr(Archive, 1, string:rstr(Archive, ".ch") -1)).

decomp(Archive, Output_File) ->
    case archive:start_archive_reader(Archive) of
        {ok, Reader} ->
            case file_service:start_file_writer(Output_File) of
                {ok, Writer} ->
                    decomp_loop(Reader, Writer);
                {error, Reason} ->
                    io:format("Could not open output file: ~w~n", [Reason])
            end;
        {error, Reason} ->
            io:format("Could not open input file: ~w~n", [Reason])
    end.
    
    

%% Decompresion en Procs procesos    
    
decomp_proc(Archive, Procs) ->  
    decomp_proc(Archive, string:replace(Archive, ".ch", "", trailing), Procs).   
    %%decomp_proc(Archive, string:substr(Archive, 1, string:rstr(Archive, ".ch") -1), Procs).

decomp_proc(Archive, Output_File, Procs) ->
    case archive:start_archive_reader(Archive) of
        {ok, Reader} ->
            case file_service:start_file_writer(Output_File) of
                {ok, Writer} ->
					Sorted = spawn(comp, sort_loop, [Writer]),
                    crear_dcprocs(Procs, Reader, Sorted),
					comprobar_procs(Procs),
					Reader ! stop,
					Sorted ! stop;
                {error, Reason} ->
                    io:format("Could not open output file: ~w~n", [Reason])
            end;
        {error, Reason} ->
            io:format("Could not open input file: ~w~n", [Reason])
    end.
    

%%Creacion de Proc procesos para descomprimir

crear_dcprocs(0, _ , _) -> true;

crear_dcprocs(N, Reader, Writer) -> spawn(comp, decomp_loop_con, [Reader, Writer, self()]),
				crear_dcprocs(N-1, Reader, Writer).


decomp_loop_con(Reader, Writer, From) ->
    Reader ! {get_chunk, self()},  %% request a chunk from the reader
    receive 
        {chunk, Num, Comp_Data} ->  %% got one
            Data = compress:decompress(Comp_Data),
            Writer ! {add_chunk, Num, Data},
            decomp_loop_con(Reader, Writer, From);
        eof ->    %% end of file => exit decompression
            From ! eof;
        {error, Reason} -> 
            io:format("Error reading input file: ~w~n", [Reason]),
            Writer ! abort,
            Reader ! stop
    end.
    
    
    

decomp_loop(Reader, Writer) ->
    Reader ! {get_chunk, self()},  %% request a chunk from the reader
    receive 
        {chunk, _Num, Comp_Data} ->  %% got one
            Data = compress:decompress(Comp_Data),
            Writer ! {write_chunk, Data},
            decomp_loop(Reader, Writer);
        eof ->    %% end of file => exit decompression
            Reader ! stop,
            Writer ! stop;
        {error, Reason} -> 
            io:format("Error reading input file: ~w~n", [Reason]),
            Writer ! abort,
            Reader ! stop
    end.




%% Ordenacion de chunks para decompress

sort_loop(File_Writer, Chunk) ->
	receive
		{add_chunk, Chunk, Data} -> %% matching forces Chunk to be the one we want
			File_Writer ! {write_chunk, Data},
			sort_loop(File_Writer, Chunk+1);
		stop -> File_Writer ! stop
	end.
		
sort_loop(File_Writer) ->
	sort_loop(File_Writer, 0).


