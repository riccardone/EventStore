using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Unicode;
using System.Threading.Tasks;
using EventStore.Core.LogAbstraction;
using EventStore.Core.Services;
using FASTER.core;

namespace EventStore.Core.LogV3.FASTER {
	public class FASTERStreamNameIndex :
		IStreamNameIndex<long>,
		IStreamIdLookup<long> {

		private static readonly Encoding _utf8NoBom = new UTF8Encoding(false, true);
		private readonly IDevice _log;
		//qq temp public
		public readonly FasterKV<SpanByte, long> _store;
		private readonly ClientSession<SpanByte, long, long, long, Context<long>, TryAddFunctions<long>> _writerSession;
		private readonly ClientSession<SpanByte, long, long, long, Empty, ReaderFunctions<long>> _readerSession;
		private readonly Context<long> _context = new();
		//qq we seem to be using this as the prevId rather than next.
		private long _nextId = LogV3SystemStreams.FirstRealStream;

		public class Context<T> {
			public T Value { get; set; }
			public Status Status { get; set; }
		}

		public FASTERStreamNameIndex(string logDir, bool large) {
			_log = Devices.CreateLogDevice(
				logPath: $"{logDir}/stream-name-index.log",
				//qq we will need some kind of cleanup.
				deleteOnClose: false);

			var checkpointSettings = new CheckpointSettings {
				CheckpointDir = $"{logDir}",
				RemoveOutdated = true,
			};

			_store = large
				? new FasterKV<SpanByte, long>(
					size: 1L << 27,
					checkpointSettings: checkpointSettings,
					logSettings: new LogSettings {
						LogDevice = _log,
						
						//PreallocateLog = true,
						//ReadCacheSettings = new ReadCacheSettings()
					})
				: new FasterKV<SpanByte, long>(
					size: 1L << 20,
					checkpointSettings: checkpointSettings,
					logSettings: new LogSettings {
						LogDevice = _log,
						MemorySizeBits = 15,
						PageSizeBits = 12,
						//PreallocateLog = true,
						//ReadCacheSettings = new ReadCacheSettings()
					});

			try {
				_store.Recover();
			}
			catch (Exception ex) {
				//qq this can be ok, there might not be anything to recover.
				// we could look more closely at the exception, or have some logic to determine
				// whether to recover or not.
				// probably log an info?
				Console.WriteLine($"#### Exception recovering stream name index {ex}");
			}

			_writerSession = _store
				.For(new TryAddFunctions<long>(_context))
				.NewSession<TryAddFunctions<long>>();

			_readerSession = _store
				.For(new ReaderFunctions<long>())
				.NewSession<ReaderFunctions<long>>();
		}

		//qq todo: arrange for this to be called
		public void Dispose() {
			_writerSession?.Dispose();
			_readerSession?.Dispose();
			_store?.Dispose();
			_log?.Dispose();
		}


		//qqqqqq
		// if there is something that i want from a tail record when i load up an existing fasterlog(a sort of app state rehydration),
		// is the only way to observe it to scan forward on an iterator until i hit a record whose next address is the tail?

		// You can also start the iteration from some(tail - delta) address, just make sure the address corresponds to the start of
		// a page(multiple of LogPageSize).
		//
		// ^ this is the way to win... so we can get the last (not deleted) record
		// i.e. find the most recent key.
		// 
		// ok context: the standard index has been initialised. it now contains exactly the data that we want to have in the stream name index.
		// lets call it the target.
		// there are three cases
		// 1. we are exactly in sync
		// 2. we are behind the target (must catchup)
		// 3. we are ahead of the target (must truncate)
		//
		// SO:
		// 1. find the latest non-deleted entry in the SNI
		// 2. check if that entry is present in the target.
		// 3. IF SO -> read the target forwards adding in the entries. done.
		// 4. IF NOT -> iterate backwards through the SNI (page at a time) until we find an entry in the target
		//           -> check that the target contains no other entries after.
		// then we can check if that is in the target

		//qq what exactly happens in the faster log when we delete anyway

		// we dont need the record numbers, but we need to know how many streams there are
		// and we need to be able to iterate through backwards.
		public void Init(IList<(long RecordNumber, long StreamId, string StreamName)> streamsStream) {
			using var session = _store
				.For(new AddressFunctions<long>())
				.NewSession<AddressFunctions<long>>();

			bool TryGetAddress(string streamName, out long address) {
				Span<byte> key = stackalloc byte[Measure(streamName)];
				Populate(streamName, key);
				var status = session.Read(
					key: SpanByte.FromFixedSpan(key),
					output: out address);

				switch (status) {
					case Status.OK:
						return true;
					case Status.NOTFOUND:
						return false;
					case Status.PENDING:
						throw new Exception($"PENDING"); //qqqq
					case Status.ERROR:
						throw new Exception($"Error reading {streamName}: Error when reading");
					default:
						throw new Exception($"Error reading {streamName}: Unknown status {status} when reading");
				}
			}

			// progress through the streamsstream backwards until we find one thats present in the stream name index.
			//qqqqqqq this is a bit rubbish though because we will iterate through the whole stream in the case that there aren't any in the streamnameindex.
			// we should check if the first one exists first, and if it doesn't then we should iterate through adding them all.
			var i = streamsStream.Count - 1;
			var address = 0L; //qq rename to 'truncateto' or something like that?, also what should this be initialised to
			for (; i >= 0; i--) {
				if (TryGetAddress(streamsStream[i].StreamName, out address))
					break;
			}

			//qq delete everything after that point.
			//qq what happens if we iterate while deleting, do we need to get the list of things to dleete first
			//session.Delete();
			//var count = 0;
			//using var iter = store.Log.Scan(0, store.Log.TailAddress);
			//while (iter.GetNext(out var recordInfo)) {
			//	var asdf = iter.CurrentAddress;
			//	var asdfg = iter.NextAddress;
			//	ref var key = ref iter.GetKey();
			//	ref var value = ref iter.GetValue();
			//	count++;
			//}

			// doesn't matter whether we found one or not, we have established i as the place to load from.
			// (if we found one then i points to it, but we can load it gain.
			for (; i < streamsStream.Count; i++) {
				var tuple = streamsStream[i];
				GetOrAddId(tuple.StreamName, out var streamId, out var _, out var _);

				if (streamId % 2 == 1)
					throw new Exception($"this should never happen. found odd stream id {streamId}");
				if (streamId != tuple.StreamId)
					throw new Exception($"this should never happen. tried to add stream {tuple.StreamId} to stream name index but it came out as {streamId}");
			}
		}

		public long LookupId(string streamName) {
			//qq do we need a separate session for each thread that reads?

			// convert the streamName into a UTF8 span which we can use as a key.
			Span<byte> key = stackalloc byte[Measure(streamName)];
			Populate(streamName, key);

			var status = _readerSession.Read(
				key: SpanByte.FromFixedSpan(key),
				output: out var streamNumber);

			switch (status) {
				case Status.OK:
					return streamNumber;
				case Status.NOTFOUND:
					return 0;
				case Status.PENDING:
					throw new Exception($"PENDING"); //qqqq
				case Status.ERROR:
					throw new Exception($"Error reading {streamName}: Error when reading");

				default:
					throw new Exception($"Error reading {streamName}: Unknown status {status} when reading");
			}
		}

		public bool GetOrAddId(string streamName, out long streamNumber, out long createdId, out string createdName) {
			if (string.IsNullOrEmpty(streamName))
				throw new ArgumentNullException(nameof(streamName));
			if (SystemStreams.IsMetastream(streamName))
				throw new ArgumentException(nameof(streamName));

			// convert the streamName into a UTF8 span which we can use as a key.
			Span<byte> key = stackalloc byte[Measure(streamName)];
			Populate(streamName, key);

			// attempt a atomic try-add via read-modify-write.
			bool preExisting;
			var idForNewStream = _nextId + 2; //qq + 2 a bit magic
			var status = _writerSession.RMW(
				key: SpanByte.FromFixedSpan(key),
				input: idForNewStream,
				userContext: _context);

			switch (status) {
				case Status.OK:
					// stream already exists. functions populated the number in the context.
					preExisting = true;
					streamNumber = _context.Value;
					break;

				case Status.NOTFOUND:
					// stream did not exist, it has been added with the idForNewStream
					preExisting = false;
					_nextId = idForNewStream;
					streamNumber = idForNewStream;
					break;

				case Status.PENDING:
					// operation required disk access and is not complete.
					// for now lets just wait for it.
					_writerSession.CompletePending(wait: true);
					switch (_context.Status) {
						case Status.OK:
							// stream already exists. functions populated the number in the context.
							preExisting = true;
							streamNumber = _context.Value;
							break;

						case Status.NOTFOUND:
							// stream did not exist, it has been added with the idForNewStream
							preExisting = false;
							_nextId = idForNewStream;
							streamNumber = idForNewStream;
							break;

						case Status.PENDING:
							throw new Exception($"Error Get/Adding {streamName}: Unexpectedly pending");

						case Status.ERROR:
							throw new Exception($"Error Get/Adding {streamName}: Error when completing operation");

						default:
							throw new Exception($"Error Get/Adding {streamName}: Unknown status {_context.Status} when completing operation");
					}
					break;

				case Status.ERROR:
					throw new Exception($"Error Get/Adding {streamName}: Error when performing operation");

				default:
					throw new Exception($"Error Get/Adding {streamName}: Unknown status {status} when performing operation");
			}

			createdId = streamNumber;
			createdName = streamName;
			return preExisting;
		}

		//qq consider when to persist
		public async ValueTask Persist() {
			//qq why is this amking me put configureawait false
			//qq do we need to do anything with the token
			//qq why might it not be successful?
			var (success, token) = await _store.TakeHybridLogCheckpointAsync(CheckpointType.FoldOver).ConfigureAwait(false);

			if (!success)
				throw new Exception("could not take checkpoint"); //qq details
		}

		int Measure(string source) {
			var length = _utf8NoBom.GetByteCount(source);
			return length;
		}

		static void Populate(string source, Span<byte> destination) {
			Utf8.FromUtf16(source, destination, out _, out _, true, true);
		}
	}
}
