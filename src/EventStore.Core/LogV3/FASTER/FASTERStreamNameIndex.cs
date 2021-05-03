using System;
using System.Collections.Generic;
using System.Linq;
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
		private readonly FasterKV<SpanByte, long> _store;
		private readonly LogSettings _logSettings;
		private readonly ClientSession<SpanByte, long, long, long, Context<long>, TryAddFunctions<long>> _writerSession;
		private readonly ClientSession<SpanByte, long, long, long, Empty, ReaderFunctions<long>> _readerSession;
		private readonly Context<long> _writerContext = new();
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

			if (large) {
				_logSettings = new LogSettings {
					LogDevice = _log,
					//qq PageSizeBits

					//PreallocateLog = true,
					//ReadCacheSettings = new ReadCacheSettings()
				};

				_store = new FasterKV<SpanByte, long>(
					size: 1L << 27,
					checkpointSettings: checkpointSettings,
					logSettings: _logSettings);
			}
			else {
				_logSettings = new LogSettings {
					LogDevice = _log,
					MemorySizeBits = 15,
					PageSizeBits = 12,
					//PreallocateLog = true,
					//ReadCacheSettings = new ReadCacheSettings()
				};

				_store = new FasterKV<SpanByte, long>(
					size: 1L << 20,
					checkpointSettings: checkpointSettings,
					logSettings: _logSettings);
			}

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
				.For(new TryAddFunctions<long>(_writerContext))
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

		//qq temp?
		public IEnumerable<(long, string)> Scan() => Scan(0, _store.Log.TailAddress);

		//qq temp?
		public IEnumerable<(long, string)> Scan(long beginAddress, long endAddress) {
			using var iter = _store.Log.Scan(beginAddress, endAddress);
			while (iter.GetNext(out var recordInfo)) {
				var asdf = iter.CurrentAddress;
				var asdfg = iter.NextAddress;
				var key = iter.GetKey();
				var value = iter.GetValue();


				var stringValue = _utf8NoBom.GetString(key.AsReadOnlySpan());

				//qq we can't just look to see if this is a tombstone or not
				//qq but give this a bit more thought.
				var currentValue = LookupId(stringValue);

				if (currentValue != value) {
					throw new Exception($"interesting. current value for key {stringValue} is {currentValue} but value is {value}");
//					continue;
				}

				//qqif (recordInfo.Invalid)
				//	continue;

				//if (recordInfo.Tombstone)
				//	continue;

				yield return (value, stringValue);
			}
		}

		// if there is something that i want from a tail record when i load up an existing fasterlog(a sort of app state rehydration),
		// is the only way to observe it to scan forward on an iterator until i hit a record whose next address is the tail?

		// You can also start the iteration from some(tail - delta) address, just make sure the address corresponds to the start of
		// a page(multiple of LogPageSize).
		//
		// so here we skip backwards through the pages
		public IEnumerable<(long, string)> ScanBackwards() {
			var pageSize = 1 << _logSettings.PageSizeBits;
			var endAddress = _store.Log.TailAddress;
			var beginAddress = endAddress / pageSize * pageSize;

			if (beginAddress < 0)
				beginAddress = 0;

			//if (beginAddress != 0) {
			//	//qq multiple pages!
			//	throw new Exception("hurrah multiple pages");
			//}

			//qq double check that scan can scan an empty range
			var count = 0;
			var detailCount = 0;
			while (endAddress > 0) {
				var entries = Scan(beginAddress, endAddress).ToList();
				for (int i = entries.Count - 1; i >= 0; i--) {
					yield return entries[i];
					detailCount++;
				}
				endAddress = beginAddress;
				beginAddress -= pageSize;
				count++;
			}
		}

		public long LookupId(string streamName) {
			//qq do we need a separate session for each thread that reads?
			//qq perhaps the important thing is that we have different contexts for 
			// different reads.. thread static?

			// convert the streamName into a UTF8 span which we can use as a key.
			Span<byte> key = stackalloc byte[Measure(streamName)];
			Populate(streamName, key);

			//var session = _readerSession;
			var session = _store.For(new ReaderFunctions<long>()).NewSession<ReaderFunctions<long>>();

			var status = session.Read(
				key: SpanByte.FromFixedSpan(key),
				output: out var streamNumber);

			switch (status) {
				case Status.OK:
					return streamNumber;
				case Status.NOTFOUND:
					return 0;
				case Status.PENDING:
					//					//qq maybe we can call 'withoutputs' version instead of the context hack that we did before.
					//					// in the case of reads that will save us having to put the read result in the context
					//					// it doesn't help us for RMW though because there is no output.
					//					//
					//					// CompletePending completes the pending calls for this (faster?) thread
					//					// hang on... we want to be sure that we dont get anyone elses results here
					//					// we need to call CompletePending and have it put the read results in the 
					//					// respective contexts.
					//					//qq make sure that we can call CompletePending from different threads simultaneously
					//					//
					//					//qq so plan: we do want to use 'withoutputs' for read, to make our functions simpler
					//					// we just need to make sure that since LookupId can be called by multiple threads
					//					// (can't it?), that it needs to have a session per thread (And context per thread if we need a context)

					//
					// LATER try without 'with outputs' and see if it makes any difference to the readcompletioncallback
					// its weird that neither the callback nor the completion here has the key set correctly
					// that might provide a clue as to why it isn't working. we could see if its a regresstion in faster too.
					session.CompletePendingWithOutputs(out var completedOutputs, wait: true);

					if (!completedOutputs.Next())
						throw new Exception("expected read to complete but it didnt");

					ref var output = ref completedOutputs.Current;

					var stringValue = _utf8NoBom.GetString(output.Key.AsReadOnlySpan());
					//if (stringValue != streamName)
					//	throw new Exception($"hmm completed \"{stringValue}\" but expected \"{streamName}\"");

					switch (output.Status) {
						case Status.OK:
							streamNumber = output.Output;
							break;
						case Status.NOTFOUND:
							streamNumber = 0;
							break;
						//qq probably apply this case struture to the other switches.
						default:
							throw new Exception($"Unexpected status {output.Status} completing read for \"{streamName}\"");
					}
					if (completedOutputs.Next())
						throw new Exception("expected single read to complete but there was multiple");

					completedOutputs.Dispose();

					return streamNumber;
				default:
					throw new Exception($"Unexpected status {status} reading {streamName}");
		}
	}

	//qq temp?
	// this uses the writer session, which has a single context, so we should
	// make sure it doesn't get called at the same time as a getoradd
	public void Delete(string streamName) {
			if (string.IsNullOrEmpty(streamName))
				throw new ArgumentNullException(nameof(streamName));
			if (SystemStreams.IsMetastream(streamName))
				throw new ArgumentException(nameof(streamName));

			// convert the streamName into a UTF8 span which we can use as a key.
			Span<byte> key = stackalloc byte[Measure(streamName)];
			Populate(streamName, key);

			//qq which session should we use, probably the init session?
			//
			//
			//qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq on monday: carry on here
			//qq am expecting to have to fill in the deletecompleted handler
			// might want to have different functions for that reason.
			var status = _writerSession.Delete(
				key: SpanByte.FromFixedSpan(key));

			switch (status) {
				case Status.OK:
					break;

				case Status.NOTFOUND:
					break;

				case Status.PENDING:
//					throw new Exception("pending"); //qq
					_writerSession.CompletePending(wait: true);
					switch (_writerContext.Status) {
						case Status.OK:
							break;

						case Status.NOTFOUND:
							break;

						case Status.PENDING:
							throw new Exception($"Error Deleting {streamName}: Unexpectedly pending");

						case Status.ERROR:
							throw new Exception($"Error Deleting {streamName}: Error when completing operation");

						default:
							throw new Exception($"Error Deleting {streamName}: Unknown status {_writerContext.Status} when completing operation");
					}
					break;

				case Status.ERROR:
					throw new Exception($"Error Deleting {streamName}: Error when performing operation");

				default:
					throw new Exception($"Error Deleting {streamName}: Unknown status {status} when performing operation");
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
				userContext: _writerContext);

			switch (status) {
				case Status.OK:
					// stream already exists. functions populated the number in the context.
					preExisting = true;
					streamNumber = _writerContext.Value;
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
					switch (_writerContext.Status) {
						case Status.OK:
							// stream already exists. functions populated the number in the context.
							preExisting = true;
							streamNumber = _writerContext.Value;
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
							throw new Exception($"Error Get/Adding {streamName}: Unknown status {_writerContext.Status} when completing operation");
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
