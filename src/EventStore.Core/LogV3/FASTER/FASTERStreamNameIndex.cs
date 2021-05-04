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
		private readonly ClientSession<SpanByte, long, long, long, TryAddFunctions<long>.Context, TryAddFunctions<long>> _writerSession;
		private readonly ClientSession<SpanByte, long, long, long, ReaderFunctions<long>.Context, ReaderFunctions<long>> _readerSession;
		private readonly TryAddFunctions<long>.Context _writerContext = new();
		private long _nextId = LogV3SystemStreams.FirstRealStream;

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

			_writerSession = _store
				.For(new TryAddFunctions<long>(_writerContext))
				.NewSession<TryAddFunctions<long>>();

			_readerSession = _store
				.For(new ReaderFunctions<long>())
				.NewSession<ReaderFunctions<long>>();

			Recover();
		}

		//qq todo: arrange for this to be called
		public void Dispose() {
			_writerSession?.Dispose();
			_readerSession?.Dispose();
			_store?.Dispose();
			_log?.Dispose();
		}

		void Recover() {
			try {
				_store.Recover();
				var (streamId, streamName) = ScanBackwards().LastOrDefault();
				if (streamName != null)
					_nextId = streamId + LogV3SystemStreams.StreamInterval;
			} catch (Exception ex) {
				//qq this can be ok, there might not be anything to recover.
				// we could look more closely at the exception, or have some logic to determine
				// whether to recover or not.
				// probably log an info?
				Console.WriteLine($"#### Exception recovering stream name index {ex}");
			}
		}

		//qqqqqq
		// ok context: the standard index has been initialised. it now contains exactly the data that we want to have in the stream name index.
		// lets call it the source.
		// there are three cases
		// 1. we are exactly in sync with the source (do nothing)
		// 2. we are behind the source (could be by a lot, say we are rebuilding) -> catch up
		// 3. we are ahead of the source (should only be by a little) -> truncate
		//
		// SO proceed as follows:
		// 1. find the latest non-deleted entry in the stream name index
		// 2. check if that entry is present in the source.
		// 3. IF SO -> read the source forwards adding in the entries. done.
		// 4. IF NOT -> iterate backwards through the SNI (page at a time) until we find an entry in the source
		//           -> check that the source contains no other entries after.
		// then we can check if that is in the source


		public void Init(Dictionary<long, string> source) {
			var iter = ScanBackwards().GetEnumerator();

			if (!iter.MoveNext()) {
				// stream name index is empty, catch up from beginning of source
				CatchUp(source, prevStream: 0);
				return;
			}

			// got hold of the most recent entry. is it in the source?
			if (source.TryGetValue(iter.Current.StreamId, out var sourceStreamName)) {
				if (sourceStreamName != iter.Current.StreamName)
					throw new Exception("value mismatch between the indexes, should never happen");

				// the most recent entry we have is in the source. the source is equal to
				// or ahead of us. catch up.
				CatchUp(source, prevStream: iter.Current.StreamId);
				return;
			}

			// we have a most recent entry but it is not in the source.
			// scan backwards until we find something in common with the source and
			// delete everything in between.

			var streamNamesToDelete = new List<string>();
			void PopStream() {
				streamNamesToDelete.Add(iter.Current.StreamName);
				_nextId -= LogV3SystemStreams.StreamInterval;
				if (_nextId != iter.Current.StreamId)
					throw new Exception($"this should never happen {_nextId} {iter.Current.StreamId}");
			}

			PopStream();

			bool found = false;
			while (!found && iter.MoveNext()) {
				found = source.TryGetValue(iter.Current.StreamId, out sourceStreamName);
				if (!found) {
					PopStream();
				}
			}

			if (found) {
				//qqq iter.Current.StreamId should be the max stream that is in the source.
				var max = source.Keys.Max();
				if (iter.Current.StreamId != max)
					throw new Exception("this should never happen"); //qq details
				if (iter.Current.StreamName != sourceStreamName)
					throw new Exception(
						$"this should never happen. name mismatch. " +
						$"id: {iter.Current.StreamId} name: \"{iter.Current.StreamName}\"/\"{sourceStreamName}\"");
			}
			else {
				//qqq assert that source is empty (max is nostream?)
				if (source.Count != 0)
					throw new Exception("this should never happen. source shouldh ave been empty but wasn't"); //qq details
			}

			Delete(streamNamesToDelete);

			//qqq we need to persist the deletions or otherwise we might diverge. a test will show this.
			// (very similar to a checkpoint except it isn't going to write the metadata and wont work
			// for being able to recover the index)
			_store.Log.Flush(wait: true);
		}

		//qq todo: consider the prevStream argument carefully in each case.
		int CatchUp(Dictionary<long, string> source, long prevStream) {
			var count = 0;
			//qqqqqqqqqqq need some way to get the max from the source? not sure, perhaps we pass in a IStreamNameLookupHere
			// and extend the interface with a 'get max' member
			var max = source.Keys.Max();
			var startStream = prevStream == 0
				? LogV3SystemStreams.FirstRealStream
				: prevStream + LogV3SystemStreams.StreamInterval;

			for (var i = startStream; i <= max; i += LogV3SystemStreams.StreamInterval) {
				count++;
				var preExisting = GetOrAddId(source[i], out var streamId, out var _, out var _);

				if (preExisting)
					throw new Exception(
						$"this should never happen. found preexisting stream when catching up. " +
						$"name: \"{source[i]}\" id: {i}/{streamId}");
				if (streamId % 2 == 1)
					throw new Exception(
						$"this should never happen. found odd numbered stream. " +
						$"name: \"{source[i]}\" id: {i}/{streamId}");
				if (streamId != i)
					throw new Exception(
						$"this should never happen. streamid mismatch. " +
						$"name: \"{source[i]}\" id: {i}/{streamId}");
			}
			return count;
		}

		//qq temp?
		public IEnumerable<(long StreamId, string StreamName)> Scan() => Scan(0, _store.Log.TailAddress);

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
//					throw new Exception($"interesting. current value for key {stringValue} is {currentValue} but value is {value}");
					continue;
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
		public IEnumerable<(long StreamId, string StreamName)> ScanBackwards() {
			var pageSize = 1 << _logSettings.PageSizeBits;
			var endAddress = _store.Log.TailAddress;
			var beginAddress = endAddress / pageSize * pageSize;

			if (beginAddress < 0)
				beginAddress = 0;

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
			//qq is creating a session slow, better have a thread static readersession.
			var session = _store.For(new ReaderFunctions<long>()).NewSession<ReaderFunctions<long>>();

			//qq thredstatic context?
			var context = new ReaderFunctions<long>.Context();
			var status = session.Read(
				key: SpanByte.FromFixedSpan(key),
				output: out var streamNumber,
				userContext: context);

			switch (status) {
				case Status.OK:
					return streamNumber;
				case Status.NOTFOUND:
					return 0;
				case Status.PENDING:

					session.CompletePending(wait: true);

					switch (context.Status) {
						case Status.OK:
							streamNumber = context.Output;
							break;
						case Status.NOTFOUND:
							streamNumber = 0;
							break;
						//qq probably apply this case struture to the other switches.
						default:
							throw new Exception($"Unexpected status {context.Status} completing read for \"{streamName}\"");
					}

					return streamNumber;
				default:
					throw new Exception($"Unexpected status {status} reading {streamName}");
			}
		}


		void Delete(IList<string> streamNames) {
			for (int i = 0; i < streamNames.Count; i++) {
				Delete(streamNames[i]);
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

		public bool GetOrAddId(string streamName, out long streamId, out long createdId, out string createdName) {
			if (string.IsNullOrEmpty(streamName))
				throw new ArgumentNullException(nameof(streamName));
			if (SystemStreams.IsMetastream(streamName))
				throw new ArgumentException(nameof(streamName));

			// convert the streamName into a UTF8 span which we can use as a key.
			Span<byte> key = stackalloc byte[Measure(streamName)];
			Populate(streamName, key);

			// attempt a atomic try-add via read-modify-write.
			bool preExisting;
			var idForNewStream = _nextId;
			var status = _writerSession.RMW(
				key: SpanByte.FromFixedSpan(key),
				input: idForNewStream,
				userContext: _writerContext);

			switch (status) {
				case Status.OK:
					// stream already exists. functions populated the number in the context.
					preExisting = true;
					streamId = _writerContext.Value;
					break;

				case Status.NOTFOUND:
					// stream did not exist, it has been added with the idForNewStream
					preExisting = false;
					streamId = idForNewStream;
					_nextId = idForNewStream + LogV3SystemStreams.StreamInterval;
					break;

				case Status.PENDING:
					// operation required disk access and is not complete.
					// for now lets just wait for it.
					_writerSession.CompletePending(wait: true);
					switch (_writerContext.Status) {
						case Status.OK:
							// stream already exists. functions populated the number in the context.
							preExisting = true;
							streamId = _writerContext.Value;
							break;

						case Status.NOTFOUND:
							// stream did not exist, it has been added with the idForNewStream
							preExisting = false;
							streamId = idForNewStream;
							_nextId = idForNewStream + LogV3SystemStreams.StreamInterval;
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

			createdId = streamId;
			createdName = streamName;
			return preExisting;
		}

		//qq consider when to persist
		public async ValueTask CheckpointLog() {
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
