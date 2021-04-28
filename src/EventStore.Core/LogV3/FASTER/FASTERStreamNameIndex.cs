using System;
using System.Text;
using System.Text.Unicode;
using EventStore.Core.LogAbstraction;
using EventStore.Core.Services;
using FASTER.core;

namespace EventStore.Core.LogV3.FASTER {
	// uses RMW and functions inspired by the TryAddFunctions
	public class FASTERStreamNameIndex :
		IStreamNameIndex<long>,
		IStreamIdLookup<long> {

		private static readonly Encoding _utf8NoBom = new UTF8Encoding(false, true);
		private readonly IDevice _log;
		private readonly FasterKV<SpanByte, long> _store;
		private readonly ClientSession<SpanByte, long, long, long, Context<long>, TryAddFunctions<long>> _session;
		private readonly ClientSession<SpanByte, long, long, long, Empty, ReaderFunctions<long>> _readerSession;
		private readonly Context<long> _context = new();
		//qq we seem to be using this as the prevId rather than next.
		private long _nextId = LogV3SystemStreams.FirstRealStream;

		public class Context<T> {
			public T Value { get; set; }
			public Status Status { get; set; }
		}

		public FASTERStreamNameIndex(string logPath, bool large) {
			_log = Devices.CreateLogDevice(
				logPath: logPath,
				deleteOnClose: true);

			_store = large
				? new FasterKV<SpanByte, long>(
					size: 1L << 27,
					logSettings: new LogSettings {
						LogDevice = _log,
						//PreallocateLog = true,
						//ReadCacheSettings = new ReadCacheSettings()
					})
				: new FasterKV<SpanByte, long>(
					size: 1L << 20,
					logSettings: new LogSettings {
						LogDevice = _log,
						MemorySizeBits = 15,
						PageSizeBits = 12,
						//PreallocateLog = true,
						//ReadCacheSettings = new ReadCacheSettings()
					});

			_session = _store
				.For(new TryAddFunctions<long>(_context))
				.NewSession<TryAddFunctions<long>>();

			_readerSession = _store
				.For(new ReaderFunctions<long>())
				.NewSession<ReaderFunctions<long>>();
		}

		//qq todo: arrange for this to be called
		public void Dispose() {
			_session?.Dispose();
			_readerSession?.Dispose();
			_store?.Dispose();
			_log?.Dispose();
		}

		public long LookupId(string streamName) {
			//qq do we need a separate session for each thread that reads?

			// convert the streamName into a UTF8 span which we can use as a key.
			var length = _utf8NoBom.GetByteCount(streamName);
			Span<byte> key = stackalloc byte[length];
			Utf8.FromUtf16(streamName, key, out _, out _, true, true);

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
					throw new Exception($"Error reading {streamName}: Unknown status {_context.Status} when reading");
			}
		}

		public bool GetOrAddId(string streamName, out long streamNumber, out long createdId, out string createdName) {
			if (string.IsNullOrEmpty(streamName))
				throw new ArgumentNullException(nameof(streamName));
			if (SystemStreams.IsMetastream(streamName))
				throw new ArgumentException(nameof(streamName));

			// convert the streamName into a UTF8 span which we can use as a key.
			var length = _utf8NoBom.GetByteCount(streamName);
			Span<byte> key = stackalloc byte[length];
			Utf8.FromUtf16(streamName, key, out _, out _, true, true);

			// attempt a atomic try-add via read-modify-write.
			bool preExisting;
			var idForNewStream = _nextId + 2;
			var status = _session.RMW(
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
					_session.CompletePending(wait: true);
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
					throw new Exception($"Error Get/Adding {streamName}: Unknown status {_context.Status} when performing operation");
			}

			createdId = streamNumber;
			createdName = streamName;
			return preExisting;
		}
	}
}
