﻿using EventStore.Common.Utils;
using EventStore.Core.Index.Hashes;
using EventStore.Core.LogV2;
using EventStore.Core.LogV3;
using EventStore.Core.LogV3.FASTER;

namespace EventStore.Core.LogAbstraction {
	public class LogFormatAbstractor {
		public static LogFormatAbstractor<string> V2 { get; } = CreateV2();
		public static LogFormatAbstractor<long> V3 { get; } = CreateV3();

		public static LogFormatAbstractor<string> CreateV2() {
			var streamNameIndex = new LogV2StreamNameIndex();
			return new LogFormatAbstractor<string>(
				new XXHashUnsafe(),
				new Murmur3AUnsafe(),
				streamNameIndex,
				streamNameIndex,
				new SingletonStreamNamesProvider<string>(new LogV2SystemStreams(), streamNameIndex),
				new LogV2StreamIdValidator(),
				emptyStreamId: string.Empty,
				new LogV2Sizer(),
				new LogV2RecordFactory());
		}

		public static LogFormatAbstractor<long> CreateV3() {
//			var logV3StreamNameIndex = new InMemoryStreamNameIndex();
//qq we are going to need to pass this in and it be different for differnt concurrent tests i think
			var logV3StreamNameIndex = new FASTERStreamNameIndex("stream-name-index.idx", large: false);
			var metastreams = new LogV3Metastreams();
			return new LogFormatAbstractor<long>(
				lowHasher: new IdentityLowHasher(),
				highHasher: new IdentityHighHasher(),
				streamNameIndex: new StreamNameIndexMetastreamDecorator(logV3StreamNameIndex, metastreams),
				streamIds: new StreamIdLookupMetastreamDecorator(logV3StreamNameIndex, metastreams),
				streamNamesProvider: new AdHocStreamNamesProvider<long>(indexReader => {
					IStreamNameLookup<long> streamNames = new StreamIdToNameFromStandardIndex(indexReader);
					var systemStreams = new LogV3SystemStreams(metastreams, streamNames);
					streamNames = new StreamNameLookupMetastreamDecorator(streamNames, metastreams);
					return (systemStreams, streamNames);
				}),
				streamIdValidator: new LogV3StreamIdValidator(),
				emptyStreamId: 0,
				streamIdSizer: new LogV3Sizer(),
				recordFactory: new LogV3RecordFactory());
		}
	}

	public class LogFormatAbstractor<TStreamId> : LogFormatAbstractor {
		public LogFormatAbstractor(
			IHasher<TStreamId> lowHasher,
			IHasher<TStreamId> highHasher,
			IStreamNameIndex<TStreamId> streamNameIndex,
			IStreamIdLookup<TStreamId> streamIds,
			IStreamNamesProvider<TStreamId> streamNamesProvider,
			IValidator<TStreamId> streamIdValidator,
			TStreamId emptyStreamId,
			ISizer<TStreamId> streamIdSizer,
			IRecordFactory<TStreamId> recordFactory) {

			LowHasher = lowHasher;
			HighHasher = highHasher;
			StreamNameIndex = streamNameIndex;
			StreamIds = streamIds;
			StreamNamesProvider = streamNamesProvider;
			StreamIdValidator = streamIdValidator;
			EmptyStreamId = emptyStreamId;
			StreamIdSizer = streamIdSizer;
			RecordFactory = recordFactory;
		}

		public IHasher<TStreamId> LowHasher { get; }
		public IHasher<TStreamId> HighHasher { get; }
		public IStreamNameIndex<TStreamId> StreamNameIndex { get; }
		public IStreamIdLookup<TStreamId> StreamIds { get; }
		public IStreamNamesProvider<TStreamId> StreamNamesProvider { get; }
		public IValidator<TStreamId> StreamIdValidator { get; }
		public TStreamId EmptyStreamId { get; }
		public ISizer<TStreamId> StreamIdSizer { get; }
		public IRecordFactory<TStreamId> RecordFactory { get; }

		public IStreamNameLookup<TStreamId> StreamNames => StreamNamesProvider.StreamNames;
		public ISystemStreamLookup<TStreamId> SystemStreams => StreamNamesProvider.SystemStreams;
	}
}
