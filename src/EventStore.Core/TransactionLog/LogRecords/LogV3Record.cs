﻿using System;
using System.IO;
using EventStore.LogCommon;
using EventStore.LogV3;

namespace EventStore.Core.TransactionLog.LogRecords {
	// This is the adapter to plug V3 records into the standard machinery.
	public class LogV3Record<T> : ILogRecord where T : unmanaged {
		public RecordView<T> Record { get; init; }

		public long GetNextLogPosition(long logicalPosition, int length) {
			return logicalPosition + length + 2 * sizeof(int);
		}

		public long GetPrevLogPosition(long logicalPosition, int length) {
			return logicalPosition - length - 2 * sizeof(int);
		}

		public LogRecordType RecordType => Record.Header.Type;

		public byte Version => Record.Header.Version;

		public long LogPosition => Record.Header.LogPosition;

		public DateTime TimeStamp => Record.Header.TimeStamp;

		public LogV3Record() {
		}

		public LogV3Record(ReadOnlyMemory<byte> populatedBytes) {
			Record = new RecordView<T>(populatedBytes);
		}

		public void WriteTo(BinaryWriter writer) {
			writer.Write(Record.Bytes.Span);
		}

		public int GetSizeWithLengthPrefixAndSuffix() {
			return 2 * sizeof(int) + Record.Bytes.Length;
		}
	}
}
