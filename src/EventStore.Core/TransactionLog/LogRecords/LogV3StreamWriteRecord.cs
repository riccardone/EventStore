﻿using System;
using System.Text;
using EventStore.LogCommon;
using EventStore.LogV3;

namespace EventStore.Core.TransactionLog.LogRecords {
	// implements iprepare because currently the strem write contains exactly one event
	// but when we generalise it to contain muliple events i exect we will be able to remove
	// implementing iprepare here.
	public class LogV3StreamWriteRecord : LogV3Record<StreamWriteRecord>, IEquatable<LogV3StreamWriteRecord>, IPrepareLogRecord<long> {
		public LogV3StreamWriteRecord(ReadOnlyMemory<byte> bytes) : base() {
			Record = new StreamWriteRecord(new RecordView<Raw.StreamWriteHeader>(bytes));
		}

		public unsafe LogV3StreamWriteRecord(
			long logPosition,
			Guid correlationId,
			Guid eventId,
			long eventStreamId,
			long expectedVersion,
			DateTime timeStamp,
			PrepareFlags flags,
			string eventType,
			ReadOnlySpan<byte> data,
			ReadOnlySpan<byte> metadata) {

			Span<byte> eventTypeBytes = stackalloc byte[RecordCreator.MeasureString(eventType)];
			RecordCreator.PopulateString(eventType, eventTypeBytes);

			Record = RecordCreator.CreateStreamWriteRecordForSingleEvent(
				timeStamp: timeStamp,
				recordId: correlationId,
				logPosition: logPosition,
				streamNumber: eventStreamId,
				startingEventNumber: expectedVersion + 1,
				recordMetadata: ReadOnlySpan<byte>.Empty,
				eventId: eventId,
				// temporarily storing the event type as the system metadata. later it will have a number.
				eventSystemMetadata: eventTypeBytes,
				eventData: data,
				eventMetadata: metadata,
				eventFlags: flags);
		}

		public override LogRecordType RecordType => LogRecordType.Prepare;

		public PrepareFlags Flags => Record.Event.Header.Flags;
		public long TransactionPosition => Record.Header.LogPosition;
		public int TransactionOffset => 0;
		public long ExpectedVersion => Record.SubHeader.StartingEventNumber - 1;
		public long EventStreamId => Record.SubHeader.StreamNumber;
		public Guid EventId => Record.Event.Header.EventId;
		public Guid CorrelationId => Record.Header.RecordId;
		// temporarily storing the event type as the system metadata. later it will have a number.
		public string EventType => Encoding.UTF8.GetString(Record.Event.SystemMetadata.Span);
		public ReadOnlyMemory<byte> Data => Record.Event.Data;
		public ReadOnlyMemory<byte> Metadata => Record.Event.Metadata;
		public bool Equals(LogV3StreamWriteRecord other) {
			if (ReferenceEquals(null, other)) return false;
			if (ReferenceEquals(this, other)) return true;
			return 	other.Version == Version
			        && other.LogPosition == LogPosition
			        && other.TimeStamp.Equals(TimeStamp)
			        && other.RecordType == RecordType
			        && other.Flags == Flags
			        && other.TransactionPosition == TransactionPosition
			        && other.TransactionOffset == TransactionOffset
			        && other.ExpectedVersion == ExpectedVersion
			        && other.EventStreamId.Equals(EventStreamId)
			        && other.EventId == EventId
			        && other.CorrelationId == CorrelationId
			        && other.EventType.Equals(EventType)
			        && other.Data.Span.SequenceEqual(Data.Span)
			        && other.Metadata.Span.SequenceEqual(Metadata.Span);
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != this.GetType()) return false;
			return Equals((LogV3StreamWriteRecord) obj);
		}
		public override int GetHashCode() {
			unchecked {
				var result = Version.GetHashCode();
				result = (result * 397) ^ LogPosition.GetHashCode();
				result = (result * 397) ^ TimeStamp.GetHashCode();
				result = (result * 397) ^ RecordType.GetHashCode();
				result = (result * 397) ^ Flags.GetHashCode();
				result = (result * 397) ^ TransactionPosition.GetHashCode();
				result = (result * 397) ^ TransactionOffset;
				result = (result * 397) ^ ExpectedVersion.GetHashCode();
				result = (result * 397) ^ EventStreamId.GetHashCode();
				result = (result * 397) ^ EventId.GetHashCode();
				result = (result * 397) ^ CorrelationId.GetHashCode();
				result = (result * 397) ^ EventType.GetHashCode();
				result = (result * 397) ^ Data.GetHashCode();
				result = (result * 397) ^ Metadata.GetHashCode();
				return result;
			}
		}
	}
}
