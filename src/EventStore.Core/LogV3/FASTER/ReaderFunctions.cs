using System;
using FASTER.core;

namespace EventStore.Core.LogV3.FASTER {
	public class ReaderFunctions<TValue> : IFunctions<SpanByte, TValue, ReaderFunctions<TValue>.Context> {
		public class Context { 
			public Status Status { get; set; }
			public TValue Output { get; set; }
		}

		//qq dont think we need locking, but check what this is exactly
		public bool SupportsLocking => false;

		public void ConcurrentReader(ref SpanByte key, ref TValue input, ref TValue value, ref TValue dst) {
			dst = value;
		}

		public void SingleReader(ref SpanByte key, ref TValue input, ref TValue value, ref TValue dst) {
			dst = value;
		}

		public void ReadCompletionCallback(ref SpanByte key, ref TValue input, ref TValue output, Context context, Status status) {
			context.Status = status;
			context.Output = output;
		}

		public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint) =>
			throw new NotImplementedException();

		public bool ConcurrentWriter(ref SpanByte key, ref TValue src, ref TValue dst) =>
			throw new NotImplementedException();

		public void CopyUpdater(ref SpanByte key, ref TValue input, ref TValue oldValue, ref TValue newValue) =>
			throw new NotImplementedException();

		public void DeleteCompletionCallback(ref SpanByte key, Context context) =>
			throw new NotImplementedException();

		public void InitialUpdater(ref SpanByte key, ref TValue input, ref TValue value) =>
			throw new NotImplementedException();

		public bool InPlaceUpdater(ref SpanByte key, ref TValue input, ref TValue value) =>
			throw new NotImplementedException();

		public void Lock(ref RecordInfo recordInfo, ref SpanByte key, ref TValue value, LockType lockType, ref long lockContext) =>
			throw new NotImplementedException();

		public void RMWCompletionCallback(ref SpanByte key, ref TValue input, Context context, Status status) =>
			throw new NotImplementedException();

		public void SingleWriter(ref SpanByte key, ref TValue src, ref TValue dst) =>
			throw new NotImplementedException();

		public bool Unlock(ref RecordInfo recordInfo, ref SpanByte key, ref TValue value, LockType lockType, long lockContext) =>
			throw new NotImplementedException();

		public void UpsertCompletionCallback(ref SpanByte key, ref TValue value, Context context) =>
			throw new NotImplementedException();
	}
}

