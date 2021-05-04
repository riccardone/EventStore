using System;
using EventStore.Common.Utils;
using FASTER.core;

namespace EventStore.Core.LogV3.FASTER {
	public class TryAddFunctions<TValue> : IFunctions<SpanByte, TValue, TValue, TValue, TryAddFunctions<TValue>.Context> {
		public class Context {
			public TValue Value { get; set; }
			public Status Status { get; set; }
		}

		readonly Context _context;

		//qq dont think we need locking, but check what this is exactly
		public bool SupportsLocking => false;

		public TryAddFunctions(Context context) : base() {
			_context = context;
		}

		public void RMWCompletionCallback(ref SpanByte key, ref TValue input, Context context, Status status) {
			context.Status = status;
		}

		// when inserting with RMW
		public void InitialUpdater(ref SpanByte key, ref TValue input, ref TValue value) {
			value = input;
		}

		public bool InPlaceUpdater(ref SpanByte key, ref TValue input, ref TValue value) {
			_context.Value = value;
			return true;
		}

		public bool NeedCopyUpdate(ref SpanByte key, ref TValue input, ref TValue oldValue) {
			_context.Value = oldValue;
			return false;
		}

		public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint) {
			if (commitPoint.ExcludedSerialNos?.Count != 0)
				throw new Exception($"{commitPoint.ExcludedSerialNos?.Count} serial numbers were excluded from checkpoint. This is not expected to ever happen");
			//qq use logger.
			// is this the serial number i provided for the operation or will faster count it for me? if it counts it for me, is it per session or global
			Console.WriteLine($"#### WRITER COMMITTED UNTIL {commitPoint.UntilSerialNo}");
		}

		// dont need any of the following
		public void SingleReader(ref SpanByte key, ref TValue input, ref TValue value, ref TValue dst) =>
			throw new NotImplementedException();

		public void ConcurrentReader(ref SpanByte key, ref TValue input, ref TValue value, ref TValue dst) =>
			throw new NotImplementedException();

		public void CopyUpdater(ref SpanByte key, ref TValue input, ref TValue oldValue, ref TValue newValue) =>
			throw new NotImplementedException();

		public void ReadCompletionCallback(ref SpanByte key, ref TValue input, ref TValue output, Context ctx, Status status) =>
			throw new NotImplementedException();

		public void UpsertCompletionCallback(ref SpanByte key, ref TValue value, Context ctx) =>
			throw new NotImplementedException();

		public void DeleteCompletionCallback(ref SpanByte key, Context ctx) =>
			throw new NotImplementedException();


		public void SingleWriter(ref SpanByte key, ref TValue src, ref TValue dst) =>
			throw new NotImplementedException();

		public bool ConcurrentWriter(ref SpanByte key, ref TValue src, ref TValue dst) =>
			throw new NotImplementedException();

		public void Lock(ref RecordInfo recordInfo, ref SpanByte key, ref TValue value, LockType lockType, ref long lockContext) =>
			throw new NotImplementedException();

		public bool Unlock(ref RecordInfo recordInfo, ref SpanByte key, ref TValue value, LockType lockType, long lockContext) =>
			throw new NotImplementedException();
	}
}

