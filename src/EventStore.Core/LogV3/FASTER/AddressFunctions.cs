using System;
using FASTER.core;

namespace EventStore.Core.LogV3.FASTER {
	public class AddressFunctions<TValue> : IAdvancedFunctions<SpanByte, long> {
		public bool SupportsLocking => false;

		public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint) {
			throw new NotImplementedException();
		}

		public void ConcurrentDeleter(ref SpanByte key, ref long value, ref RecordInfo recordInfo, long address) {
			throw new NotImplementedException();
		}

		public void ConcurrentReader(ref SpanByte key, ref long input, ref long value, ref long dst, ref RecordInfo recordInfo, long address) {
			dst = address;
		}

		//qq why does it sometimes call single and sometimes call concurrent, what difference does it make to us?
		public void SingleReader(ref SpanByte key, ref long input, ref long value, ref long dst, long address) {
			dst = address;
		}

		public bool ConcurrentWriter(ref SpanByte key, ref long src, ref long dst, ref RecordInfo recordInfo, long address) {
			throw new NotImplementedException();
		}

		public void CopyUpdater(ref SpanByte key, ref long input, ref long oldValue, ref long newValue) {
			throw new NotImplementedException();
		}

		public void DeleteCompletionCallback(ref SpanByte key, Empty ctx) {
			throw new NotImplementedException();
		}

		public void InitialUpdater(ref SpanByte key, ref long input, ref long value) {
			throw new NotImplementedException();
		}

		public bool InPlaceUpdater(ref SpanByte key, ref long input, ref long value, ref RecordInfo recordInfo, long address) {
			throw new NotImplementedException();
		}

		public void Lock(ref RecordInfo recordInfo, ref SpanByte key, ref long value, LockType lockType, ref long lockContext) {
			throw new NotImplementedException();
		}

		public void ReadCompletionCallback(ref SpanByte key, ref long input, ref long output, Empty ctx, Status status, RecordInfo recordInfo) {
			throw new NotImplementedException();
		}

		public void RMWCompletionCallback(ref SpanByte key, ref long input, Empty ctx, Status status) {
			throw new NotImplementedException();
		}

		public void SingleWriter(ref SpanByte key, ref long src, ref long dst) {
			throw new NotImplementedException();
		}

		public bool Unlock(ref RecordInfo recordInfo, ref SpanByte key, ref long value, LockType lockType, long lockContext) {
			throw new NotImplementedException();
		}

		public void UpsertCompletionCallback(ref SpanByte key, ref long value, Empty ctx) {
			throw new NotImplementedException();
		}
	}
}

