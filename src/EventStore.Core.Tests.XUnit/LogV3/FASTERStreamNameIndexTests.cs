using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using EventStore.Core.LogV3;
using EventStore.Core.LogV3.FASTER;
using Xunit;

namespace EventStore.Core.Tests.XUnit.LogV3 {
	public class FASTERStreamNameIndexTests {
		readonly string _logName = $"index/{nameof(FASTERStreamNameIndexTests)}.{DateTime.Now:s}.{Guid.NewGuid()}".Replace(':', '_');
		readonly FASTERStreamNameIndex _sut;

		public FASTERStreamNameIndexTests() {
			_sut = new(_logName, large: false);
		}

		//qq this is already covered by the logabstrator tests, so just a quick check here
		// but the logabstractor tests only cover one implemenation
		// so perhaps we want some matrix tests to run against the streamnameindex implementations?
		// but some of them dont survive restarts etc so those tests will have to go in some specialisation.
		[Fact]
		public void sanity_check() {
			Assert.False(_sut.GetOrAddId("streamA", out var streamNumber, out var newNumber, out var newName));
			Assert.Equal("streamA", newName);
			Assert.Equal(streamNumber, newNumber);
		}

		[Fact]
		public async Task can_checkpoint_log() {
			Assert.False(_sut.GetOrAddId("streamA", out var streamNumber, out _, out _));
			await _sut.CheckpointLog();
			_sut.Dispose();
			
			var sut2 = new FASTERStreamNameIndex(_logName, large: false);
			Assert.True(sut2.GetOrAddId("streamA", out var streamNumberAfterRecovery, out _, out _));
			Assert.Equal(streamNumber, streamNumberAfterRecovery);
			Assert.False(sut2.GetOrAddId("streamB", out var streamNumberB, out _, out _));
			Assert.Equal(streamNumber + 2, streamNumberB);
		}

		//qq
		//[Fact]
		//public void experiments() {
		//	var store = _sut._store;

		//	var count = 0;
		//	using var iter = store.Log.Scan(0, store.Log.TailAddress);
		//	while (iter.GetNext(out var recordInfo)) {
		//		var asdf = iter.CurrentAddress;
		//		var asdfg = iter.NextAddress;
		//		ref var key = ref iter.GetKey();
		//		ref var value = ref iter.GetValue();
		//		count++;
		//	}
		//}

		static int Offset = 512; //qq magic number
		static readonly IEnumerable<(long RecordNumber, long StreamId, string StreamName)> _streamsSource =
			Enumerable
				.Range(Offset, int.MaxValue - Offset)
				.Select(x => (RecordNumber: (long) x, StreamId: (long) x * 2, StreamName: $"stream{x * 2}"));
	
		void PopulateSut(int numStreams) {
			_streamsSource.Take(numStreams).ToList().ForEach(tuple => {
				_sut.GetOrAddId(tuple.StreamName, out var streamId, out var _, out var _);
				Assert.Equal(tuple.StreamId, streamId);
			});
		}

		void DeleteStreams(int numTotalStreams, int numToDelete) {
			_streamsSource.Skip(numTotalStreams - numToDelete).Take(numToDelete).ToList().ForEach(tuple =>
				_sut.Delete(tuple.StreamName));
		}

		static IList<(long RecordNumber, long StreamId, string StreamName)> GenerateStreamsStream(int numStreams) {
			return _streamsSource.Take(numStreams).ToList();
		}

		void Test(int numInStreamNameIndex, int numInStandardIndex) {
			// given
			PopulateSut(numInStreamNameIndex);
			var streamsStream = GenerateStreamsStream(numInStandardIndex);

			// when
			_sut.Init(streamsStream.ToDictionary(x => x.StreamId, x => x.StreamName));

			// then
			// now we have caught up we should be able to check that both indexes are equal
			// check that all of the streams stream is in the stream name index
			var i = 0;
			for (; i < streamsStream.Count; i++) {
				var tuple = streamsStream[i];
				Assert.True(_sut.GetOrAddId(tuple.StreamName, out var streamId, out var _, out var _));
				Assert.Equal(tuple.StreamId, streamId);
			}

			{
				// check that the streamnameindex doesn't contain anything extra.
				//qqqqq this isn't sufficient really
				Assert.False(_sut.GetOrAddId($"{Guid.NewGuid()}", out var streamId, out var _, out var _));
				Assert.Equal(streamsStream.Last().StreamId + 2, streamId);
			}
		}

		[Fact]
		public void can_write() {
			var numStreams = 5000;
			PopulateSut(numStreams);

			Assert.True(_sut.GetOrAddId("stream4500", out var streamNumber, out var newNumber, out var newName));
			Assert.Equal("stream4500", newName);
			Assert.Equal(4500, streamNumber);

			Assert.Equal(4500, _sut.LookupId("stream4500"));
		}

		[Fact]
		public void can_wtf() {
			var numStreams = 2000;
			PopulateSut(numStreams);

			//			Assert.True(_sut.GetOrAddId("stream1030", out var streamNumber, out var newNumber, out var newName));
			//			Assert.Equal(1030, streamNumber);

			Assert.Equal(1026, _sut.LookupId("stream1026"));
			Assert.Equal(1026, _sut.LookupId("stream1026"));
			Assert.Equal(1026, _sut.LookupId("stream1026"));
		}

		//qq probably temp
		[Fact]
		public void can_scan() {
			var numStreams = 5000;
			PopulateSut(numStreams);

			var scanned = _sut.Scan().ToList();

			//qq horrible numbers, probbly want a loop so we can test across pages
			Assert.Equal(numStreams, scanned.Count);
			Assert.Equal(1024, scanned[0].Item1);
			Assert.Equal(1026, scanned[1].Item1);
			Assert.Equal(1028, scanned[2].Item1);
			Assert.Equal(1030, scanned[3].Item1);
			Assert.Equal(1032, scanned[4].Item1);
		}

		//qq probably temp
		[Fact]
		public void can_scan_backwards() {
			var numStreams = 5000;
			PopulateSut(numStreams);

			var scanned = _sut.ScanBackwards().ToList();
			scanned.Reverse();

			Assert.Equal(numStreams, scanned.Count);

			var expectedStreamId = LogV3SystemStreams.FirstRealStream;
			for (int i = 0; i < numStreams; i++) {
				Assert.Equal(expectedStreamId, scanned[i].StreamId);
				Assert.Equal($"stream{expectedStreamId}", scanned[i].StreamName);
				expectedStreamId += LogV3SystemStreams.StreamInterval;
			}
		}

		[Fact]
		public void can_scan_empty_range() {
			var numStreams = 10000;
			PopulateSut(numStreams);

			var scanned = _sut.Scan(0, 0).ToList();
			Assert.Empty(scanned);
		}

		[Fact]
		public void can_scan_forwards_skipping_deleted() {
			var numStreams = 10000;
			var deletedStreams = 500;
			var remainingStreams = numStreams - deletedStreams;
			PopulateSut(numStreams);
			DeleteStreams(numStreams, deletedStreams);

			var scanned = _sut.Scan().ToList();

			Assert.Equal(remainingStreams, scanned.Count);

			var expectedStreamId = LogV3SystemStreams.FirstRealStream;
			for (int i = 0 ; i < remainingStreams; i++) {
				Assert.Equal(expectedStreamId, scanned[i].StreamId);
				Assert.Equal($"stream{expectedStreamId}", scanned[i].StreamName);
				expectedStreamId += LogV3SystemStreams.StreamInterval;
			}
		}

		[Fact]
		public void can_scan_backwards_skipping_deleted() {
			var numStreams = 10000;
			var deletedStreams = 500;
			var remainingStreams = numStreams - deletedStreams;
			PopulateSut(numStreams);
			DeleteStreams(numStreams, deletedStreams);

			var scanned = _sut.ScanBackwards().ToList();
			scanned.Reverse();

			Assert.Equal(remainingStreams, scanned.Count);

			var expectedStreamId = LogV3SystemStreams.FirstRealStream;
			for (int i = 0; i < remainingStreams; i++) {
				Assert.Equal(expectedStreamId, scanned[i].StreamId);
				Assert.Equal($"stream{expectedStreamId}", scanned[i].StreamName);
				expectedStreamId += LogV3SystemStreams.StreamInterval;
			}
		}

		[Fact]
		public void on_init_can_catchup() {
			Test(
				numInStreamNameIndex: 3,
				numInStandardIndex: 5);
		}

		[Fact]
		public void on_init_can_catchup_from_0() {
			Test(
				numInStreamNameIndex: 0,
				numInStandardIndex: 5);
		}

		//
		// we must make sure that the two indexes do not ever diverge. one must be the start of the other.
		// maybe checking that we can catchup after restoring from persisted?

		[Fact]
		public void on_init_can_truncate() {
			Test(
				numInStreamNameIndex: 5,
				numInStandardIndex: 3);
		}

		[Fact]
		public void can_recover_after_truncating() {
			// make sure we dont get a problem if we do something like recovery twice.
			// i.e. do one recovery where we delete the last entry, then replace it with another
			// (same number, different name), but neither the deletion or the recreation are persisted
			// now when we restart it will have diverted.
			throw new NotImplementedException();
		}
	}
}
