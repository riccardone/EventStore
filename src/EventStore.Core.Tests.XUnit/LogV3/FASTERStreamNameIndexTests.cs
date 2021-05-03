using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using EventStore.Core.LogV3.FASTER;
using Xunit;

namespace EventStore.Core.Tests.XUnit.LogV3 {
	public class FASTERStreamNameIndexTests {
		readonly string _logName = $"index/{nameof(FASTERStreamNameIndexTests)}.{DateTime.Now:s}".Replace(':', '_');
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
		public async Task can_persist() {
			Assert.False(_sut.GetOrAddId("streamA", out var streamNumber, out _, out _));
			await _sut.Persist();
			_sut.Dispose();
			
			var sut2 = new FASTERStreamNameIndex(_logName, large: false);
			Assert.True(sut2.GetOrAddId("streamA", out var streamNumberAfterRecovery, out _, out _));
			Assert.Equal(streamNumber, streamNumberAfterRecovery);
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

		static int Offset = 513; //qq magic number
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

		IList<(long RecordNumber, long StreamId, string StreamName)> GenerateStreamsStream(int numStreams) {
			return _streamsSource.Take(numStreams).ToList();
		}

		void Test(int numInStreamNameIndex, int numInStandardIndex) {
			// given
			PopulateSut(numInStreamNameIndex);
			var streamsStream = GenerateStreamsStream(numInStandardIndex);

			// when
			_sut.Init(streamsStream);

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
			Assert.Equal(1026, scanned[0].Item1);
			Assert.Equal(1028, scanned[1].Item1);
			Assert.Equal(1030, scanned[2].Item1);
			Assert.Equal(1032, scanned[3].Item1);
			Assert.Equal(1034, scanned[4].Item1);
		}

		//qq probably temp
		[Fact]
		public void can_scan_backwards() {
			var numStreams = 1000;
			PopulateSut(numStreams);

			var scanned = _sut.ScanBackwards().ToList();

			Assert.Equal(numStreams, scanned.Count);

			for (int i = numStreams - 1; i >= 0; i--) {
				var expectedStreamId = 1024 + 2 * (numStreams - i);
				Assert.Equal(expectedStreamId, scanned[i].Item1);
				Assert.Equal($"stream{expectedStreamId}", scanned[i].Item2);
			}
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

			for (int i = remainingStreams - 1; i >= 0; i--) {
				//qqqq fix this
				var expectedStreamId = 1024 + 2 * (remainingStreams - i);
				Assert.Equal(expectedStreamId, scanned[i].Item1);
				Assert.Equal($"stream{expectedStreamId}", scanned[i].Item2);
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

			Assert.Equal(remainingStreams, scanned.Count);

			for (int i = remainingStreams - 1; i >= 0; i--) {
				var expectedStreamId = 1024 + 2 * (remainingStreams - i);
				Assert.Equal(expectedStreamId, scanned[i].Item1);
				Assert.Equal($"stream{expectedStreamId}", scanned[i].Item2);
			}
		}

		[Fact]
		public void on_init_can_catchup() {
			// in a catchup scenario the standard index has more records than the stream name index.
			// we need to add them.
			//
			//qq we need to find the last entry that is in the faster log.
			// in faster it seems you can't scan backwards, so we are going to need to get a begin address
			// from somewhere that we know to be before the end of the log.
			//
			// 1. have a chaser that follows along as records get committed and checkpoints them.
			// 2. use entries from the standard index.
			//
			// lets go with option 2 because it involves less ongoing work as the system is running.
			//
			// so we need to get the address of a record that we just read. perhaps the advanced functions can help us with that?

			//qq dont hardcode 514 here.

			Test(
				numInStreamNameIndex: 3,
				numInStandardIndex: 5);


			//qq idea: lets iterate through and see what is there.
			// perhaps we can grab the last entry in the standard index to see if it is present.
			// if not we proceed backwards through the index until we find one that is present, and then
			// go forward from there putting them in.
			//
			// if the first one is present, then we need to delete everything after that point
			// which means we need to scan forwards starting from the address of that last record.
			//
			// OR we could have a chaser that chases along whatever has been persisted to disk and
			// writes the address in a mem mapped checkpoint.


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
