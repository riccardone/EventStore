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

		static readonly IEnumerable<(long RecordNumber, long StreamId, string StreamName)> _streamsSource =
			Enumerable
				.Range(513, 1000)
				.Select(x => (RecordNumber: (long) x, StreamId: (long) x * 2, StreamName: $"stream{x * 2}"));
	
		void PopulateSut(int numStreams) {
			_streamsSource.Take(numStreams).ToList().ForEach(tuple => {
				_sut.GetOrAddId(tuple.StreamName, out var streamId, out var _, out var _);
				Assert.Equal(tuple.StreamId, streamId);
			});
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
