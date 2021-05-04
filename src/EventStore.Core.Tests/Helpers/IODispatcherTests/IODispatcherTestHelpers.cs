using System;
using System.Text;
using EventStore.Core.Data;
using EventStore.Core.Helpers;
using EventStore.Core.Bus;
using EventStore.Core.LogAbstraction;
using EventStore.Core.TransactionLog.LogRecords;
using ResolvedEvent = EventStore.Core.Data.ResolvedEvent;

namespace EventStore.Core.Tests.Helpers.IODispatcherTests {
	public static class IODispatcherTestHelpers {
		public static ResolvedEvent[] CreateResolvedEvent(string streamId, string eventType, string data,
			string metadata = "", long eventNumber = 0) {
			var recordFactory = LogFormatAbstractor.V2.RecordFactory;
			var record = new EventRecord(eventNumber, LogRecord.Prepare(recordFactory, 0, Guid.NewGuid(), Guid.NewGuid(), 0, 0,
				streamId, eventNumber, PrepareFlags.None, eventType, Encoding.UTF8.GetBytes(data),
				Encoding.UTF8.GetBytes(metadata)), streamId);
			return new ResolvedEvent[] {
				ResolvedEvent.ForUnresolvedEvent(record, 0)
			};
		}

		public static void SubscribeIODispatcher(IODispatcher ioDispatcher, IBus bus) {
			bus.Subscribe(ioDispatcher);
			bus.Subscribe(ioDispatcher.ForwardReader);
			bus.Subscribe(ioDispatcher.BackwardReader);
			bus.Subscribe(ioDispatcher.Writer);
			bus.Subscribe(ioDispatcher.Awaker);
			bus.Subscribe(ioDispatcher.StreamDeleter);
		}
	}
}
