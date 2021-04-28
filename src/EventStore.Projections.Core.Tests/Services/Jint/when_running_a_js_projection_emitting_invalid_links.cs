using System;
using EventStore.Projections.Core.Services;
using EventStore.Projections.Core.Services.Processing;
using NUnit.Framework;

namespace EventStore.Projections.Core.Tests.Services.Jint
{
	public class when_running_a_js_projection_emitting_invalid_links : TestFixtureWithInterpretedProjection {
		protected override void Given() {
			_projection = @"
                fromAll().when({$any: 
                    function(state, event) {
                        event.sequenceNumber = null;
                    linkTo('output-stream', event);
                    return {};
                }});
            ";
		}
		
		[Test, Category(_projectionType)]
		public void process_event_ignores_emitted_event() {
			string state;
			EmittedEventEnvelope[] emittedEvents;
			_stateHandler.ProcessEvent(
				"", CheckpointTag.FromPosition(0, 20, 10), "stream1", "type1", "category", Guid.NewGuid(), 0,
				"metadata",
				@"{""a"":""b""}", out state, out emittedEvents);

			Assert.IsNull(emittedEvents);
		}
	}
}