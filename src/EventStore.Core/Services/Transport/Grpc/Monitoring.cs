using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using EventStore.Client.Monitoring;
using EventStore.Core.Bus;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Empty = EventStore.Client.Shared.Empty;

namespace EventStore.Core.Services.Transport.Grpc {
	public partial class Monitoring : EventStore.Client.Monitoring.Monitoring.MonitoringBase {
		private readonly IPublisher _publisher;
		
		public override async Task Stats(Empty request, IServerStreamWriter<StatsResp> responseStream, ServerCallContext context) {
			var enumerator = CollectStats();
			return base.Stats(request, responseStream, context);
		}

		public Monitoring(IPublisher publisher) {
			_publisher = publisher;
		}

		private async IAsyncEnumerable<StatsResp> CollectStats() {
			for (;;) {
				var source = new TaskCompletionSource<StatsResp>();
				var envelope = new CallbackEnvelope(message => {
					if (!(message is MonitoringMessage.GetFreshStatsCompleted completed)) {
						source.TrySetException(UnknownMessage<MonitoringMessage.GetFreshStatsCompleted>(message));
					} else {
						var jsonStr = JsonSerializer.Serialize(completed.Stats);
						Value stats;

						if (string.IsNullOrEmpty(jsonStr)) {
							stats = new Value {
								StructValue = new Struct()
							};
						} else {
							// FIXME - Maybe there is a better way of achieving this.
							stats = GetProtoValue(JsonDocument.Parse(jsonStr).RootElement);
						}

						source.TrySetResult(new StatsResp {
							Stats = stats
						});
					}
				});
				_publisher.Publish(new MonitoringMessage.GetFreshStats(envelope, x => x, false, true));
				var resp = await source.Task.ConfigureAwait(false);
				
				yield return resp;

				await Task.Delay(TimeSpan.FromSeconds(3)).ConfigureAwait(false);
			}
		}
		
		private static Exception UnknownMessage<T>(Message message) where T : Message =>
			new RpcException(
				new Status(StatusCode.Unknown,
					$"Envelope callback expected {typeof(T).Name}, received {message.GetType().Name} instead"));
		
		private static Value GetProtoValue(JsonElement element) =>
			element.ValueKind switch {
				JsonValueKind.Null => new Value {NullValue = NullValue.NullValue},
				JsonValueKind.Array => new Value {
					ListValue = new ListValue {
						Values = {
							element.EnumerateArray().Select(GetProtoValue)
						}
					}
				},
				JsonValueKind.False => new Value {BoolValue = false},
				JsonValueKind.True => new Value {BoolValue = true},
				JsonValueKind.String => new Value {StringValue = element.GetString()},
				JsonValueKind.Number => new Value {NumberValue = element.GetDouble()},
				JsonValueKind.Object => new Value {StructValue = GetProtoStruct(element)},
				JsonValueKind.Undefined => new Value(),
				_ => throw new InvalidOperationException()
			};

		private static Struct GetProtoStruct(JsonElement element) {
			var structValue = new Struct();
			foreach (var property in element.EnumerateObject()) {
				structValue.Fields.Add(property.Name, GetProtoValue(property.Value));
			}

			return structValue;
		}
	}
}
