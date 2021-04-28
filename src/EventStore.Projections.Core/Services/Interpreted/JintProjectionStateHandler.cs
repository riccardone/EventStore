using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using EventStore.Core.Services;
using EventStore.Projections.Core.Messages;
using EventStore.Projections.Core.Services.Processing;
using Jint;
using Jint.Native;
using Jint.Native.Array;
using Jint.Native.Function;
using Jint.Native.Json;
using Jint.Native.Object;
using Jint.Runtime;
using Jint.Runtime.Descriptors;
using Jint.Runtime.Interop;


#nullable enable
namespace EventStore.Projections.Core.Services.Interpreted {
	public class JintProjectionStateHandler : IProjectionStateHandler {
		private static readonly Serilog.ILogger _log = Serilog.Log.ForContext<JintProjectionStateHandler>();
		private readonly Action<string, object[]> _logger;
		private readonly bool _enableContentTypeValidation;
		static readonly Stopwatch _sw = Stopwatch.StartNew();
		private readonly Engine _engine;
		private readonly SourceDefinitionBuilder _definitionBuilder;
		private readonly TimeConstraint _timeConstraint;
		private readonly List<EmittedEventEnvelope> _emitted;
		private readonly Selector _selector;
		private readonly JsonInstance _json;
		private CheckpointTag? _currentPosition;

		private JsValue _state;
		private JsValue _sharedState;


		public JintProjectionStateHandler(string source, Action<string, object[]> logger, bool enableContentTypeValidation, TimeSpan compilationTimeout, TimeSpan executionTimeout) {
			_logger = logger;
			_enableContentTypeValidation = enableContentTypeValidation;
			_definitionBuilder = new SourceDefinitionBuilder();
			_definitionBuilder.NoWhen();
			_definitionBuilder.AllEvents();
			_timeConstraint = new TimeConstraint(compilationTimeout, executionTimeout);
			_engine = new Engine(opts => opts.Constraint(_timeConstraint));

			_state = JsValue.Undefined;
			_sharedState = JsValue.Undefined;
			_selector = new Selector(_engine, _definitionBuilder);
			_json = JsonInstance.CreateJsonObject(_engine);
			_engine.Global.FastAddProperty("log", new ClrFunctionInstance(_engine, "log", Log), false, false, false);
			_engine.Global.FastAddProperty("options", new ClrFunctionInstance(_engine, "options", _selector.SetOptions, 1), true, false, true);
			_engine.Global.FastAddProperty("fromStream", new ClrFunctionInstance(_engine, "fromStream", _selector.FromStream, 1), true, false, true);
			_engine.Global.FastAddProperty("fromCategory", new ClrFunctionInstance(_engine, "fromCategory", _selector.FromCategory, 4), true, false, true);
			_engine.Global.FastAddProperty("fromCategories", new ClrFunctionInstance(_engine, "fromCategories", _selector.FromCategory, 4), true, false, true);
			_engine.Global.FastAddProperty("fromAll", new ClrFunctionInstance(_engine, "fromAll", _selector.FromAll, 0), true, false, true);
			_engine.Global.FastAddProperty("fromStreams", new ClrFunctionInstance(_engine, "fromStreams", _selector.FromStreams, 0), true, false, true);

			_timeConstraint.Compiling();
			_engine.Execute(source);
			_timeConstraint.Executing();
			_engine.Global.FastAddProperty("emit", new ClrFunctionInstance(_engine, "emit", Emit, 4), true, false, true);
			_engine.Global.FastAddProperty("linkTo", new ClrFunctionInstance(_engine, "linkTo", LinkTo, 3), true, false, true);
			_engine.Global.FastAddProperty("linkStreamTo", new ClrFunctionInstance(_engine, "linkStreamTo", LinkStreamTo, 3), true, false, true);
			_engine.Global.FastAddProperty("copyTo", new ClrFunctionInstance(_engine, "copyTo", CopyTo, 3), true, false, true);
			_emitted = new List<EmittedEventEnvelope>();
		}

		void Log(string message) {
			_logger(message, Array.Empty<object>());
		}

		JsValue Log(JsValue thisValue, JsValue[] parameters) {
			if (parameters.Length == 0)
				return JsValue.Undefined;
			if (parameters.Length == 1) {
				var p0 = parameters.At(0);
				if (p0 != null && p0.IsPrimitive())
					Log(p0.ToString());
				if (p0 is ObjectInstance oi)
					Log(_json.Stringify(JsValue.Undefined, new[] { oi }).AsString());
			}
			if (parameters.Length > 1) { }
			return JsValue.Undefined;
		}

		public void Dispose() {
		}

		public IQuerySources GetSourceDefinition() {
			_engine.ResetConstraints();
			return _definitionBuilder.Build();
		}

		public void Load(string state) {
			_engine.ResetConstraints();
			if (state != null) {
				var jsValue = _json.Parse(_selector, new JsValue[] {new JsString(state)});
				LoadCurrentState(jsValue);
			} else {
				LoadCurrentState(JsValue.Null);
			}
		}

		private void LoadCurrentState(JsValue jsValue)
		{
			if (_definitionBuilder.IsBiState)
			{
				if (_state == null || _state == JsValue.Undefined)
					_state = new ArrayInstance(_engine, new[]
					{
						PropertyDescriptor.Undefined, PropertyDescriptor.Undefined
					});

				_state.AsArray().Set(0, jsValue);
			}
			else
			{
				_state = jsValue;
			}
		}

		public void LoadShared(string state) {
			_engine.ResetConstraints();
			var jsValue = _json.Parse(_selector, new JsValue[] {new JsString(state)});
			LoadCurrentSharedState(jsValue);
		}

		private void LoadCurrentSharedState(JsValue jsValue)
		{
			if (_state == null || _state == JsValue.Undefined)
				_state = new ArrayInstance(_engine, new[]
				{
					PropertyDescriptor.Undefined, PropertyDescriptor.Undefined
				});

			if (!_definitionBuilder.IsBiState || !_state.IsArray())
				throw new InvalidOperationException("projection is not biState");

			_state.AsArray().Set(1, jsValue);
		}

		public void Initialize() {
			_engine.ResetConstraints();
			var state = _selector.InitializeState();
			LoadCurrentState(state);
			
		}

		public void InitializeShared() {
			_engine.ResetConstraints();
			_sharedState = _selector.InitializeSharedState();
			LoadCurrentSharedState(_sharedState);
		}

		public string? GetStatePartition(CheckpointTag eventPosition, string category, ResolvedEvent data) {
			_currentPosition = eventPosition;
			_engine.ResetConstraints();
			var envelope = CreateEnvelope("", data);
			var partition = _selector.GetPartition(envelope);
			if (partition == JsValue.Null || partition == JsValue.Undefined || !partition.IsString())
				return null;
			return partition.AsString();
		}

		public bool ProcessPartitionCreated(string partition, CheckpointTag createPosition, ResolvedEvent @event,
			out EmittedEventEnvelope[]? emittedEvents) {
			_engine.ResetConstraints();
			_currentPosition = createPosition;
			var envelope = CreateEnvelope(partition, @event);
			_selector.HandleCreated(_state, envelope);

			emittedEvents = _emitted.Count > 0 ? _emitted.ToArray() : null;
			_emitted.Clear();
			return true;
		}

		public bool ProcessPartitionDeleted(string partition, CheckpointTag deletePosition, out string newState) {
			_engine.ResetConstraints();
			_currentPosition = deletePosition;
			throw new NotImplementedException();
		}

		public string? TransformStateToResult() {
			_engine.ResetConstraints();
			var result = _selector.TransformStateToResult(_state);
			if (result == JsValue.Null || result == JsValue.Undefined) return null;
			return _json.Stringify(JsValue.Null, new[] { result }).AsString();
		}

		public bool ProcessEvent(string partition, CheckpointTag eventPosition, string category, ResolvedEvent @event,
			out string? newState, out string? newSharedState, out EmittedEventEnvelope[]? emittedEvents) {
			_currentPosition = eventPosition;
			_engine.ResetConstraints();
			if ((@event.IsJson && string.IsNullOrWhiteSpace(@event.Data)) ||
			    (!_enableContentTypeValidation && !@event.IsJson && string.IsNullOrEmpty(@event.Data))) {
				PrepareOutput(out newState, out newSharedState, out emittedEvents);
				return true;
			}
			
			
			var envelope = CreateEnvelope(partition, @event);
			_state = _selector.Handle(_state, envelope);
			PrepareOutput(out newState, out newSharedState, out emittedEvents);
			return true;
		}

		private void PrepareOutput(out string? newState, out string? newSharedState, out EmittedEventEnvelope[]? emittedEvents) {
			if (_definitionBuilder.IsBiState && _state.IsArray()) {
				var arr = _state.AsArray();
				if(arr.TryGetValue(0, out var state)) {
					newState = ConvertToStringHandlingNulls(_json,state);
				} else {
					newState = "";
				}

				if (arr.TryGetValue(1, out var sharedState)) {
					newSharedState = ConvertToStringHandlingNulls(_json,sharedState);
				} else {
					newSharedState = null;
				}
				
			} else {
				newState = ConvertToStringHandlingNulls(_json, _state);
				newSharedState = null;
			}
			emittedEvents = _emitted.Count > 0 ? _emitted.ToArray() : null;
			_emitted.Clear();

			static string? ConvertToStringHandlingNulls(JsonInstance json,JsValue value) {
				if (value.IsNull() || value.IsUndefined()) return null;
				return json.Stringify(JsValue.Undefined, new[] {value}).AsString();
			}
		}

		private EventEnvelope CreateEnvelope(string partition, ResolvedEvent @event) {
			var envelope = new EventEnvelope(_engine, _json);
			envelope.Partition = partition;
			envelope.BodyRaw = @event.Data;
			envelope.MetadataRaw = @event.Metadata;
			envelope.StreamId = @event.EventStreamId;
			envelope.EventType = @event.EventType;
			envelope.LinkMetadataRaw = @event.PositionMetadata;
			envelope.IsJson = @event.IsJson;
			//sadly we cannot trust isJson right now, need to check with HJC
			//TODO: category?
			envelope.SequenceNumber = @event.EventSequenceNumber;
			return envelope;
		}


		JsValue Emit(JsValue thisValue, JsValue[] parameters) {
			if (parameters.Length < 3)
				throw new ArgumentException("invalid number of parameters");//qq we just log warnings when an event is invalid but it seems more useful to fault the projection if it is emitting invalid events

			string stream = EnsureNonNullStringValue(parameters.At(0), "streamId");
			var eventType = EnsureNonNullStringValue(parameters.At(1), "eventName");
			var eventBody = EnsureNonNullObjectValue(parameters.At(2), "eventBody");

			if (parameters.Length == 4 && !parameters.At(3).IsObject())
				throw new ArgumentException("object expected", "metadata");

			var data = _json.Stringify(JsValue.Undefined, new JsValue[] { eventBody }).AsString();
			ExtraMetaData? metadata = null;
			if (parameters.Length == 4) {
				var md = parameters.At(3).AsObject();
				var d = new Dictionary<string, string?>();
				foreach (var kvp in md.GetOwnProperties()) {
					d.Add(kvp.Key.AsString(), AsString(kvp.Value.Value));
				}

				metadata = new ExtraMetaData(d);
			}
			_emitted.Add(new EmittedEventEnvelope(new EmittedDataEvent(stream, Guid.NewGuid(), eventType, true, data, metadata, _currentPosition, null)));
			return JsValue.Undefined;
		}

		private static ObjectInstance EnsureNonNullObjectValue(JsValue parameter, string parameterName) {
			if (parameter == JsValue.Null || parameter == JsValue.Undefined)
				throw new ArgumentNullException(parameterName);
			if (!parameter.IsObject())
				throw new ArgumentException("object expected", parameterName);
			return parameter.AsObject();
		}

		private static string EnsureNonNullStringValue(JsValue parameter, string parameterName) {
			if (parameter != JsValue.Null &&
				parameter.IsString() &&
				(parameter.AsString() is { } value &&
				 !string.IsNullOrWhiteSpace(value)))
				return value;

			if (parameter == JsValue.Null || parameter == JsValue.Undefined || parameter.IsString())
				throw new ArgumentNullException(parameterName);

			throw new ArgumentException("string expected", parameterName);
		}

		string? AsString(JsValue? value) {
			return value switch {
				JsBoolean b => b.AsBoolean() ? "true" : "false",
				JsString s => s.AsString(),
				JsNumber n => n.AsNumber().ToString(CultureInfo.InvariantCulture),
				JsNull => null,
				JsUndefined => null,
				JsValue v => _json.Stringify(JsValue.Undefined, new[] { v }).AsString(),
				_ => null
			};
		}

		JsValue LinkTo(JsValue thisValue, JsValue[] parameters) {
			if (parameters.Length != 2 && parameters.Length != 3)
				throw new ArgumentException("wrong number of parameters");
			var stream = EnsureNonNullStringValue(parameters.At(0), "streamId");
			var @event = EnsureNonNullObjectValue(parameters.At(1), "event");
			if (!@event.TryGetValue("sequenceNumber", out var numberValue) || !numberValue.IsNumber() ||
			    !@event.TryGetValue("streamId", out var sourceValue) || !sourceValue.IsString()) {
				//Log warning
				//if(warn on invalid events)
				//_log.Warning("Invalid emitted event was ignored");
				//else
				//throw new Exception("Invalid link to event {numberValue}@{sourceValue}");
				return JsValue.Undefined;
			}

			var number = (long)numberValue.AsNumber();
			var source = sourceValue.AsString();
			ExtraMetaData? metadata = null;
			if (parameters.Length == 3) {
				var md = parameters.At(4).AsObject();
				var d = new Dictionary<string, string?>();
				foreach (var kvp in md.GetOwnProperties()) {
					d.Add(kvp.Key.AsString(), AsString(kvp.Value.Value));
				}
				metadata = new ExtraMetaData(d);
			}
			
			_emitted.Add(new EmittedEventEnvelope(
				new EmittedDataEvent(stream, Guid.NewGuid(), SystemEventTypes.LinkTo, false, $"{number}@{source}", metadata, _currentPosition, null, null)));
			return JsValue.Undefined;
		}

		JsValue LinkStreamTo(JsValue thisValue, JsValue[] parameters) {

			var stream = EnsureNonNullStringValue(parameters.At(0), "streamId");
			var linkedStreamId = EnsureNonNullStringValue(parameters.At(1), "linkedStreamId");
			if (parameters.Length == 3) {

			}

			ExtraMetaData? metadata = null;
			if (parameters.Length == 3) {
				var md = parameters.At(4).AsObject();
				var d = new Dictionary<string, string?>();
				foreach (var kvp in md.GetOwnProperties()) {
					d.Add(kvp.Key.AsString(), AsString(kvp.Value.Value));
				}
				metadata = new ExtraMetaData(d);
			}
			_emitted.Add(new EmittedEventEnvelope(
				new EmittedDataEvent(stream, Guid.NewGuid(), SystemEventTypes.StreamReference, false, linkedStreamId, metadata, _currentPosition, null, null)));
			return JsValue.Undefined;
		}

		JsValue CopyTo(JsValue thisValue, JsValue[] parameters) {
			return JsValue.Undefined;
		}

		class TimeConstraint : IConstraint {
			private readonly TimeSpan _compilationTimeout;
			private readonly TimeSpan _executionTimeout;
			private TimeSpan _start;
			private TimeSpan _timeout;

			public TimeConstraint() {
				_timeout = _compilationTimeout;
			}

			public TimeConstraint(TimeSpan compilationTimeout, TimeSpan executionTimeout) {
				_compilationTimeout = compilationTimeout;
				_executionTimeout = executionTimeout;
			}

			public void Compiling() {
				_timeout = _compilationTimeout;
			}

			public void Executing() {

				_timeout = _executionTimeout;

			}
			public void Reset() {
				_start = _sw.Elapsed;
			}

			public void Check() {
				if (_sw.Elapsed - _start >= _timeout) {
					if (Debugger.IsAttached)
						return;
					throw new TimeoutException();
				}
			}
		}

		class Selector : ObjectInstance {

			private readonly Dictionary<string, ScriptFunctionInstance> _handlers;

			private readonly List<(TransformType,ScriptFunctionInstance)> _transforms;
			private readonly List<ScriptFunctionInstance> _createdHandlers;
			private ScriptFunctionInstance? _init;
			private ScriptFunctionInstance? _initShared;
			private ScriptFunctionInstance? _any;
			private ScriptFunctionInstance? _deleted;
			private ScriptFunctionInstance? _partitioner;

			private readonly JsValue _whenInstance;
			private readonly JsValue _partitionByInstance;
			private readonly JsValue _outputStateInstance;
			private readonly JsValue _foreachStreamInstance;
			private readonly JsValue _transformByInstance;
			private readonly JsValue _filterByInstance;
			private readonly JsValue _outputToInstance;
			private readonly JsValue _definesStateTransformInstance;

			private readonly SourceDefinitionBuilder _definitionBuilder;

			private static readonly IReadOnlyDictionary<string, Action<Selector>> _possibleProperties = new Dictionary<string, Action<Selector>>() {
				["when"] = i => i.FastAddProperty("when", i._whenInstance, true, false, true),
				["partitionBy"] = i => i.FastAddProperty("partitionBy", i._partitionByInstance, true, false, true),
				["outputState"] = i => i.FastAddProperty("outputState", i._outputStateInstance, true, false, true),
				["foreachStream"] = i => i.FastAddProperty("foreachStream", i._foreachStreamInstance, true, false, true),
				["transformBy"] = i => i.FastAddProperty("transformBy", i._transformByInstance, true, false, true),
				["filterBy"] = i => i.FastAddProperty("filterBy", i._filterByInstance, true, false, true),
				["outputTo"] = i => i.FastAddProperty("outputTo", i._outputToInstance, true, false, true),
				["$defines_state_transform"] = i => i.FastAddProperty("$defines_state_transform", i._definesStateTransformInstance, true, false, true),
			};

			private static readonly IReadOnlyDictionary<string, string[]> _availableProperties = new Dictionary<string, string[]>() {
				["fromStream"] = new[] { "when", "partitionBy", "outputState" },
				["fromAll"] = new[] { "when", "partitionBy", "outputState", "foreachStream" },
				["fromStreams"] = new[] { "when", "partitionBy", "outputState" },//qq  multiple streams but no foreachStream
				["fromCategory"] = new[] { "when", "partitionBy", "outputState", "foreachStream" },
				["when"] = new[] { "transformBy", "filterBy", "outputState", "outputTo", "$defines_state_transform" },
				["foreachStream"] = new[] { "when" },
				["outputState"] = new[] { "transformBy", "filterBy", "outputTo" },
				["partitionBy"] = new[] { "when" },
				["transformBy"] = new[] { "transformBy", "filterBy", "outputState", "outputTo" },
				["filterBy"] = new[] { "transformBy", "filterBy", "outputState", "outputTo" },
				["outputTo"] = Array.Empty<string>()
			};

			private static readonly IReadOnlyDictionary<string, Action<SourceDefinitionBuilder, JsValue>> _setters =
				new Dictionary<string, Action<SourceDefinitionBuilder, JsValue>>(StringComparer.OrdinalIgnoreCase) {
					{"$includeLinks", (options, value) => options.SetIncludeLinks(value.IsBoolean()? value.AsBoolean() : throw new Exception("Invalid value"))},
					{"reorderEvents", (options, value) => options.SetReorderEvents(value.IsBoolean()? value.AsBoolean(): throw new Exception("Invalid value"))},
					{"processingLag", (options, value) => options.SetProcessingLag(value.IsNumber() ? (int)value.AsNumber() : throw new Exception("Invalid value"))},
					{"resultStreamName", (options, value) => options.SetResultStreamNameOption(value.IsString() ? value.AsString() : throw new Exception("Invalid value"))},
					{"biState", (options, value) => options.SetIsBiState(value.IsBoolean()? value.AsBoolean() : throw new Exception("Invalid value"))},
				};



			public Selector(Engine engine, SourceDefinitionBuilder builder) : base(engine) {
				_definitionBuilder = builder;
				_handlers = new Dictionary<string, ScriptFunctionInstance>(StringComparer.OrdinalIgnoreCase);//TODO: is this correct?
				_createdHandlers = new List<ScriptFunctionInstance>();
				_transforms = new List<(TransformType, ScriptFunctionInstance)>();

				_whenInstance = new ClrFunctionInstance(engine, "when", When, 1);
				_partitionByInstance = new ClrFunctionInstance(engine, "partitionBy", PartitionBy, 1);
				_outputStateInstance = new ClrFunctionInstance(engine, "outputState", OutputState, 1);
				_foreachStreamInstance = new ClrFunctionInstance(engine, "foreachStream", ForEachStream, 1);
				_transformByInstance = new ClrFunctionInstance(engine, "transformBy", TransformBy, 1);
				_filterByInstance = new ClrFunctionInstance(engine, "filterBy", FilterBy, 1);
				_outputToInstance = new ClrFunctionInstance(engine, "outputTo", OutputTo, 1);
				_definesStateTransformInstance = new ClrFunctionInstance(engine, "$defines_state_transform", DefinesStateTransform);
				var ni = new ClrFunctionInstance(Engine, "notImplemented", NotImplemented);
				engine.Global.FastAddProperty("on_event", new ClrFunctionInstance(Engine, "on_event", OnEvent, 2), true, false, true);//TODO: verify
				engine.Global.FastAddProperty("on_any", new ClrFunctionInstance(Engine, "on_any", OnAny, 1), true, false, true);
				engine.Global.FastAddProperty("on_raw", ni, true, false, true);
			}

			public JsValue FromStream(JsValue _, JsValue[] parameters) {
				var stream = parameters.At(0);
				if (stream is not JsString)
					throw new ArgumentException("stream");
				_definitionBuilder.FromStream(stream.AsString());
				RestrictProperties("fromStream");

				return this;
			}

			public JsValue FromCategory(JsValue thisValue, JsValue[] parameters) {
				if (parameters.Length == 0)
					return this;
				if (parameters.Length == 1 && parameters.At(0).IsArray()) {
					foreach (var cat in parameters.At(0).AsArray()) {
						if (cat is not JsString s) {
							throw new ArgumentException("categories");
						}
						_definitionBuilder.FromStream($"$ce-{s.AsString()}");
					}
				} else if (parameters.Length > 1) {
					foreach (var cat in parameters) {
						if (cat is not JsString s) {
							throw new ArgumentException("categories");
						}
						_definitionBuilder.FromStream($"$ce-{s.AsString()}");
					}
				} else {
					var p0 = parameters.At(0);
					if (p0 is not JsString s)
						throw new ArgumentException("category");
					_definitionBuilder.FromCategory(s.AsString());
				}

				RestrictProperties("fromCategory");

				return this;
			}

			JsValue When(JsValue thisValue, JsValue[] parameters) {
				if (parameters.At(0) is ObjectInstance handlers) {
					foreach (var kvp in handlers.GetOwnProperties()) {
						if (kvp.Key.IsString() && kvp.Value.Value is ScriptFunctionInstance) {
							var key = kvp.Key.AsString();
							AddHandler(key, (ScriptFunctionInstance)kvp.Value.Value);
						}
					}
				}
				_definitionBuilder.SetDefinesFold();
				RestrictProperties("when");
				return this;
			}

			JsValue PartitionBy(JsValue thisValue, JsValue[] parameters) {
				if (parameters.At(0) is ScriptFunctionInstance partitioner) {
					_definitionBuilder.SetByCustomPartitions();


					_partitioner = partitioner;
					RestrictProperties("partitionBy");
					return this;
				}

				throw new ArgumentException("partitionBy");
			}

			JsValue ForEachStream(JsValue thisValue, JsValue[] parameters) {
				_definitionBuilder.SetByStream();
				RestrictProperties("foreachStream");
				return this;
			}

			JsValue OutputState(JsValue thisValue, JsValue[] parameters) {
				RestrictProperties("outputState");
				_definitionBuilder.SetOutputState();
				return this;
			}

			JsValue OutputTo(JsValue thisValue, JsValue[] parameters) {
				if(parameters.Length != 1 && parameters.Length != 2)
					throw new ArgumentException("invalid number of parameters");
				if (!parameters.At(0).IsString()) throw new ArgumentException("expected string value", "resultStream");
				if(parameters.Length == 2 && !parameters.At(1).IsString()) throw new ArgumentException("expected string value", "partitionResultStreamPattern");
				_definitionBuilder.SetResultStreamNameOption(parameters.At(0).AsString());
				if(parameters.Length == 2)
					_definitionBuilder.SetPartitionResultStreamNamePatternOption(parameters.At(1).AsString());
				RestrictProperties("outputTo");
				return this;
			}

			JsValue DefinesStateTransform(JsValue thisValue, JsValue[] parameters) {
				_definitionBuilder.SetDefinesStateTransform();
				return Undefined;
			}

			JsValue FilterBy(JsValue thisValue, JsValue[] parameters) {
				if (parameters.At(0) is ScriptFunctionInstance fi) {
					_definitionBuilder.SetDefinesStateTransform();
					_transforms.Add((TransformType.Filter, fi));
					RestrictProperties("filterBy");
					return this;
				}

				throw new ArgumentException("expected function");
			}

			JsValue TransformBy(JsValue thisValue, JsValue[] parameters) {
				if (parameters.At(0) is ScriptFunctionInstance fi) {
					_definitionBuilder.SetDefinesStateTransform();
					_transforms.Add((TransformType.Transform, fi));
					RestrictProperties("transformBy");
					return this;
				}

				throw new ArgumentException("expected function");
			}

			JsValue OnEvent(JsValue thisValue, JsValue[] parameters) {
				if (parameters.Length != 2)
					throw new ArgumentException("invalid number of parameters");
				var eventName = parameters.At(0);
				var handler = parameters.At(1);
				if (!eventName.IsString())
					throw new ArgumentException("eventName");
				if (handler is not ScriptFunctionInstance fi)
					throw new ArgumentException("eventHandler");
				AddHandler(eventName.AsString(), fi);
				return Undefined;
			}

			JsValue OnAny(JsValue thisValue, JsValue[] parameters) {
				if (parameters.Length != 1)
					throw new ArgumentException("invalid number of parameters");
				if (parameters.At(0) is not ScriptFunctionInstance fi)
					throw new ArgumentException("eventHandler");
				AddHandler("$any", fi);
				return Undefined;
			}

			JsValue NotImplemented(JsValue thisValue, JsValue[] parameters) {
				throw new NotImplementedException();
			}

			void AddHandler(string name, ScriptFunctionInstance handler) {
				switch (name) {
					case "$init":
						_init = handler;
						_definitionBuilder.SetDefinesStateTransform();
						break;
					case "$initShared":
						_definitionBuilder.SetIsBiState(true);
						_initShared = handler;
						break;
					case "$any":
						_any = handler;
						_definitionBuilder.AllEvents();
						break;
					case "$created":
						_createdHandlers.Add(handler);
						break;
					case "$deleted" when !_definitionBuilder.IsBiState:
						_definitionBuilder.SetHandlesStreamDeletedNotifications();
						_deleted = handler;
						break;
					case "$deleted" when _definitionBuilder.IsBiState:
						throw new Exception("Cannot handle deletes");
					default:
						_definitionBuilder.NotAllEvents();
						_definitionBuilder.IncludeEvent(name);
						_handlers.Add(name, handler);
						break;
				}
			}

			private void RestrictProperties(string state) {
				var allowed = _availableProperties[state];
				var current = GetOwnPropertyKeys();
				foreach (var p in current) {
					if (!allowed.Contains(p.AsString())) {
						RemoveOwnProperty(p);
					}
				}

				foreach (var p in allowed) {
					if (!HasOwnProperty(p)) {
						_possibleProperties[p](this);
					}
				}
			}

			public JsValue InitializeState() {
				if (_init == null)
					return new ObjectInstance(Engine);
				return _init.Invoke();
			}

			public JsValue InitializeSharedState() {
				if (_initShared == null)
					return new ObjectInstance(Engine);
				return _initShared.Invoke();
			}

			public JsValue Handle(JsValue state, EventEnvelope eventEnvelope) {
				JsValue newState;
				if (_handlers.TryGetValue(eventEnvelope.EventType, out var handler)) {
					newState = handler.Invoke(state, JsValue.FromObject(Engine, eventEnvelope));
				} else if (_any != null) {
					newState = _any.Invoke(state, JsValue.FromObject(Engine, eventEnvelope));
				} else {
					newState = eventEnvelope.IsJson ? eventEnvelope.Body : eventEnvelope.BodyRaw;
				}

				return newState == Undefined ? state : newState;
			}

			public JsValue TransformStateToResult(JsValue state) {
				foreach (var (type, transform) in _transforms) {
					if(type == TransformType.Transform)
						state = transform.Invoke(state);
					else {
						var result = transform.Invoke(state);
						if (!(result.IsBoolean() && result.AsBoolean()) || result == Null || result == Undefined) {
							return Null;
						}
					}

					if (state == Null || state == Undefined) return Null;
				}

				return state;
			}

			public JsValue FromAll(JsValue _, JsValue[] __) {
				_definitionBuilder.FromAll();
				RestrictProperties("fromAll");
				return this;
			}

			public JsValue FromStreams(JsValue _, JsValue[] parameters) {
				IEnumerator<JsValue>? streams = null;
				try {
					streams = parameters.At(0).IsArray() ? parameters.At(0).AsArray().GetEnumerator() : parameters.AsEnumerable().GetEnumerator();
					while (streams.MoveNext()) {
						if (!streams.Current.IsString())
							throw new ArgumentException("streams");
						_definitionBuilder.FromStream(streams.Current.AsString());
					}
				} finally {
					streams?.Dispose();
				}

				RestrictProperties("fromStreams");
				return this;
			}


			public JsValue SetOptions(JsValue thisValue, JsValue[] parameters) {
				var p0 = parameters.At(0);
				if (p0 is ObjectInstance opts) {
					foreach (var kvp in opts.GetOwnProperties()) {
						if (_setters.TryGetValue(kvp.Key.AsString(), out var setter)) {
							setter(_definitionBuilder, kvp.Value.Value);
						} else {
							throw new Exception($"Unrecognized option: {kvp.Key}");
						}
					}
				}

				return JsValue.Undefined;
			}

			public JsValue GetPartition(EventEnvelope envelope) {
				if (_partitioner != null)
					return _partitioner.Invoke(envelope);
				return Null;
			}

			public void HandleCreated(JsValue state, EventEnvelope envelope) {
				for (int i = 0; i < _createdHandlers.Count; i++) {
					_createdHandlers[i].Call(JsValue.Undefined, new JsValue[] { state, envelope });
				}
			}

			enum TransformType {
				None,
				Filter,
				Transform
			}
		}

		class EventEnvelope : ObjectInstance {
			private readonly JsonInstance _json;

			public string StreamId {
				get => Get("streamId").AsString();
				set => FastSetProperty("streamId", new PropertyDescriptor(value, true, false, true));
			}
			public long SequenceNumber {
				get => (long)Get("sequenceNumber").AsNumber();
				set => FastSetProperty("sequenceNumber", new PropertyDescriptor(value, true, false, true));
			}

			public string EventType {
				get => Get("eventType").AsString();
				set => FastSetProperty("eventType", new PropertyDescriptor(value, true, false, true));
			}

			public JsValue Body {
				get {
					if (TryGetValue("body", out var value) && value is ObjectInstance oi)
						return oi;
					if (IsJson && TryGetValue("bodyRaw", out var raw)) {
						var args = new JsValue[1];
						args[0] = raw;
						var body = _json.Parse(JsValue.Undefined, args);
						FastSetProperty("body", PropertyDescriptor.ToPropertyDescriptor(Engine, body));
						return (ObjectInstance)body;
					}

					return Undefined;
				}
			}

			public bool IsJson {
				get => Get("isJson").AsBoolean();
				set => FastSetProperty("isJson", new PropertyDescriptor(value, true, false, true));
			}

			public string BodyRaw {
				get => Get("bodyRaw").AsString();
				set => FastSetProperty("bodyRaw", new PropertyDescriptor(value, true, false, true));
			}

			private JsValue Metadata {
				get {
					if (TryGetValue("metadata", out var value) && value is ObjectInstance oi)
						return oi;
					if (TryGetValue("metadataRaw", out var raw)) {
						var args = new JsValue[1];
						args[0] = raw;
						var metadata = _json.Parse(JsValue.Undefined, args);
						FastSetProperty("metadata", PropertyDescriptor.ToPropertyDescriptor(Engine, metadata));
						return (ObjectInstance)metadata;
					}

					return Undefined;
				}
			}

			public string MetadataRaw {
				get => Get("metadataRaw").AsString();
				set => FastSetProperty("metadataRaw", new PropertyDescriptor(value, true, false, true));
			}

			private JsValue LinkMetadata {
				get {
					if (TryGetValue("linkMetadata", out var value) && value is ObjectInstance oi)
						return oi;
					if (TryGetValue("linkMetadataRaw", out var raw)) {
						var args = new JsValue[1];
						args[0] = raw;
						var metadata = _json.Parse(JsValue.Undefined, args);
						FastSetProperty("linkMetadata", PropertyDescriptor.ToPropertyDescriptor(Engine, metadata));
						return (ObjectInstance)metadata;
					}

					return Undefined;
				}
			}

			public string LinkMetadataRaw {
				get => Get("linkMetadataRaw").AsString();
				set => FastSetProperty("linkMetadataRaw", new PropertyDescriptor(value, true, false, true));
			}

			public string Partition {
				get => Get("partition").AsString();
				set => FastSetProperty("partition", new PropertyDescriptor(value, true, false, true));
			}

			public EventEnvelope(Engine engine, JsonInstance json) : base(engine) {
				_json = json;
			}

			public override JsValue Get(JsValue property, JsValue receiver) {
				if (property == "body" || property == "data") {
					return Body;
				}

				if (property == "metadata") {
					return Metadata;
				}

				if (property == "linkMetadata") {
					return LinkMetadata;
				}
				return base.Get(property, receiver);
			}
		}
	}

}
