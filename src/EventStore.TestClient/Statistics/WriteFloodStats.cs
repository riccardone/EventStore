using System;
using System.Collections.Generic;
using System.Globalization;
using EventStore.Core.Services.Monitoring.Stats;
using Serilog;

namespace EventStore.TestClient.Statistics {
	/// <summary>
	/// Statistics for WriteFlood
	/// </summary>
	public class WriteFloodStats {
		/// <summary>
		/// The number of successful writes
		/// </summary>
		public long Succ = 0;
		/// <summary>
		/// The number of failing writes
		/// </summary>
		public long Fail = 0;
		/// <summary>
		/// The number of writes that failed with PrepareTimeout
		/// </summary>
		public long PrepTimeout = 0;
		/// <summary>
		/// The number of writes that failed with CommitTimeout
		/// </summary>
		public long CommitTimeout = 0;
		/// <summary>
		/// The number of writes that failed with ForwardTimeout
		/// </summary>
		public long ForwardTimeout = 0;
		/// <summary>
		/// The number of writes that failed with WrongExpectedVersion
		/// </summary>
		public long WrongExpVersion = 0;
		/// <summary>
		/// The number of writes that failed with StreamDeleted
		/// </summary>
		public long StreamDeleted = 0;
		/// <summary>
		/// The total number of completed writes
		/// </summary>
		public long All = 0;
		/// <summary>
		/// The amount of time that has elapsed since the last measurement
		/// </summary>
		public TimeSpan Elapsed = TimeSpan.Zero;
		/// <summary>
		/// The time in UTC that the command was started
		/// </summary>
		public DateTime StartTime = DateTime.UtcNow;
		/// <summary>
		/// The command that was run
		/// </summary>
		public string Command;
		/// <summary>
		/// The current write rate
		/// </summary>
		public double Rate;
		/// <summary>
		/// The command and arguments used to run this command
		/// </summary>
		public string CommandString;

		/// <summary>
		/// </summary>
		/// <param name="command"></param>
		/// <param name="args"></param>
		public WriteFloodStats(string command, string[] args) {
			Command = command;
			CommandString = $"{Command} {string.Join(" ", args)}";
		}

		private Dictionary<string, object> GetStats() {
			var stats = new Dictionary<string, object>();
			stats[$"{Command}-starttime"] = StartTime.ToString("O", CultureInfo.InvariantCulture);
			stats[$"{Command}-succ"] = Succ;
			stats[$"{Command}-fail"] = Fail;
			stats[$"{Command}-prepTimeout"] = PrepTimeout;
			stats[$"{Command}-commitTimeout"] = CommitTimeout;
			stats[$"{Command}-forwardTimeout"] = ForwardTimeout;
			stats[$"{Command}-wrongExpVersion"] = WrongExpVersion;
			stats[$"{Command}-streamDeleted"] = StreamDeleted;
			stats[$"{Command}-all"] = All;
			stats[$"{Command}-elapsed"] = Elapsed;
			stats[$"{Command}-rate"] = Rate;
			stats[$"{Command}-command"] = CommandString;

			return stats;
		}

		/// <summary>
		/// Write the current round of stats to the log
		/// </summary>
		/// <param name="log"></param>
		public void WriteStatsToFile(ILogger log) {
			try {
				var statsContainer = new StatsContainer();
				statsContainer.Add(GetStats());
				var rawStats = statsContainer.GetStats(useGrouping: false, useMetadata: false);

				rawStats.Add("timestamp", DateTime.UtcNow.ToString("O", CultureInfo.InvariantCulture));
				log.Information("{@stats}", rawStats);

			} catch (Exception ex) {
				Log.Error(ex, "Error on regular stats collection.");
			}
		}
	}
}
