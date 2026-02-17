const fs = require('fs');
const path = require('path');
const { DuckDBDriver } = require('@cubejs-backend/duckdb-driver');

// Log file for pretty-printed JSON objects (one after another)
const LOG_PATH = process.env.CUBE_QUERY_LOG_PATH || path.join('/cube/conf', 'query_logs.json');

// Toggle this flag to also include the raw SQL text in each log entry.
// Set to true if you want to capture the SQL again.
const CAPTURE_SQL = false;

function appendJsonLog(entry) {
  try {
    const enriched = {
      timestamp: new Date().toISOString(),
      ...entry,
    };
    const pretty = JSON.stringify(enriched, null, 2);
    const line = `${pretty}\n`;
    fs.appendFile(LOG_PATH, line, (err) => {
      if (err) {
        // Don't break queries if logging fails; just report.
        // eslint-disable-next-line no-console
        console.error('Failed to write Cube query log:', err);
      }
    });
  } catch (e) {
    // eslint-disable-next-line no-console
    console.error('Error during Cube query logging:', e);
  }
}

// We capture the most recent semantic query so we can attach it
// to the subsequent SQL execution timing record.
let lastCubeQuery = null;

class LoggingDuckDBDriver extends DuckDBDriver {
  async query(query, values, options) {
    const text = (query || '').trim();
    const upper = text.toUpperCase();
    const isTestQuery = upper === 'SELECT 1' || upper === 'SELECT 1;';
    const start = process.hrtime.bigint();
    try {
      const result = await super.query(query, values, options);
      const end = process.hrtime.bigint();
      const durationMs = Number(end - start) / 1e6;

      // Skip logging trivial internal health-check queries like SELECT 1
      if (!isTestQuery) {
        const entry = {
          type: 'CUBE_QUERY_EXECUTION',
          duration_ms: durationMs,
          cube_query: lastCubeQuery,
        };
        if (CAPTURE_SQL) {
          entry.sql = query;
        }
        appendJsonLog(entry);
      }

      return result;
    } catch (err) {
      const end = process.hrtime.bigint();
      const durationMs = Number(end - start) / 1e6;

      if (!isTestQuery) {
        const entry = {
          type: 'CUBE_QUERY_ERROR',
          duration_ms: durationMs,
          cube_query: lastCubeQuery,
          error: err && err.message ? err.message : String(err),
        };
        if (CAPTURE_SQL) {
          entry.sql = query;
        }
        appendJsonLog(entry);
      }
      throw err;
    } finally {
      // Only clear after logging the real query; keep semantic query
      // in place if this was just an internal test like SELECT 1.
      if (!isTestQuery) {
        lastCubeQuery = null;
      }
    }
  }
}

module.exports = {
  driverFactory: () => new LoggingDuckDBDriver(),

  // Log the Cube REST API JSON (semantic query) before it is translated to SQL
  queryRewrite: (query, { securityContext }) => {
    try {
      // Stash the latest semantic query so the next
      // executed SQL can include it in its log entry.
      lastCubeQuery = query;
    } catch (e) {
      // eslint-disable-next-line no-console
      console.error('Failed to log Cube REST query JSON:', e);
    }
    return query;
  },
};
