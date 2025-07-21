import React, { useState } from 'react';
import './App.css';

interface QueryResult {
  [key: string]: any;
}

function App() {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<QueryResult[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [funnyMessage, setFunnyMessage] = useState<string | null>(null);

  const handleRunQuery = async () => {
    setResults(null);
    setError(null);
    setFunnyMessage(null);

    try {
      const response = await fetch('http://localhost:3001/run-query', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ query }),
      });

      const data = await response.json();

      if (response.ok) {
        setResults(data);
      } else {
        setError(data.error || 'An unknown error occurred.');
        setFunnyMessage(data.funnyMessage || null);
      }
    } catch (err: any) {
      setError(err.message || 'Failed to connect to the backend.');
    }
  };

  return (
    <div className="App">
      <header className="App-header">
        <h1>BroQuery</h1>
      </header>
      <div className="query-section">
        <textarea
          placeholder="Enter your BigQuery SQL here..."
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          rows={10}
          cols={80}
        ></textarea>
        <button onClick={handleRunQuery}>Run Query</button>
      </div>

      {error && (
        <div className="error-section">
          <p className="error-message">Error: {error}</p>
          {funnyMessage && <p className="funny-message">{funnyMessage}</p>}
        </div>
      )}

      {results && results.length > 0 && (
        <div className="results-section">
          <h2>Query Results</h2>
          <table>
            <thead>
              <tr>
                {Object.keys(results[0]).map((key) => (
                  <th key={key}>{key}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {results.map((row, rowIndex) => (
                <tr key={rowIndex}>
                  {Object.values(row).map((value, colIndex) => (
                    <td key={colIndex}>{String(value)}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {results && results.length === 0 && !error && (
        <div className="results-section">
          <p>No results found.</p>
        </div>
      )}
    </div>
  );
}

export default App;