import express from 'express';
import { BigQuery } from '@google-cloud/bigquery';

const app = express();
const port = 3001;

app.use(express.json()); // Enable JSON body parsing

const bigquery = new BigQuery();

app.post('/run-query', async (req, res) => {
  const { query } = req.body;

  if (!query) {
    return res.status(400).send('Query is required.');
  }

  try {
    const [job] = await bigquery.createQueryJob({ query: query });
    const [rows] = await job.getQueryResults();
    res.json(rows);
  } catch (error: any) {
    console.error('BigQuery error:', error);
    if (error.message && error.message.includes('Syntax error')) {
      res.status(400).json({
        error: error.message,
        funnyMessage: "Do you even lift bro?"
      });
    } else {
      res.status(500).json({ error: error.message || 'An unknown error occurred.' });
    }
  }
});

app.get('/', (req, res) => {
  res.send('Hello from BroQuery Backend!');
});

app.listen(port, () => {
  console.log(`BroQuery backend listening at http://localhost:${port}`);
});