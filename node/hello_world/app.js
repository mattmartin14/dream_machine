const express = require('express');
const app = express();
const port = 3000;

// Define a route to serve the HTML page
app.get('/', (req, res) => {
  res.sendFile(__dirname + '/index.html');
});

app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});
