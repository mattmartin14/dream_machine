# BroQuery

BroQuery is a simple application that allows you to run Google BigQuery queries and display the results in a grid format.

## Project Structure

This project is a monorepo with two main parts:

- `client`: The React frontend application.
- `server`: The Node.js Express backend API.

## Getting Started

### 1. Clone the repository

```bash
git clone <repository_url>
cd broquery_test
```

### 2. Backend Setup

Navigate to the `server` directory and install the dependencies:

```bash
cd server
npm install
```

To compile the TypeScript code and start the backend server, run:

```bash
npm run build
npm start
```

### 3. Frontend Setup

Navigate to the `client` directory and install the dependencies:

```bash
cd ../client
npm install
```

To start the frontend development server, run:

```bash
npm start
```

This will open the application in your browser at `http://localhost:3000`.

### 4. Google Cloud BigQuery Authentication

To allow the backend to access BigQuery, you need to set up Google Cloud authentication. The recommended way for local development is to use Application Default Credentials (ADC).

1.  **Install the Google Cloud CLI:** Follow the instructions [here](https://cloud.google.com/sdk/docs/install) to install the `gcloud` CLI.

2.  **Authenticate:** Run the following command in your terminal:

    ```bash
    gcloud auth application-default login
    ```

    This will open a browser window for you to log in with your Google account. Once authenticated, your application will be able to use your credentials to access Google Cloud services, including BigQuery.

3.  **Set your Google Cloud Project ID:** You might need to set the `GOOGLE_CLOUD_PROJECT` environment variable in your `server` directory to your Google Cloud Project ID. Create a `.env` file in the `server` directory with the following content:

    ```
    GOOGLE_CLOUD_PROJECT=your-project-id
    ```

    Replace `your-project-id` with your actual Google Cloud Project ID.

## Features

- Run BigQuery SQL queries.
- Display query results in a grid.
- Humorous error messages for syntax errors.

## Contributing

Feel free to contribute to BroQuery by submitting issues or pull requests.
