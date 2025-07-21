"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const express_1 = __importDefault(require("express"));
const bigquery_1 = require("@google-cloud/bigquery");
const app = (0, express_1.default)();
const port = 3001;
app.use(express_1.default.json()); // Enable JSON body parsing
const bigquery = new bigquery_1.BigQuery();
app.post('/run-query', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    const { query } = req.body;
    if (!query) {
        return res.status(400).send('Query is required.');
    }
    try {
        const [job] = yield bigquery.createQueryJob({ query: query });
        const [rows] = yield job.getQueryResults();
        res.json(rows);
    }
    catch (error) {
        console.error('BigQuery error:', error);
        if (error.message && error.message.includes('Syntax error')) {
            res.status(400).json({
                error: error.message,
                funnyMessage: "Do you even lift bro?"
            });
        }
        else {
            res.status(500).json({ error: error.message || 'An unknown error occurred.' });
        }
    }
}));
app.get('/', (req, res) => {
    res.send('Hello from BroQuery Backend!');
});
app.listen(port, () => {
    console.log(`BroQuery backend listening at http://localhost:${port}`);
});
