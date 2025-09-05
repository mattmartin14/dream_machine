# Workout Assistant

A natural language query tool for your DuckDB workout database.

## Usage

```bash
python workout_assistant.py "Your question about workouts"
```

## Example Questions

- "What was my longest workout?"
- "What's my fastest pace for 5k+ workouts?"
- "How many workouts did I do in 2025?"
- "What's my average distance by equipment type?"
- "Show me my recent workouts"

## Features

- **AI-Powered**: Attempts to use OpenAI or Anthropic APIs if available
- **Fallback System**: Uses keyword matching when AI APIs aren't available
- **Smart Formatting**: Displays results in a readable format
- **Error Handling**: Graceful handling of database and API errors

## Optional AI Setup

To enable AI-powered SQL generation, install one of these:

```bash
pip install openai  # For OpenAI GPT
pip install anthropic  # For Anthropic Claude
```

Then set your API key:
```bash
export OPENAI_API_KEY="your-key-here"
# or
export ANTHROPIC_API_KEY="your-key-here"
```

The tool works perfectly fine without AI APIs using the built-in fallback system.
