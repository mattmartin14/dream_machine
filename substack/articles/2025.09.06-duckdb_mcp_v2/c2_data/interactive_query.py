#!/usr/bin/env python3

import asyncio
from natural_language_query import WorkoutQueryAssistant

async def interactive_query():
    """
    Interactive function where you can ask natural language questions
    about your workout data.
    """
    assistant = WorkoutQueryAssistant()
    
    print("🏋️  Workout Query Assistant")
    print("Ask me questions about your Concept2 workouts!")
    print("Examples:")
    print("- What was my longest workout?")
    print("- What's my best 6k time?") 
    print("- How many RowErg workouts have I done?")
    print("- Show me my recent workouts")
    print("- What's my average distance by equipment type?")
    print("\nType 'quit' to exit\n")
    
    while True:
        try:
            question = input("❓ Your question: ").strip()
            
            if question.lower() in ['quit', 'exit', 'q']:
                print("👋 Goodbye!")
                break
                
            if not question:
                continue
                
            print(f"\n🤔 Processing: {question}")
            result = await assistant.ask_question(question)
            print("📊 Result:")
            print(result)
            print("\n" + "="*60 + "\n")
            
        except KeyboardInterrupt:
            print("\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"❌ Error: {e}")

# Function you can call directly with a question
async def ask_workout_question(question: str) -> str:
    """
    Simple function to ask a single question and get the result.
    
    Args:
        question (str): Natural language question about workouts
        
    Returns:
        str: Formatted result from the database
    """
    assistant = WorkoutQueryAssistant()
    return await assistant.ask_question(question)

if __name__ == "__main__":
    asyncio.run(interactive_query())
