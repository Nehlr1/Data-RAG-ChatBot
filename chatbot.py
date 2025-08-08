import os
import uuid
import json
import psycopg2
from datetime import datetime, timezone
from typing import TypedDict, Annotated
from dotenv import load_dotenv
from database import DatabaseManager
from langchain.chat_models import init_chat_model
from langchain.memory import ConversationBufferMemory
from langgraph.graph.message import add_messages
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

load_dotenv()

# --- PostgreSQL Configuration ---
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"), 
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}

USER_ID = "abc"

# 🎯 Generate a unique conversation ID
conversation_id = str(uuid.uuid4())
created_at = datetime.now(timezone.utc)

# 🧠 Use MemorySaver to track conversation memory
memory_saver = MemorySaver()
memory = ConversationBufferMemory(return_messages=True)
llm = init_chat_model("google_genai:gemini-2.0-flash")

# --- Define State Schema ---
class ChatState(TypedDict):
    user_input: str
    messages: Annotated[list, add_messages]

# --- LangGraph Nodes ---
def user_node(state: ChatState) -> ChatState:
    user_input = state['user_input']
    memory.chat_memory.add_user_message(user_input)
    return {"messages": memory.chat_memory.messages}

def assistant_node(state: ChatState) -> ChatState:
    response = llm.invoke(memory.chat_memory.messages)
    memory.chat_memory.add_ai_message(response.content)
    print(f"🤖 Assistant: {response.content}\n")
    return {"messages": memory.chat_memory.messages}

# --- Build the LangGraph ---
builder = StateGraph(ChatState)
builder.add_node("user", user_node)
builder.add_node("assistant", assistant_node)
builder.set_entry_point("user")
builder.add_edge("user", "assistant")
builder.add_edge("assistant", END)
graph = builder.compile(checkpointer=memory_saver)

# Initialize database manager and ensure database/table exist
db_manager = DatabaseManager(DB_CONFIG)
db_manager.create_database_if_not_exists()
db_manager.create_chat_table()

print("💬 Chat started. Type 'exit' to end the conversation.\n")

# --- Chat Loop ---
while True:
    user_input = input("👤 You: ")
    if user_input.strip().lower() in ["exit", "quit"]:
        print("👋 Ending chat. Goodbye!")
        break

    # Run the LangGraph with input
    config = {"configurable": {"thread_id": conversation_id}}
    state = graph.invoke({"user_input": user_input, "messages": []}, config)

    # Format messages for PostgreSQL
    formatted_messages = [
        {"role": msg.type, "content": msg.content}
        for msg in memory.chat_memory.messages
    ]

    # Insert/update into PostgreSQL
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO chat_history (conversation_id, user_id, created_at, messages)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (conversation_id) DO UPDATE
            SET messages = EXCLUDED.messages
        """, (conversation_id, USER_ID, created_at, json.dumps(formatted_messages)))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print("❌ Error saving to database:", e)