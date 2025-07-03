import streamlit as st
import os
import uuid
from dotenv import load_dotenv
from langchain_litellm import ChatLiteLLM
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from langgraph.graph import StateGraph, END, START
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph.message import MessagesState
from pinecone import Pinecone
from langchain_community.embeddings import OpenAIEmbeddings
from pdf_ingest import pdf_ingestion_component
from audio_recorder_streamlit import audio_recorder

import openai
import time
import streamlit.components.v1 as components
import streamlit as st

# Load environment variables
load_dotenv()

# Constants
DEFAULT_MODEL = "gpt-3.5-turbo"
PINECONE_INDEX_NAME = "weather-capstone-project"
PINECONE_NAMESPACE = "weather-data"
TOP_K_RESULTS = 2

AVAILABLE_MODELS = {
    "OpenAI: GPT-3.5-turbo": "gpt-3.5-turbo",
    "OpenAI: GPT-4": "gpt-4"
}

SAMPLE_PROMPTS = [
    "Tell me about FoundryAI's training programs",
    "What courses do you offer?"
]

# -------------------------------
# 🔹 Pinecone Helper Functions
# -------------------------------
def initialize_pinecone(api_key: str):
    pc = Pinecone(api_key=api_key)
    return pc.Index(PINECONE_INDEX_NAME)

def search_similar_chunks(index, query: str):
    # Use OpenAI embeddings to convert query to vector
    embedder = OpenAIEmbeddings()
    query_vector = embedder.embed_query(query)

    results = index.query(
        namespace=PINECONE_NAMESPACE,
        vector=query_vector,
        top_k=TOP_K_RESULTS,
        include_metadata=True
    )
    return results['matches']

# -------------------------------
# 🔹 RAG Chatbot Creator
# -------------------------------
def create_rag_chatbot(model_name: str = DEFAULT_MODEL):
    pinecone_api_key = os.getenv("PINECONE_API_KEY")
    if not pinecone_api_key:
        raise ValueError("Please set your PINECONE_API_KEY in environment variables.")

    index = initialize_pinecone(pinecone_api_key)
    llm = ChatLiteLLM(model=model_name, temperature=0.7)

    workflow = StateGraph(state_schema=MessagesState)

    def rag_node(state: MessagesState):
        last_message = state["messages"][-1]
        user_query = last_message.content

        # Embed and search
        similar_chunks = search_similar_chunks(index, user_query)

        if similar_chunks:  # Case 1: PDF context found
            context = "\n".join([chunk['metadata']['chunk_text'] for chunk in similar_chunks])
            system_prompt = f"""You are a helpful assistant. Use the context below to answer the user's question, but you can also use your general knowledge to provide a comprehensive response.

Context:
{context}

Please provide a helpful answer using both the context and your general knowledge when appropriate.
"""
        else:  # Case 2: No relevant context — allow OpenAI to answer freely
            system_prompt = "You are a helpful assistant. Answer the user's question as best as you can based on your general knowledge."

        system_message = SystemMessage(content=system_prompt)
        messages = [system_message] + state["messages"]

        response = llm.invoke(messages)
        return {"messages": response}

    workflow.add_node("rag", rag_node)
    workflow.add_edge(START, "rag")
    workflow.add_edge("rag", END)

    memory = MemorySaver()
    return workflow.compile(checkpointer=memory)

# -------------------------------
# 🔹 Session Management
# -------------------------------
def init_session_state():
    if "conversations" not in st.session_state:
        st.session_state.conversations = {}
    if "current_conversation_id" not in st.session_state:
        st.session_state.current_conversation_id = str(uuid.uuid4())

    if st.session_state.current_conversation_id not in st.session_state.conversations:
        st.session_state.conversations[st.session_state.current_conversation_id] = []

    if "model_name" not in st.session_state:
        st.session_state.model_name = DEFAULT_MODEL
    if "chatbot" not in st.session_state:
        st.session_state.chatbot = create_rag_chatbot(st.session_state.model_name)

    if "uploaded_files" not in st.session_state:
        st.session_state.uploaded_files = {}

def create_new_conversation():
    conversation_id = str(uuid.uuid4())
    st.session_state.conversations[conversation_id] = []
    st.session_state.current_conversation_id = conversation_id
    st.session_state.chatbot = create_rag_chatbot(st.session_state.model_name)
    # st.session_state.audio = None
    # st.session_state.voice_input = None
    st.rerun()

def switch_conversation(conversation_id):
    st.session_state.current_conversation_id = conversation_id

# -------------------------------
# 🔹 Sidebar Configuration
# -------------------------------
def config_sidebar():
    st.sidebar.title("Chat Configuration")

    selected_model = st.sidebar.selectbox(
        "Select Model",
        options=list(AVAILABLE_MODELS.keys()),
        index=list(AVAILABLE_MODELS.values()).index(st.session_state.model_name)
    )

    if AVAILABLE_MODELS[selected_model] != st.session_state.model_name:
        st.session_state.model_name = AVAILABLE_MODELS[selected_model]
        st.session_state.chatbot = create_rag_chatbot(st.session_state.model_name)
        st.session_state.audio = None
        st.session_state.voice_input = None

    st.sidebar.title("Conversations")
    if st.sidebar.button("🆕 New Conversation"):
        create_new_conversation()

    conversation_options = []
    for conv_id, messages in st.session_state.conversations.items():
        has_uploads = conv_id in st.session_state.uploaded_files
        has_messages = len(messages) > 0
        # ✅ Always include current conversation
        if has_uploads or has_messages or conv_id == st.session_state.current_conversation_id:
            label = f"{conv_id[:8]}"
            if has_uploads:
                label += " 📎"
            conversation_options.append((label, conv_id))

    if conversation_options:
        labels, ids = zip(*conversation_options)
        selected_label = st.sidebar.radio(
            "Select Conversation",
            options=labels,
            index=ids.index(st.session_state.current_conversation_id) if st.session_state.current_conversation_id in ids else 0
        )

        # ✅ Map label back to ID
        selected_conv_id = ids[labels.index(selected_label)]
        if selected_conv_id != st.session_state.current_conversation_id:
            switch_conversation(selected_conv_id)
            st.rerun()


    if st.sidebar.checkbox("Show Debug Info"):
        st.sidebar.json(st.session_state)

# -------------------------------
# 🔹 Sample Prompts
# -------------------------------
def display_sample_prompts():
    st.markdown("### 💡 Sample Prompts")
    cols = st.columns(2)
    for i, prompt in enumerate(SAMPLE_PROMPTS):
        with cols[i % 2]:
            if st.button(prompt, key=f"sample_{i}"):
                return prompt
    return None

def transcribe_audio(audio_bytes):
    client = openai.OpenAI()  # Assumes OPENAI_API_KEY is set
    with open("temp_audio.wav", "wb") as f:
        f.write(audio_bytes)
    with open("temp_audio.wav", "rb") as f:
        transcript = client.audio.transcriptions.create(
            model="whisper-1",
            file=f
        )
    return transcript.text


# -------------------------------
# 🔹 Streamlit Main
# -------------------------------
def main():

    if not os.getenv('OPENAI_API_KEY'):
        st.error("[red]Error: Please set your OPENAI_API_KEY environment variable[/red]")
        return

    st.set_page_config(page_title="FoundryAI RAG Chatbot", page_icon="🧠", layout="wide")
    st.title("🧠 Weather AI ChatBot")

    init_session_state()
    config_sidebar()

    with st.expander("📄 Upload PDF to Pinecone"):
        pdf_ingestion_component()
        conv_id = st.session_state.current_conversation_id
        uploaded_list = st.session_state.uploaded_files.get(conv_id, [])
        if uploaded_list:
            st.markdown("### 📂 Uploaded Files in this Conversation:")
            for f in uploaded_list:
                st.markdown(f"- `{f}`")

    for message in st.session_state.conversations.get(st.session_state.current_conversation_id, []):
        if message["role"] != "system":
            with st.chat_message(message["role"]):
                st.markdown(message["content"])

    selected_prompt = None

    # Use either text input or voice input
    prompt = st.chat_input("Ask something about FoundryAI...") or st.session_state.get("voice_input") or selected_prompt

    if prompt:
        if st.session_state.current_conversation_id not in st.session_state.conversations:
            st.session_state.conversations[st.session_state.current_conversation_id] = []

        st.session_state.conversations[st.session_state.current_conversation_id].append({
            "role": "user",
            "content": prompt
        })

        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            message_placeholder = st.empty()
            with st.spinner("Retrieving context and generating answer..."):
                try:
                    input_messages = [HumanMessage(content=prompt)]

                    result = st.session_state.chatbot.invoke(
                        {"messages": input_messages},
                        config={"configurable": {"thread_id": st.session_state.current_conversation_id}}
                    )

                    if result and "messages" in result and result["messages"]:
                        response = result["messages"][-1].content
                        message_placeholder.markdown(response)

                        st.session_state.conversations[st.session_state.current_conversation_id].append({
                            "role": "assistant",
                            "content": response
                        })
                except Exception as e:
                    st.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()
