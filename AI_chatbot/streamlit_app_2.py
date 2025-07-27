import os
import uuid

import streamlit as st
from dotenv import load_dotenv
from langchain_community.embeddings import OpenAIEmbeddings
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_litellm import ChatLiteLLM
from langchain.agents import initialize_agent, AgentType
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, StateGraph
from langchain.prompts import PromptTemplate
from langgraph.graph.message import MessagesState
from pdf_ingest import pdf_ingestion_component # type: ignore
from pinecone import Pinecone
from langchain_community.chat_models import ChatOpenAI
from langchain.tools import tool
import snowflake.connector
import dateparser
from datetime import datetime, timedelta
import re

# Load environment variables
load_dotenv()

# Constants
DEFAULT_MODEL = "gpt-4"
PINECONE_INDEX_NAME = "weather-capstone-project"
PINECONE_NAMESPACE = "weather-data"
TOP_K_RESULTS = 2

AVAILABLE_MODELS = {"OpenAI: GPT-3.5-turbo": "gpt-3.5-turbo", "OpenAI: GPT-4": "gpt-4"}

SAMPLE_PROMPTS = [
    "Tell me about FoundryAI's training programs",
    "What courses do you offer?",
]


# -------------------------------
# 🔹 Pinecone Helper Functions
# -------------------------------
def initialize_pinecone(api_key: str):
    pc = Pinecone(api_key=api_key)
    return pc.Index(PINECONE_INDEX_NAME)

@tool
def search_similar_chunks(query: str) -> str:
    """Searches Pinecone for relevant context and answers the user's question."""

    pinecone_api_key = os.getenv("PINECONE_API_KEY")
    if not pinecone_api_key:
        raise ValueError("Please set your PINECONE_API_KEY in environment variables.")
    index =initialize_pinecone(pinecone_api_key)

    embedder = OpenAIEmbeddings()
    query_vector = embedder.embed_query(query)

    results = index.query(
        namespace=PINECONE_NAMESPACE,
        vector=query_vector,
        top_k=TOP_K_RESULTS,
        include_metadata=True,
    )
    if results["matches"]: # type: ignore
        formatted_chunks = []
        sources = set()
        file_names = set()

        for chunk in results["matches"]: # type: ignore
            meta = chunk["metadata"]
            chunk_text = meta.get("chunk_text", "").strip()
            file_name = meta.get("file_name", "Unknown file")
            topic = meta.get("topic", "General")

            file_names.add(file_name)

            formatted_chunks.append(f"[{file_name}, topic: {topic}]\n{chunk_text}")
            sources.add(f"- {file_name} (topic: {topic})")

        context = "\n\n".join(formatted_chunks)

        # More informative system prompt
        system_prompt = f"""You are a helpful assistant. Use the extracted context below from uploaded PDFs to answer the user's question.

PDF Context Chunks:
{context}

Sources:
{chr(10).join(sources)}

Provide a clear and accurate response based on this context whenever possible and Please reference the source file(s) in your answer if relevant.
"""

    else:
        file_name = set()
        system_prompt = "You are a helpful assistant. Answer based on your general knowledge."

    messages = [
        SystemMessage(content=system_prompt),
        {"role": "user", "content": query}
    ]

    model_name = st.session_state.model_name

    llm = ChatOpenAI(model=model_name, temperature=0.7)
    response = llm.invoke(messages)

    if file_names:
        source_note = (f"\n\n_This answer was generated using the uploaded file(s): "
            f"**{', '.join(sorted(file_names))}**_"
            )
    else:
        source_note = ""

    # Ensure response.content is a string for concatenation
    content = response.content
    if isinstance(content, list):
        content = ''.join(str(x) for x in content)
    return content + source_note



@tool
def query_current_weather(query: str) -> str:
    """
    Handles queries for current weather from Snowflake.
    Extracts city/province and fetches current weather only.
    """
    llm = ChatOpenAI(model=st.session_state.model_name, temperature=0.7)

    def extract_location_with_llm(query: str) -> str:
        prompt = PromptTemplate.from_template(
            "Extract the city or province name from this query: {query}\nJust return the name only."
        )
        result = llm.invoke(prompt.format(query=query)).content
        if isinstance(result, list):
            return str(result[0])
        return str(result).strip()

    city = extract_location_with_llm(query)
    FCT_CURRENT = "FCT_CURRENT_WEATHER_PROVINCE"
    sql = f"SELECT {FCT_CURRENT}.*, DIM_VIETNAM_PROVINCES.PROVINCE_NAME FROM {FCT_CURRENT} JOIN DIM_VIETNAM_PROVINCES ON {FCT_CURRENT}.PROVINCE_ID = DIM_VIETNAM_PROVINCES.PROVINCE_ID WHERE DIM_VIETNAM_PROVINCES.province_name ILIKE %s"
    params = [city]

    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE", "USER_DBT_ROLE"),
    )
    cursor = conn.cursor()
    try:
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        headers = [col[0] for col in cursor.description]
    finally:
        cursor.close()
        conn.close()

    if not rows:
        return f"No current weather data found for **{city}**."
    formatted = [dict(zip(headers, row)) for row in rows]
    preview = formatted[:3]
    response = f"Here is the current weather data for **{city}** from table `{FCT_CURRENT}`:\n\n"
    for row in preview:
        for key, val in row.items():
            response += f"- {key}: {val}\n"
        response += "\n"
    if len(formatted) > 3:
        response += f"... and {len(formatted) - 3} more rows."
    return response


@tool
def query_past_weather(query: str) -> str:
    """
    Handles queries for past weather from Snowflake.
    Extracts city/province and date, fetches historical weather.
    """
    llm = ChatOpenAI(model=st.session_state.model_name, temperature=0.7)

    def extract_location_with_llm(query: str) -> str:
        prompt = PromptTemplate.from_template(
            "Extract the city or province name from this query: {query}\nJust return the name only."
        )
        result = llm.invoke(prompt.format(query=query)).content
        if isinstance(result, list):
            return str(result[0])
        return str(result).strip()

    def extract_date_with_llm(query: str) -> str:
        prompt = PromptTemplate.from_template(
            "Extract the date or time reference from this query: {query}\nReturn only the time-related expression like 'yesterday', 'July 2nd', '23/07/2025', '2025-07-23', etc. If there's a date range, return the first date only."
        )
        result = llm.invoke(prompt.format(query=query)).content
        if isinstance(result, list):
            return str(result[0])
        return str(result).strip()

    city = extract_location_with_llm(query)
    date_filter = extract_date_with_llm(query)
    parsed_date = dateparser.parse(date_filter)
    if not parsed_date:
        # Try to parse date directly from the query if LLM fails
        parsed_date = dateparser.parse(query)
        if parsed_date:
            date_filter = parsed_date.strftime("%Y-%m-%d")
        else:
            # Try to extract date patterns from the query
            import re
            date_patterns = [
                r'\d{2}/\d{2}/\d{4}',  # DD/MM/YYYY
                r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
                r'\d{2}-\d{2}-\d{4}',  # DD-MM-YYYY
            ]
            for pattern in date_patterns:
                match = re.search(pattern, query)
                if match:
                    parsed_date = dateparser.parse(match.group())
                    if parsed_date:
                        date_filter = parsed_date.strftime("%Y-%m-%d")
                        break
            if not parsed_date:
                date_filter = "unknown"
    FCT_HISTORY = "FCT_WEATHER_PROVINCE"
    sql = f"SELECT {FCT_HISTORY}.*, DIM_VIETNAM_PROVINCES.PROVINCE_NAME FROM {FCT_HISTORY} JOIN DIM_VIETNAM_PROVINCES ON {FCT_HISTORY}.PROVINCE_ID = DIM_VIETNAM_PROVINCES.PROVINCE_ID WHERE DIM_VIETNAM_PROVINCES.province_name ILIKE %s"
    params = [city]
    if parsed_date:
        sql += " AND weather_date = %s"
        params.append(parsed_date.strftime("%Y-%m-%d"))
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE", "USER_DBT_ROLE"),
    )
    cursor = conn.cursor()
    try:
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        headers = [col[0] for col in cursor.description]
    finally:
        cursor.close()
        conn.close()
    if not rows:
        return f"No past weather data found for **{city}** on **{date_filter}**."
    formatted = [dict(zip(headers, row)) for row in rows]
    preview = formatted[:3]
    response = f"Here is the past weather data for **{city}** on **{date_filter}** from table `{FCT_HISTORY}`:\n\n"
    for row in preview:
        for key, val in row.items():
            response += f"- {key}: {val}\n"
        response += "\n"
    if len(formatted) > 3:
        response += f"... and {len(formatted) - 3} more rows."
    return response


@tool
def query_predict_weather(query: str) -> str:
    """
    Handles queries for future weather (forecast) from Snowflake.
    Extracts city/province and date, fetches forecast weather.
    """
    llm = ChatOpenAI(model=st.session_state.model_name, temperature=0.7)

    def extract_location_with_llm(query: str) -> str:
        prompt = PromptTemplate.from_template(
            "Extract the city or province name from this query: {query}\nJust return the name only."
        )
        result = llm.invoke(prompt.format(query=query)).content
        if isinstance(result, list):
            return str(result[0])
        return str(result).strip()

    def extract_date_with_llm(query: str) -> str:
        prompt = PromptTemplate.from_template(
            "Extract the date or time reference from this query: {query}\nReturn only the time-related expression like 'tomorrow', 'next week', '23/07/2025', '2025-07-23', etc. If there's a date range, return the first date only."
        )
        result = llm.invoke(prompt.format(query=query)).content
        if isinstance(result, list):
            return str(result[0])
        return str(result).strip()

    city = extract_location_with_llm(query)
    date_filter = extract_date_with_llm(query)
    parsed_date = dateparser.parse(date_filter)
    if not parsed_date:
        # Try to parse date directly from the query if LLM fails
        parsed_date = dateparser.parse(query)
        if parsed_date:
            date_filter = parsed_date.strftime("%Y-%m-%d")
        else:
            # Try to extract date patterns from the query
            import re
            date_patterns = [
                r'\d{2}/\d{2}/\d{4}',  # DD/MM/YYYY
                r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
                r'\d{2}-\d{2}-\d{4}',  # DD-MM-YYYY
            ]
            for pattern in date_patterns:
                match = re.search(pattern, query)
                if match:
                    parsed_date = dateparser.parse(match.group())
                    if parsed_date:
                        date_filter = parsed_date.strftime("%Y-%m-%d")
                        break
            if not parsed_date:
                date_filter = "unknown"
    FCT_FORECAST = "PREDICT_WEATHER_PROVINCE_7DAYS"
    sql = f"SELECT {FCT_FORECAST}.*, DIM_VIETNAM_PROVINCES.PROVINCE_NAME FROM {FCT_FORECAST} JOIN DIM_VIETNAM_PROVINCES ON {FCT_FORECAST}.PROVINCE_ID = DIM_VIETNAM_PROVINCES.PROVINCE_ID WHERE DIM_VIETNAM_PROVINCES.province_name ILIKE %s"
    params = [city]
    if parsed_date:
        sql += " AND predicted_date = %s"
        params.append(parsed_date.strftime("%Y-%m-%d"))
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE", "USER_DBT_ROLE"),
    )
    cursor = conn.cursor()
    try:
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        headers = [col[0] for col in cursor.description]
    finally:
        cursor.close()
        conn.close()
    if not rows:
        return f"No forecast weather data found for **{city}** on **{date_filter}**."
    formatted = [dict(zip(headers, row)) for row in rows]
    preview = formatted[:3]
    response = f"Here is the forecast weather data for **{city}** on **{date_filter}** from table `{FCT_FORECAST}`:\n\n"
    for row in preview:
        for key, val in row.items():
            response += f"- {key}: {val}\n"
        response += "\n"
    if len(formatted) > 3:
        response += f"... and {len(formatted) - 3} more rows."
    return response





# -------------------------------
# 🔹 RAG Chatbot Creator
# -------------------------------
def create_rag_chatbot(model_name: str = DEFAULT_MODEL):

    llm_model = ChatOpenAI(model=model_name, temperature=0.7)

    agent = initialize_agent(
            tools = [search_similar_chunks, query_current_weather, query_past_weather, query_predict_weather] ,
            llm  = llm_model,
            agent_type = AgentType.OPENAI_FUNCTIONS,
            verbose =True

    )

    workflow = StateGraph(state_schema=MessagesState)

    def rag_node(state: MessagesState):
        user_query = state["messages"][-1].content
        response = agent.run(user_query)
        return {
            "messages": state["messages"] + [{"role": "assistant", "content": response}]
        }


    # def query_node(state: MessagesState):

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
        index=list(AVAILABLE_MODELS.values()).index(st.session_state.model_name),
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
        if (
            has_uploads
            or has_messages
            or conv_id == st.session_state.current_conversation_id
        ):
            label = f"{conv_id[:8]}"
            if has_uploads:
                label += " 📎"
            conversation_options.append((label, conv_id))

    if conversation_options:
        labels, ids = zip(*conversation_options)
        selected_label = st.sidebar.radio(
            "Select Conversation",
            options=labels,
            index=(
                ids.index(st.session_state.current_conversation_id)
                if st.session_state.current_conversation_id in ids
                else 0
            ),
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


# def transcribe_audio(audio_bytes):
#     client = openai.OpenAI()  # Assumes OPENAI_API_KEY is set
#     with open("temp_audio.wav", "wb") as f:
#         f.write(audio_bytes)
#     with open("temp_audio.wav", "rb") as f:
#         transcript = client.audio.transcriptions.create(
#             model="whisper-1",
#             file=f
#         )
#     return transcript.text


# -------------------------------
# 🔹 Streamlit Main
# -------------------------------
def main():

    if not os.getenv("OPENAI_API_KEY"):
        st.error(
            "[red]Error: Please set your OPENAI_API_KEY environment variable[/red]"
        )
        return

    st.set_page_config(
        page_title="Weather AI Chatbot", page_icon="☀️", layout="wide"
    )
    st.title("☀️🌧️⛅ Weather AI ChatBot")

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

    for message in st.session_state.conversations.get(
        st.session_state.current_conversation_id, []
    ):
        if message["role"] != "system":
            with st.chat_message(message["role"]):
                st.markdown(message["content"])

    selected_prompt = None

    # Use either text input or voice input
    prompt = (
        st.chat_input("Ask something about FoundryAI...")
        or st.session_state.get("voice_input")
        or selected_prompt
    )

    if prompt:
        if (
            st.session_state.current_conversation_id
            not in st.session_state.conversations
        ):
            st.session_state.conversations[st.session_state.current_conversation_id] = (
                []
            )

        st.session_state.conversations[st.session_state.current_conversation_id].append(
            {"role": "user", "content": prompt}
        )

        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            message_placeholder = st.empty()
            with st.spinner("Retrieving context and generating answer..."):
                try:
                    input_messages = [HumanMessage(content=prompt)]

                    result = st.session_state.chatbot.invoke(
                        {"messages": input_messages},
                        config={
                            "configurable": {
                                "thread_id": st.session_state.current_conversation_id
                            }
                        },
                    )

                    if result and "messages" in result and result["messages"]:
                        response = result["messages"][-1].content
                        message_placeholder.markdown(response)

                        st.session_state.conversations[
                            st.session_state.current_conversation_id
                        ].append({"role": "assistant", "content": response})
                except Exception as e:
                    st.error(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()
