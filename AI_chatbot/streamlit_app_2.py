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
from pdf_ingest import pdf_ingestion_component
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
DEFAULT_MODEL = "gpt-3.5-turbo"
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
    if results["matches"]:
        formatted_chunks = []
        sources = set()
        file_names = set()

        for chunk in results["matches"]:
            meta = chunk["metadata"]
            chunk_text = meta.get("chunk_text", "").strip()
            file_name = meta.get("file_name", "Unknown file")
            topic = meta.get("topic", "General")

            file_names.add(file_name)

            formatted_chunks.append(f"[{file_name}, topic: {topic}]\n{chunk_text}")
            sources.add(f"- {file_name} (topic: {topic})")

        context = "\n\n".join(formatted_chunks)

        # More informative system prompt
        system_prompt = f"""You are a helpful assistant. Use the extracted context below from uploaded PDFs to answer the user's question. When needed, you may also use your general knowledge to fill in gaps.

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
def query_snowflake_weather(query: str) -> str:
    """
    Handles weather queries for current, past, or future weather from Snowflake.
    Determines correct table and filters based on location and time.
    """

    llm = ChatOpenAI(model=st.session_state.model_name, temperature=0.7)

    # --- Extract city/province ---
    def extract_location_with_llm(query: str) -> str:
        prompt = PromptTemplate.from_template(
            "Extract the city or province name from this query: {query}\nJust return the name only."
        )
        return llm.invoke(prompt.format(query=query)).content.strip()

    # --- Extract date/time ---
    def extract_date_with_llm(query: str) -> str:
        prompt = PromptTemplate.from_template(
            "Extract the date or time reference from this query: {query}\n"
            "Return only the time-related expression like 'yesterday', 'July 2nd', 'today', 'now', etc."
        )
        return llm.invoke(prompt.format(query=query)).content.strip()

    city = extract_location_with_llm(query)
    date_filter = extract_date_with_llm(query)
    parsed_date = dateparser.parse(date_filter)

    # --- Detect 'next X days' or 'in X days' for forecast range ---
    forecast_days = None
    forecast_start = None
    forecast_end = None
    weekend_dates = None
    next_days_match = re.search(r"next (\d+) days", date_filter.lower())
    in_days_match = re.search(r"in (\d+) days", date_filter.lower())
    if next_days_match:
        forecast_days = int(next_days_match.group(1))
    elif in_days_match:
        forecast_days = int(in_days_match.group(1))
    elif "weekend" in date_filter.lower():
        # Find next Saturday and Sunday
        today_weekday = datetime.now().weekday()  # Monday=0, Sunday=6
        days_until_saturday = (5 - today_weekday) % 7
        days_until_sunday = (6 - today_weekday) % 7
        next_saturday = datetime.now().date() + timedelta(days=days_until_saturday)
        next_sunday = datetime.now().date() + timedelta(days=days_until_sunday)
        weekend_dates = [next_saturday, next_sunday]

    # --- Decide which table to query ---
    FCT_CURRENT = "FCT_CURRENT_WEATHER_PROVINCE"
    FCT_HISTORY = "FCT_WEATHER_PROVINCE"
    FCT_FORECAST = "PREDICT_WEATHER_PROVINCE_7DAYS"

    today = datetime.now().date()
    table_name = FCT_CURRENT  # Default

    if forecast_days:
        table_name = FCT_FORECAST
        if forecast_start is None:
            forecast_start = datetime.now().date() + timedelta(days=1)
        if forecast_end is None:
            forecast_end = forecast_start + timedelta(days=forecast_days - 1)
    elif weekend_dates:
        table_name = FCT_FORECAST
    elif parsed_date:
        date_only = parsed_date.date()
        if date_only > today:
            table_name = FCT_FORECAST
        elif date_only < today:
            table_name = FCT_HISTORY
        else:
            table_name = FCT_CURRENT
    else:
        if date_filter.lower() in ["forecast", "tomorrow", "next week","next"]:
            table_name = FCT_FORECAST
        elif date_filter.lower() in ["yesterday", "last week", "last month"]:
            table_name = FCT_HISTORY
        else:
            table_name = FCT_CURRENT

    # --- Build SQL query ---
    sql = f"SELECT {table_name}.*, DIM_VIETNAM_PROVINCES.PROVINCE_NAME FROM {table_name} JOIN DIM_VIETNAM_PROVINCES ON {table_name}.PROVINCE_ID = DIM_VIETNAM_PROVINCES.PROVINCE_ID   WHERE DIM_VIETNAM_PROVINCES.province_name ILIKE %s"
    params = [city]

    if forecast_days:
        # Query for a range of forecast days
        if forecast_start is not None and forecast_end is not None:
            sql += " AND predicted_date >= %s AND predicted_date <= %s"
            params.append(forecast_start.strftime("%Y-%m-%d"))
            params.append(forecast_end.strftime("%Y-%m-%d"))
    elif weekend_dates:
        # Query for both Saturday and Sunday
        sql += " AND predicted_date IN (%s, %s)"
        params.append(weekend_dates[0].strftime("%Y-%m-%d"))
        params.append(weekend_dates[1].strftime("%Y-%m-%d"))
    elif parsed_date and table_name != FCT_CURRENT:
        date_column = "predicted_date" if table_name == FCT_FORECAST else "created_at"
        sql += f" AND {date_column} = %s"
        params.append(parsed_date.strftime("%Y-%m-%d"))

    # --- Connect to Snowflake ---
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
    finally:
        cursor.close()
        conn.close()

    # --- Format result ---
    if not rows:
        return f"No weather data found for **{city}** on **{date_filter or 'today'}**."

    headers = [col[0] for col in cursor.description]
    formatted = [dict(zip(headers, row)) for row in rows]

    preview = formatted[:3]  # Limit preview
    response = f"Here is the weather data for **{city}** on **{date_filter or 'today'}** from table `{table_name}`:\n\n"
    for row in preview:
        for key, val in row.items():
            response += f"- {key}: {val}\n"
        response += "\n"

    if len(formatted) > 3:
        response += f"... and {len(formatted) - 3} more rows."

    # Add last update info with clear date and time
    # Try WEATHER_UPDATED_AT first, then fallback to created_at
    update_col = None
    if formatted and ("WEATHER_UPDATED_AT" in formatted[0]):
        update_col = "WEATHER_UPDATED_AT"
    elif formatted and ("created_at" in formatted[0]):
        update_col = "created_at"

    if update_col:
        update_times = [row.get(update_col) for row in formatted if row.get(update_col)]
        if update_times:
            try:
                # Try to sort as datetimes, fallback to string sort
                from dateutil.parser import parse as dtparse
                update_times_dt = [dtparse(str(t)) for t in update_times]
                most_recent = max(update_times_dt)
                formatted_time = most_recent.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                most_recent = max(update_times)
                formatted_time = str(most_recent)
            response += f"\n**Last update:** {formatted_time}"

    return response





# -------------------------------
# 🔹 RAG Chatbot Creator
# -------------------------------
def create_rag_chatbot(model_name: str = DEFAULT_MODEL):

    llm_model = ChatOpenAI(model=model_name, temperature=0.7)

    agent = initialize_agent(
            tools = [search_similar_chunks,query_snowflake_weather] ,
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
        page_title="FoundryAI RAG Chatbot", page_icon="🧠", layout="wide"
    )
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
