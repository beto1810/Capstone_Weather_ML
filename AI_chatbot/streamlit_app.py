import streamlit as st
import os
from dotenv import load_dotenv
from langchain_litellm import ChatLiteLLM
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from langgraph.graph import StateGraph, END, START
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph.message import MessagesState
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
import uuid
import json

# Load environment variables
load_dotenv()

# Constants
DEFAULT_MODEL = "gpt-3.5-turbo"
DEFAULT_SYSTEM_PROMPT = """You are a helpful FoundryAI training assistant. FoundryAI is an educational facility for AI training.
Keep your responses concise and friendly. Introduce yourself and ask for the user's name."""

# Available models
AVAILABLE_MODELS = {
    "OpenAI: GPT-3.5-turbo": "gpt-3.5-turbo",
    "Snowflake: Mistral-7b": "snowflake/mistral-7b"
}

# Sample prompts for quick selection
SAMPLE_PROMPTS = [
    "Tell me about FoundryAI's training programs",
    "What courses do you offer?"
]

def setup_snowflake_credentials():
    """Set up Snowflake credentials from environment variables"""
    required_vars = [
        "SNOWFLAKE_JWT",
        "SNOWFLAKE_ACCOUNT_ID"
    ]

    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    if missing_vars:
        st.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        return None

    return {
        "jwt": os.environ["SNOWFLAKE_JWT"],
        "account_id": os.environ["SNOWFLAKE_ACCOUNT_ID"]
    }

def create_chatbot(model_name: str, system_prompt: str):
    """Create a chatbot instance using LangGraph and LiteLLM"""
    # Initialize the LLM with appropriate configuration
    if model_name.startswith("snowflake/"):
        credentials = setup_snowflake_credentials()
        if not credentials:
            return None

        llm = ChatLiteLLM(
            model=model_name,
            temperature=0.7,
            api_key=credentials["jwt"],
            api_base=f"https://{credentials['account_id']}.snowflakecomputing.com/api/v2/cortex/inference:complete"
        )
    else:
        llm = ChatLiteLLM(
            model=model_name,
            temperature=0.7
        )

    # Create prompt template
    prompt = ChatPromptTemplate.from_messages([
        ("system", system_prompt),
        MessagesPlaceholder(variable_name="messages"),
    ])

    # Create the workflow
    workflow = StateGraph(state_schema=MessagesState)

    # Define the model node
    def call_model(state: MessagesState):
        """Call the model with the current state"""
        formatted_prompt = prompt.format_messages(messages=state["messages"])
        response = llm.invoke(formatted_prompt)
        return {"messages": response}

    # Add the model node
    workflow.add_node("model", call_model)

    # Add edges
    workflow.add_edge(START, "model")
    workflow.add_edge("model", END)

    # Compile the workflow with memory
    memory = MemorySaver()
    app = workflow.compile(checkpointer=memory)

    return app

def init_session_state():
    """Initialize session state variables"""
    if "conversations" not in st.session_state:
        st.session_state.conversations = {}
    if "current_conversation_id" not in st.session_state:
        st.session_state.current_conversation_id = str(uuid.uuid4())
    if "model_name" not in st.session_state:
        st.session_state.model_name = DEFAULT_MODEL
    if "system_prompt" not in st.session_state:
        st.session_state.system_prompt = DEFAULT_SYSTEM_PROMPT
    if "chatbot" not in st.session_state:
        st.session_state.chatbot = create_chatbot(st.session_state.model_name, st.session_state.system_prompt)

def create_new_conversation():
    """Create a new conversation"""
    conversation_id = str(uuid.uuid4())
    st.session_state.conversations[conversation_id] = []
    st.session_state.current_conversation_id = conversation_id
    st.session_state.chatbot = create_chatbot(st.session_state.model_name, st.session_state.system_prompt)

def switch_conversation(conversation_id):
    """Switch to a different conversation"""
    st.session_state.current_conversation_id = conversation_id

def config_sidebar():
    """Configure sidebar options"""
    st.sidebar.title("Chat Configuration")

    # Model selection
    selected_model = st.sidebar.selectbox(
        "Select Model",
        options=list(AVAILABLE_MODELS.keys()),
        index=list(AVAILABLE_MODELS.keys()).index(next(k for k, v in AVAILABLE_MODELS.items() if v == st.session_state.model_name))
    )

    if AVAILABLE_MODELS[selected_model] != st.session_state.model_name:
        st.session_state.model_name = AVAILABLE_MODELS[selected_model]
        st.session_state.chatbot = create_chatbot(st.session_state.model_name, st.session_state.system_prompt)

    # System prompt customization
    st.sidebar.title("System Prompt")
    new_system_prompt = st.sidebar.text_area(
        "Customize System Prompt",
        value=st.session_state.system_prompt,
        height=150
    )

    if new_system_prompt != st.session_state.system_prompt:
        st.session_state.system_prompt = new_system_prompt
        st.session_state.chatbot = create_chatbot(st.session_state.model_name, st.session_state.system_prompt)

    # Conversation management
    st.sidebar.title("Conversations")
    if st.sidebar.button("New Conversation"):
        create_new_conversation()

    # Display existing conversations
    for conv_id in st.session_state.conversations:
        if st.sidebar.button(
            f"Conversation {conv_id[:8]}...",
            key=f"conv_{conv_id}",
            type="secondary" if conv_id == st.session_state.current_conversation_id else "primary"
        ):
            switch_conversation(conv_id)

    # Debug information
    if st.sidebar.checkbox("Show Debug Info"):
        st.sidebar.json(st.session_state)

def display_sample_prompts():
    """Display sample prompts for quick selection"""
    st.markdown("### Sample Prompts")
    cols = st.columns(2)
    for i, prompt in enumerate(SAMPLE_PROMPTS):
        with cols[i % 2]:
            if st.button(prompt, key=f"sample_{i}"):
                return prompt
    return None

def main():
    st.title("FoundryAI Training Assistant")

    # Initialize session state
    init_session_state()

    # Configure sidebar
    config_sidebar()

    # Display chat messages
    for message in st.session_state.conversations.get(st.session_state.current_conversation_id, []):
        if message["role"] != "system":  # Don't display system messages
            with st.chat_message(message["role"]):
                st.markdown(message["content"])

    # Display sample prompts
    selected_prompt = display_sample_prompts()

    # Chat input
    prompt = st.chat_input("What would you like to know?") or selected_prompt

    if prompt:
        # Add user message to chat history
        if st.session_state.current_conversation_id not in st.session_state.conversations:
            st.session_state.conversations[st.session_state.current_conversation_id] = []

        st.session_state.conversations[st.session_state.current_conversation_id].append({
            "role": "user",
            "content": prompt
        })

        # Display user message
        with st.chat_message("user"):
            st.markdown(prompt)

        # Get assistant response
        with st.chat_message("assistant"):
            message_placeholder = st.empty()
            with st.spinner("Thinking..."):
                try:
                    # Create input message
                    input_messages = [HumanMessage(content=prompt)]

                    # Get response from the model
                    result = st.session_state.chatbot.invoke(
                        {"messages": input_messages},
                        config={"configurable": {"thread_id": st.session_state.current_conversation_id}}
                    )

                    if result and "messages" in result and result["messages"]:
                        response = result["messages"][-1].content
                        message_placeholder.markdown(response)

                        # Add assistant response to chat history
                        st.session_state.conversations[st.session_state.current_conversation_id].append({
                            "role": "assistant",
                            "content": response
                        })
                except Exception as e:
                    st.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()