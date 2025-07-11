import streamlit as st

conversation_ids = ["abc12345", "def67890", "ghi11223"]

if "current_conversation_id" not in st.session_state:
    st.session_state.current_conversation_id = conversation_ids[0]

# Separate tracking key to force rerun
if "force_rerun" not in st.session_state:
    st.session_state.force_rerun = False

st.sidebar.title("Conversations")
for conv_id in conversation_ids:
    if st.sidebar.button(
        f"🗂 Conversation {conv_id[:8]}...",
        key=f"conv_{conv_id}",
        type=(
            "secondary"
            if conv_id == st.session_state.current_conversation_id
            else "primary"
        ),
    ):
        st.session_state.current_conversation_id = conv_id
        # ✅ force rerun immediately after setting the new conversation
        st.session_state.force_rerun = not st.session_state.force_rerun
        st.rerun()

st.write(f"💬 Bạn đang xem: `{st.session_state.current_conversation_id}`")
