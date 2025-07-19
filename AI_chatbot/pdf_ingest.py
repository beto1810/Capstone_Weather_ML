import os
import uuid

import streamlit as st
from dotenv import load_dotenv
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.embeddings import OpenAIEmbeddings
from pinecone import Pinecone
from PyPDF2 import PdfReader

# Load env
load_dotenv()

#test

# Constants
PINECONE_INDEX_NAME = "weather-capstone-project"
PINECONE_NAMESPACE = "weather-data"


# Set up Pinecone
def get_pinecone_index():
    pinecone_api_key = os.getenv("PINECONE_API_KEY")
    if not pinecone_api_key:
        st.error("Missing PINECONE_API_KEY in .env")
        st.stop()
    pc = Pinecone(api_key=pinecone_api_key)
    if PINECONE_INDEX_NAME not in [idx["name"] for idx in pc.list_indexes()]:
        pc.create_index(
            PINECONE_INDEX_NAME,
            dimension=1536,
            metric="cosine",
            spec={"serverless": {"cloud": "aws", "region": "us-east-1"}},
        )
    return pc.Index(PINECONE_INDEX_NAME)


# Extract PDF text
def extract_text_from_pdf(uploaded_file):
    pdf = PdfReader(uploaded_file)
    text = ""
    for page in pdf.pages:
        text += page.extract_text() or ""
    return text


# Chunk the text
def chunk_text(text):
    splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=10)
    return splitter.split_text(text)


# Embed & upsert into Pinecone
def embed_and_upsert(chunks, index,file_name:str):
    embedder = OpenAIEmbeddings()  # or your LiteLLM-compatible embedding class

    with st.spinner("Embedding and uploading to Pinecone..."):
        vectors = []
        for i, chunk in enumerate(chunks):
            chunk_id = f"chunk-{uuid.uuid4()}"
            vector = embedder.embed_query(chunk)
            vectors.append(
                {
                    "id": chunk_id,
                    "values": vector,
                    "metadata": {
                        "chunk_text": chunk,
                        "category": "uploaded_pdf",
                        "file_name": file_name }
                }
            )

        try:
            index.upsert(vectors=vectors, namespace=PINECONE_NAMESPACE)
        except Exception as e:
            st.error(f"Upsert failed: {e}")
    st.success(f"✅ Uploaded {len(chunks)} chunks to Pinecone!")


# MAIN Streamlit PDF uploader
def pdf_ingestion_component():
    st.subheader("📄 Upload a PDF for RAG")

    if "file_uploader_key" not in st.session_state:
        st.session_state.file_uploader_key = str(uuid.uuid4())

    if "pdf_upload_done" not in st.session_state:
        st.session_state.pdf_upload_done = False
    if "uploaded_text" not in st.session_state:
        st.session_state.uploaded_text = None

    # If not uploaded or processing is not done
    if not st.session_state.pdf_upload_done:
        uploaded_file = st.file_uploader(
            "Upload your PDF", type=["pdf"], key=st.session_state.file_uploader_key
        )

        if uploaded_file:
            st.session_state.uploaded_file_name = uploaded_file.name
            if st.session_state.uploaded_text is None:
                # Extract and store text (only once)
                st.session_state.uploaded_text = extract_text_from_pdf(uploaded_file)

            st.text_area(
                "📜 Extracted Text (Preview)",
                value=st.session_state.uploaded_text[:1000],
                height=200,
            )

            if st.button("Process & Upload to Pinecone"):
                chunks = chunk_text(st.session_state.uploaded_text)

                # Preview chunks
                st.subheader("📋 Chunk Preview")
                st.write(f"Total chunks created: {len(chunks)}")

                # Show first few chunks as preview
                for i, chunk in enumerate(chunks[:5]):  # Show first 5 chunks
                    with st.expander(f"Chunk {i+1} (Length: {len(chunk)} chars)"):
                        st.text(chunk)

                if len(chunks) > 5:
                    st.info(f"... and {len(chunks) - 5} more chunks")

                # Proceed with upload
                index = get_pinecone_index()
                embed_and_upsert(chunks, index,st.session_state.uploaded_file_name)

                # ✅ Save uploaded file to current conversation
                conv_id = st.session_state.current_conversation_id
                if conv_id not in st.session_state.uploaded_files:
                    st.session_state.uploaded_files[conv_id] = []
                st.session_state.uploaded_files[conv_id].append(uploaded_file.name)

                st.session_state.pdf_upload_done = True
    else:
        if st.button("📄 Upload another PDF"):
            # Reset everything to allow new upload
            st.session_state.file_uploader_key = str(uuid.uuid4())
            st.session_state.pdf_upload_done = False
            st.session_state.uploaded_text = None
            st.rerun()
