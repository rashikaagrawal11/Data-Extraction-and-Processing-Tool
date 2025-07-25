from langchain_community.llms import Ollama
from langchain.chains import RetrievalQA
from langchain.vectorstores.pgvector import PGVector
from langchain.embeddings import OllamaEmbeddings
from langchain.text_splitter import CharacterTextSplitter
from langchain.docstore.document import Document
from sqlalchemy import create_engine
import os
import json

# Load model
llm = Ollama(model="mistral")  # or llama2, etc.

# Embeddings
embedding = OllamaEmbeddings(model="mistral")  # for vector store

# PostgreSQL connection
DATABASE_URL = os.getenv("DB_URL")
engine = create_engine(DATABASE_URL)

# Create PGVector store
vector_store = PGVector(
    connection_string=DATABASE_URL,
    embedding_function=embedding,
    collection_name="table_docs"
)

# (Optional) Ingest table rows as documents into vector store
def ingest_rows():
    from prisma import Prisma
    db = Prisma()
    import asyncio

    async def _ingest():
        await db.connect()
        rows = await db.tablerecord.find_many()
        docs = []
        for row in rows:
            combined = f"Title: {row.title}\nDescription: {row.description}\nData: {row.table_json}"
            docs.append(Document(page_content=combined))
        splitter = CharacterTextSplitter(chunk_size=1000, chunk_overlap=100)
        chunks = splitter.split_documents(docs)
        vector_store.add_documents(chunks)
        await db.disconnect()
    asyncio.run(_ingest())

# Query
def query_ollama(query_text):
    retriever = vector_store.as_retriever(search_kwargs={"k": 5})
    qa_chain = RetrievalQA.from_chain_type(llm=llm, retriever=retriever)
    return qa_chain.run(query_text)
