import streamlit as st
import pandas as pd
import pyodbc
import sqlite3
import re
from sqlalchemy import create_engine, text
import pandasai as pai
import logging
import matplotlib.pyplot as plt
import time
import requests
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Check if PandasAI is available
try:
    import pandasai
    PANDASAI_AVAILABLE = True
except ImportError:
    PANDASAI_AVAILABLE = False
    st.error("PandasAI n'est pas installé. Installez-le avec: pip install pandasai==2.1.0")

# Set PandasAI global configuration
if PANDASAI_AVAILABLE:
    pai.config.verbose = False
    pai.config.enable_cache = False
    pai.api_key.set("****")

# Page configuration
st.set_page_config(
    page_title="AI Assistant + Power BI + Database",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Enhanced CSS for modern chat interface
st.markdown("""
<style>
    /* Main layout */
    .block-container {
        padding-top: 1rem;
        padding-bottom: 0rem;
        padding-left: 1rem;
        padding-right: 1rem;
    }
    
    /* Sidebar styling */
    .css-1d391kg {
        padding-top: 1rem;
    }
    .stSelectbox > div > div {
        background-color: #f0f2f6;
    }
    
    /* Chat container */
    .chat-container {
        height: 600px;
        overflow-y: auto;
        overflow-x: hidden;
        padding: 20px;
        margin-bottom: 20px;
        background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
        border-radius: 15px;
        border: 2px solid #e0e6ed;
        box-shadow: inset 0 2px 10px rgba(0, 0, 0, 0.1);
        scroll-behavior: smooth;
    }
    
    /* Custom scrollbar */
    .chat-container::-webkit-scrollbar {
        width: 8px;
    }
    
    .chat-container::-webkit-scrollbar-track {
        background: rgba(0, 0, 0, 0.1);
        border-radius: 10px;
    }
    
    .chat-container::-webkit-scrollbar-thumb {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 10px;
    }
    
    .chat-container::-webkit-scrollbar-thumb:hover {
        background: linear-gradient(135deg, #764ba2 0%, #667eea 100%);
    }
    
    /* Message bubbles */
    .user-message {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 12px 16px;
        border-radius: 18px 18px 5px 18px;
        margin: 8px 0 8px auto;
        max-width: 70%;
        word-wrap: break-word;
        box-shadow: 0 2px 10px rgba(102, 126, 234, 0.3);
        animation: slideInRight 0.3s ease-out;
        display: block;
        width: fit-content;
    }
    
    .bot-message {
        background: white;
        color: #333;
        padding: 12px 16px;
        border-radius: 18px 18px 18px 5px;
        margin: 8px auto 8px 0;
        max-width: 70%;
        word-wrap: break-word;
        box-shadow: 0 2px 10px rgba(0, 0, 0, 0.1);
        border-left: 4px solid #667eea;
        animation: slideInLeft 0.3s ease-out;
        display: block;
        width: fit-content;
    }
    
    .error-message {
        background: linear-gradient(135deg, #ff6b6b 0%, #ee5a52 100%);
        color: white;
        padding: 12px 16px;
        border-radius: 18px 18px 18px 5px;
        margin: 8px auto 8px 0;
        max-width: 70%;
        word-wrap: break-word;
        box-shadow: 0 2px 10px rgba(255, 107, 107, 0.3);
        display: block;
        width: fit-content;
    }
    
    /* Message timestamp */
    .message-time {
        font-size: 0.7rem;
        opacity: 0.7;
        margin-top: 4px;
    }
    
    /* Chat input area */
    .chat-input {
        background: white;
        border-radius: 25px;
        padding: 10px 20px;
        box-shadow: 0 2px 15px rgba(0, 0, 0, 0.1);
        border: 2px solid #e0e0e0;
        transition: border-color 0.3s ease;
    }
    
    .chat-input:focus-within {
        border-color: #667eea;
        box-shadow: 0 2px 15px rgba(102, 126, 234, 0.2);
    }
    
    /* Animations */
    @keyframes slideInRight {
        from { opacity: 0; transform: translateX(30px); }
        to { opacity: 1; transform: translateX(0); }
    }
    
    @keyframes slideInLeft {
        from { opacity: 0; transform: translateX(-30px); }
        to { opacity: 1; transform: translateX(0); }
    }
    
    
    /* Status indicators */
    .status-connected {
        color: #28a745;
        font-weight: bold;
    }
    
    .status-disconnected {
        color: #dc3545;
        font-weight: bold;
    }
    
    /* Data preview styling */
    .data-preview {
        background: #f8f9fa;
        border-radius: 10px;
        padding: 1rem;
        margin-top: 1rem;
    }
    
    /* Welcome message */
    .welcome-message {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 20px;
        border-radius: 15px;
        text-align: center;
        margin-bottom: 1rem;
    }
    
    /* Message content containers */
    .message-dataframe {
        background: #f8f9fa;
        padding: 10px;
        border-radius: 8px;
        margin-top: 8px;
        border: 1px solid #dee2e6;
    }
    
    .message-chart {
        background: white;
        padding: 10px;
        border-radius: 8px;
        margin-top: 8px;
        border: 1px solid #dee2e6;
        text-align: center;
    }
    
    /* Hide Streamlit elements */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# Function to preprocess SQL file for SQLite
def preprocess_sql_for_sqlite(sql_content):
    sql_content = re.sub(r'^\s*SET.*;$\n?', '', sql_content, flags=re.MULTILINE)
    sql_content = re.sub(r'/\*!.*?\*/;', '', sql_content)
    sql_content = re.sub(r'\s*ENGINE\s*=\s*\w+\s*', ' ', sql_content)
    sql_content = re.sub(r'\s*DEFAULT\s+CHARSET\s*=\s*\w+\s*', ' ', sql_content)
    sql_content = re.sub(r'\s*COLLATE\s*=\s*\w+\s*', ' ', sql_content)
    sql_content = re.sub(r'^\s*START TRANSACTION;$\n?', '', sql_content, flags=re.MULTILINE)
    sql_content = re.sub(r'^\s*COMMIT;$\n?', '', sql_content, flags=re.MULTILINE)
    sql_content = sql_content.replace('`', '"')
    return sql_content

# Function to connect to MySQL
@st.cache_resource
def connect_to_mysql_sqlalchemy(host, database, username, password, port=3306):
    try:
        conn_str = f"mysql+pymysql://{username}:{password or ''}@{host}:{port}/{database}"
        logger.debug(f"Attempting MySQL connection: {conn_str.replace(password or '', '[hidden]')}")
        engine = create_engine(conn_str)
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        logger.debug("MySQL connection successful")
        return engine
    except Exception as e:
        logger.error(f"MySQL connection failed: {str(e)}")
        st.error(f"Échec de la connexion MySQL : {str(e)}")
        return None

# Function to connect to Azure SQL or Dataverse
@st.cache_resource
def connect_to_database(server, database, username, password, driver="ODBC Driver 17 for SQL Server"):
    try:
        conn_str = f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={username};PWD={password};Connection Timeout=30"
        logger.debug(f"Attempting Azure SQL connection with driver: {driver}")
        conn = pyodbc.connect(conn_str)
        logger.debug("Azure SQL connection successful")
        return conn
    except Exception as e:
        logger.error(f"Azure SQL connection failed: {str(e)}")
        st.error(f"Connection failed: {e}")
        return None

# Function to check connection validity
def is_connection_valid(conn):
    if conn is None:
        return False
    try:
        if isinstance(conn, pyodbc.Connection):
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            return cursor.fetchone() is not None
        elif hasattr(conn, 'connect'):
            with conn.connect() as connection:
                result = connection.execute(text("SELECT 1"))
                return result.fetchone() is not None
        return False
    except Exception as e:
        logger.error(f"Connection validation failed: {str(e)}")
        return False

# Function to create AI assistant
def create_ai_assistant(df):
    if not PANDASAI_AVAILABLE:
        logger.error("PandasAI is not available")
        return None
    try:
        df_copy = pd.DataFrame(df.copy())
        logger.debug(f"Preparing DataFrame for chat with shape: {df_copy.shape}, columns: {df_copy.columns.tolist()}")
        return pai.SmartDataframe(df_copy)
    except Exception as e:
        logger.error(f"Error preparing DataFrame: {str(e)}")
        st.error("❌ Impossible de préparer les données pour l'assistant IA.")
        return None

# Function to check if query is meaningful
def is_meaningful_query(query):
    query_lower = query.strip().lower()
    if len(query_lower) < 3:
        return False, "Question trop courte"
    non_meaningful = ["hi", "hello", "bonjour", "salut", "test", "ok", "yes", "no", "oui", "non"]
    if query_lower in non_meaningful:
        return False, "Veuillez poser une question sur les données"
    analysis_keywords = [
        "montre", "affiche", "graphique", "tableau", "analyse", "combien", "quelle", "quel",
        "moyenne", "somme", "total", "maximum", "minimum", "répartition", "distribution",
        "compare", "comparaison", "tendance", "évolution", "statistique", "corrélation",
        "pourcentage", "proportion", "nombre", "créé", "génère", "visualise",
        "show", "display", "chart", "graph", "table", "analyze", "how many", "what", "which",
        "average", "sum", "total", "max", "min", "distribution", "compare", "comparison",
        "trend", "statistics", "correlation", "percentage", "proportion", "count", "create",
        "generate", "visualize", "plot", "most", "common", "type"
    ]
    if any(keyword in query_lower for keyword in analysis_keywords) or len(query_lower) > 10:
        return True, ""
    return False, "Veuillez poser une question sur les données"

# Function to handle special queries directly
def handle_special_queries(query, df):
    query_lower = query.lower().strip()
    if any(keyword in query_lower for keyword in ["colonnes", "columns", "nom des colonnes", "column names", "structure", "champs", "fields"]):
        columns_info = [f"{i}. **{col}** ({df[col].dtype})" for i, col in enumerate(df.columns, 1)]
        return "text", f"**Colonnes disponibles dans le dataset :**\n\n" + "\n".join(columns_info)
    elif any(keyword in query_lower for keyword in ["taille", "size", "dimensions", "combien de lignes", "rows", "shape"]):
        return "text", f"**Informations sur le dataset :**\n\n- **Nombre de lignes :** {len(df)}\n- **Nombre de colonnes :** {len(df.columns)}\n- **Taille totale :** {df.shape}"
    elif any(keyword in query_lower for keyword in ["types", "dtypes", "type de données", "data types"]):
        types_info = [f"- **{col}** : {df[col].dtype}" for col in df.columns]
        return "text", f"**Types de données :**\n\n" + "\n".join(types_info)
    elif any(keyword in query_lower for keyword in ["aperçu", "preview", "head", "premières lignes", "first rows", "échantillon", "sample"]):
        return "dataframe", df.head(10)
    return None, None

# Function to execute query with retries
def execute_pandasai_query(df, query, max_retries=2, retry_delay=2):
    for attempt in range(max_retries):
        try:
            logger.debug(f"Attempt {attempt + 1}/{max_retries} to execute query: {query}")
            response = df.chat(query)
            logger.debug(f"Query executed successfully, response type: {type(response)}")
            if response is None:
                return "text", "Je n'ai pas pu générer une réponse pour cette question. Essayez de la reformuler."
            if hasattr(response, 'figure') or str(type(response)).find('matplotlib') != -1:
                return "chart", response
            if isinstance(response, pd.DataFrame):
                return "dataframe", response
            response_str = str(response)
            return "text", response_str if response_str.strip() else "Réponse vide reçue. Essayez une question différente."
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error on attempt {attempt + 1}: {str(e)}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying after {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                return "text", "Problème de connexion avec PandasAI. Vérifiez votre connexion Internet."
        except Exception as e:
            error_msg = str(e).lower()
            logger.error(f"Error executing query on attempt {attempt + 1}: {str(e)}")
            if "invalid output" in error_msg or "incompatible type" in error_msg:
                return "text", f"PandasAI n'a pas pu générer une réponse appropriée. Essayez de reformuler votre question."
            if "connection" in error_msg or "timeout" in error_msg:
                return "text", "Problème de connexion avec PandasAI. Vérifiez votre connexion Internet."
            if attempt < max_retries - 1:
                continue
            return "text", f"Erreur lors du traitement : {str(e)}"

# Function to display chat messages
def display_chat_message(role, content, timestamp=None, extra_content=None, content_type=None):
    if timestamp is None:
        timestamp = datetime.now().strftime("%H:%M")
    if role == "user":
        st.markdown(f"""
        <div class="user-message">
            {content}
            <div class="message-time">{timestamp}</div>
        </div>
        """, unsafe_allow_html=True)
    elif role == "assistant":
        st.markdown(f"""
        <div class="bot-message">
            🤖 {content}
            <div class="message-time">{timestamp}</div>
        </div>
        """, unsafe_allow_html=True)
        if extra_content is not None:
            with st.container():
                if content_type == "dataframe":
                    st.markdown('<div class="message-dataframe">', unsafe_allow_html=True)
                    st.dataframe(extra_content, use_container_width=True, height=200)
                    st.markdown('</div>', unsafe_allow_html=True)
                elif content_type == "chart":
                    st.markdown('<div class="message-chart">', unsafe_allow_html=True)
                    st.pyplot(extra_content, use_container_width=True)
                    st.markdown('</div>', unsafe_allow_html=True)
    elif role == "error":
        st.markdown(f"""
        <div class="error-message">
            ❌ {content}
            <div class="message-time">{timestamp}</div>
        </div>
        """, unsafe_allow_html=True)

# Auto-scroll function
def auto_scroll_chat():
    st.markdown("""
    <script>
        setTimeout(function() {
            var chatContainer = document.querySelector('.chat-container');
            if (chatContainer) {
                chatContainer.scrollTop = chatContainer.scrollHeight;
            }
        }, 100);
    </script>
    """, unsafe_allow_html=True)

# Initialize session state
if "df" not in st.session_state:
    st.session_state.df = None
if "db_conn" not in st.session_state:
    st.session_state.db_conn = None
if "chat_messages" not in st.session_state:
    st.session_state.chat_messages = []
if "ai_assistant" not in st.session_state:
    st.session_state.ai_assistant = None
if "processing_query" not in st.session_state:
    st.session_state.processing_query = False
if "connection_params" not in st.session_state:
    st.session_state.connection_params = None
if "table_name" not in st.session_state:
    st.session_state.table_name = None

# Sidebar for data source selection
st.sidebar.title("🔍 Data Source & Configuration")
st.sidebar.markdown("### 📊 Select Data Source")
data_source = st.sidebar.selectbox(
    "Choose source",
    ["CSV File", "SQL File", "Azure SQL", "Dataverse", "MySQL"],
    key="data_source_select"
)

df = None

# Data source handling
if data_source == "CSV File":
    uploaded_file = st.sidebar.file_uploader("📁 Upload CSV file", type=["csv"], key="csv_uploader")
    if uploaded_file:
        try:
            df = pd.read_csv(uploaded_file)
            st.session_state.df = df
            st.session_state.db_conn = None
            st.session_state.ai_assistant = None
            st.sidebar.success("✅ CSV file loaded successfully")
        except Exception as e:
            logger.error(f"Error reading CSV file: {str(e)}")
            st.sidebar.error(f"❌ Error reading CSV: {e}")

elif data_source == "SQL File":
    uploaded_file = st.sidebar.file_uploader("📁 Upload SQL file", type=["sql"], key="sql_uploader")
    if uploaded_file:
        try:
            conn = sqlite3.connect(':memory:')
            cursor = conn.cursor()
            sql_content = uploaded_file.read().decode('utf-8')
            sql_content = preprocess_sql_for_sqlite(sql_content)
            cursor.executescript(sql_content)
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            if tables:
                table_name = tables[0][0]
                df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
                st.session_state.df = df
                st.session_state.db_conn = None
                st.session_state.ai_assistant = None
                st.sidebar.success("✅ SQL file loaded successfully")
                st.sidebar.dataframe(df.head(), use_container_width=True)
            else:
                st.sidebar.error("❌ No tables found in the SQL file.")
            conn.close()
        except Exception as e:
            logger.error(f"Error processing SQL file: {str(e)}")
            st.sidebar.error(f"❌ Error processing SQL file: {e}")

elif data_source in ["Azure SQL", "Dataverse", "MySQL"]:
    st.sidebar.markdown("### 🔗 Enter Connection Details")
    if data_source == "MySQL":
        host = st.sidebar.text_input("MySQL Host (e.g., localhost):", value="localhost", key="mysql_host")
        port = st.sidebar.text_input("MySQL Port (default 3306):", value="3306", key="mysql_port")
        database = st.sidebar.text_input("Database:", value="sales_transactions", key="mysql_db")
        username = st.sidebar.text_input("Username:", value="root", key="mysql_user")
        password = st.sidebar.text_input("Password (optional):", type="password", key="mysql_pass")
        table_name = st.sidebar.text_input("Table Name:", value="transactions", key="mysql_table")
    else:
        server = st.sidebar.text_input("Server:", key="azure_server")
        database = st.sidebar.text_input("Database:", key="azure_db")
        username = st.sidebar.text_input("Username:", key="azure_user")
        password = st.sidebar.text_input("Password:", type="password", key="azure_pass")
        table_name = st.sidebar.text_input("Table Name:", key="azure_table")

    if st.sidebar.button("Connect", key="connect_button"):
        if data_source == "MySQL":
            if not all([host, database, username, table_name, port]):
                st.sidebar.error("Veuillez remplir tous les champs obligatoires.")
            else:
                try:
                    port = int(port)
                    st.session_state.connection_params = {
                        "host": host, "port": port, "database": database,
                        "username": username, "password": password, "table_name": table_name
                    }
                    engine = connect_to_mysql_sqlalchemy(host, database, username, password, port)
                    if engine and is_connection_valid(engine):
                        df = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 20", engine)
                        st.session_state.df = df
                        st.session_state.db_conn = engine
                        st.session_state.table_name = table_name
                        st.session_state.ai_assistant = None
                        st.sidebar.success("✅ Connexion MySQL établie et données chargées.")
                        st.sidebar.dataframe(df.head(), use_container_width=True)
                    else:
                        st.sidebar.error("❌ Échec de la connexion MySQL.")
                except ValueError:
                    st.sidebar.error("Le port doit être un nombre valide.")
                except Exception as e:
                    logger.error(f"Unexpected error during MySQL connection: {str(e)}")
                    st.sidebar.error(f"❌ Erreur inattendue : {str(e)}")
        else:
            if not all([server, database, username, password, table_name]):
                st.sidebar.error("Veuillez remplir tous les champs obligatoires.")
            else:
                st.session_state.connection_params = {
                    "server": server, "database": database, "username": username,
                    "password": password, "table_name": table_name
                }
                try:
                    conn = connect_to_database(server, database, username, password)
                    if is_connection_valid(conn):
                        query = f"SELECT TOP 20 * FROM {table_name}"
                        df = pd.read_sql(query, conn)
                        st.session_state.df = df
                        st.session_state.db_conn = conn
                        st.session_state.table_name = table_name
                        st.session_state.ai_assistant = None
                        st.sidebar.success("✅ Connexion établie et données chargées.")
                        st.sidebar.dataframe(df.head(), use_container_width=True)
                    else:
                        st.sidebar.error("❌ Échec de la connexion à la base de données.")
                except Exception as e:
                    logger.error(f"Azure SQL/Dataverse connection error: {str(e)}")
                    st.sidebar.error(f"❌ Error: {e}")

    if st.session_state.db_conn and is_connection_valid(st.session_state.db_conn):
        if st.sidebar.button("Disconnect", key="disconnect_button"):
            try:
                if isinstance(st.session_state.db_conn, pyodbc.Connection):
                    st.session_state.db_conn.close()
                st.session_state.db_conn = None
                st.session_state.connection_params = None
                st.session_state.df = None
                st.session_state.ai_assistant = None
                st.session_state.table_name = None
                st.sidebar.success("✅ Disconnected from database.")
            except Exception as e:
                logger.error(f"Error closing connection: {str(e)}")
                st.sidebar.error(f"❌ Error closing connection: {e}")
                st.session_state.db_conn = None

# Data information display
if st.session_state.df is not None:
    df = st.session_state.df
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📈 Dataset Information")
    col1, col2 = st.sidebar.columns(2)
    with col1:
        st.metric("📊 Rows", len(df))
    with col2:
        st.metric("📋 Columns", len(df.columns))
    with st.sidebar.expander("👁️ Data Preview", expanded=False):
        st.dataframe(df.head())
    with st.sidebar.expander("🔍 Column Details"):
        for col in df.columns:
            st.text(f"• {col}: {df[col].dtype}")
    if st.session_state.db_conn:
        st.sidebar.markdown("### 🔗 Connection Status")
        st.sidebar.markdown('<p class="status-connected">🟢 Connected to database</p>', unsafe_allow_html=True)
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 💡 Query Examples")
    with st.sidebar.expander("Examples"):
        st.markdown("""
        **Analysis Questions:**
        - Show me trends in popular courses
        - How many free vs paid courses?
        - Create a chart of average prices by category
        - What are the most popular subjects?
        - Show course duration distribution
        - What is the most common call type?
        
        **Data Structure:**
        - Show me column names
        - What's the dataset size?
        - Give me a data preview
        """)

# Main content area
col1, col2 = st.columns([1, 2])

# Chat Interface (Left Column)
with col1:
    st.markdown("## 🤖 AI Data Assistant")
    
    if st.session_state.df is not None and PANDASAI_AVAILABLE:
        # Prepare AI assistant
        if st.session_state.ai_assistant is None:
            try:
                with st.spinner("🔄 Initializing AI assistant..."):
                    st.session_state.ai_assistant = create_ai_assistant(st.session_state.df)
                if st.session_state.ai_assistant:
                    st.success("✅ AI assistant ready!")
            except Exception as e:
                st.error(f"Error initializing AI assistant: {e}")
        
        # FIXED CHAT CONTAINER - This is the key fix
        
        # Display welcome message if no chat history
        if not st.session_state.chat_messages:
            st.markdown("""
            <div class="welcome-message">
                <h3>🤖 Welcome to your AI Data Assistant!</h3>
                <p>Ask me anything about your data. I can create charts, analyze trends, and answer questions in natural language.</p>
            </div>
            """, unsafe_allow_html=True)
        
        # Display all chat messages within the fixed container
        for msg in st.session_state.chat_messages:
            if msg["role"] == "user":
                display_chat_message("user", msg["content"], msg.get("timestamp"))
            elif msg["role"] == "assistant":
                display_chat_message(
                    "assistant", 
                    msg["content"], 
                    msg.get("timestamp"),
                    msg.get("extra_content"),
                    msg.get("content_type")
                )
            elif msg["role"] == "error":
                display_chat_message("error", msg["content"], msg.get("timestamp"))
        
        # Close the chat container
        
        # Auto-scroll to bottom when new messages are added
        if st.session_state.chat_messages:
            auto_scroll_chat()
        
        # Chat input area - OUTSIDE the scrollable container
        st.markdown("---")
        
        # Use form to handle input properly
        with st.form(key="chat_form", clear_on_submit=True):
            col_input, col_send, col_clear = st.columns([4, 1.5, 1.5])
            
            with col_input:
                user_input = st.text_input(
                    "Type your question...",
                    placeholder="e.g., What is the most common call type?",
                    key="user_question",
                    label_visibility="collapsed"
                )
            
            with col_send:
                send_button = st.form_submit_button("Send 🚀", use_container_width=False)
            
            with col_clear:
                clear_button = st.form_submit_button("Clear 🗑️", use_container_width=False)
        
        # Handle chat input
        if send_button and user_input.strip() and st.session_state.ai_assistant and not st.session_state.processing_query:
            st.session_state.processing_query = True
            
            # Validate query
            is_valid, error_msg = is_meaningful_query(user_input)
            
            if not is_valid:
                # Add error message
                st.session_state.chat_messages.append({
                    "role": "error",
                    "content": error_msg,
                    "timestamp": datetime.now().strftime("%H:%M")
                })
            else:
                # Add user message
                st.session_state.chat_messages.append({
                    "role": "user",
                    "content": user_input,
                    "timestamp": datetime.now().strftime("%H:%M")
                })
                
                # Process query
                try:
                    # Check for special queries first
                    special_type, special_response = handle_special_queries(user_input, st.session_state.df)
                    
                    if special_type:
                        response_type = special_type
                        response = special_response
                    else:
                        # Use PandasAI
                        with st.spinner("🤔 Thinking..."):
                            response_type, response = execute_pandasai_query(st.session_state.ai_assistant, user_input)
                    
                    # Add assistant response
                    assistant_msg = {
                        "role": "assistant",
                        "timestamp": datetime.now().strftime("%H:%M"),
                        "content_type": response_type
                    }
                    
                    # Handle different response types
                    if response_type == "text":
                        assistant_msg["content"] = response
                    elif response_type == "dataframe":
                        assistant_msg["content"] = "Here's the data you requested:"
                        assistant_msg["extra_content"] = response
                    elif response_type == "chart":
                        assistant_msg["content"] = "I've created this visualization for you:"
                        assistant_msg["extra_content"] = response
                    else:
                        assistant_msg["content"] = str(response)
                    
                    st.session_state.chat_messages.append(assistant_msg)
                    
                except Exception as e:
                    # Add error message
                    st.session_state.chat_messages.append({
                        "role": "error",
                        "content": f"Error processing your request: {str(e)}",
                        "timestamp": datetime.now().strftime("%H:%M")
                    })
            
            st.session_state.processing_query = False
            st.rerun()
        
        # Clear chat
        if clear_button:
            st.session_state.chat_messages = []
            st.rerun()
    
    else:
        # No data or PandasAI not available
        st.markdown("""
        <div class="welcome-message">
            <h3>🔧 Setup Required</h3>
            <p>Please select and configure a data source from the sidebar to start chatting with your data.</p>
        </div>
        """, unsafe_allow_html=True)
        
        if not PANDASAI_AVAILABLE:
            st.error("❌ PandasAI is not installed. Install with: `pip install pandasai==2.1.0`")


# Power BI Dashboard (Right Column) 
with col2:
    st.markdown("## 📊 Power BI Dashboard")
    
    # Use Streamlit's container for better control
    with st.container():
        # Embed Power BI with fixed dimensions
        st.markdown("""
        <div style="
            width: 100%; 
            height: 600px; 
            margin: 0; 
            padding: 0;
            border-radius: 10px;
            overflow: hidden;
            box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1);
            border: 1px solid #e0e6ed;
            background: white;
        ">
            <iframe 
                title="Power BI Dashboard"
                width="100%" 
                height="100%"
                src="https://app.powerbi.com/view?r=eyJrIjoiOGE1MDQwOTQtMDI0Ni00ZmY4LTljNzktNzRmYWQ2MTQ0ODE3IiwidCI6ImRiZDY2NjRkLTRlYjktNDZlYi05OWQ4LTVjNDNiYTE1M2M2MSIsImMiOjl9"
                frameborder="0"
                allowFullScreen="true"
                style="
                    border: none; 
                    width: 100%; 
                    height: 100%;
                    display: block;
                    margin: 0;
                    padding: 0;
                ">
            </iframe>
        </div>
        """, unsafe_allow_html=True)
    
   