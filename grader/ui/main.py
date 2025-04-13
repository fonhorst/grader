import streamlit as st
from streamlit.logger import get_logger

logger = get_logger(__name__)

st.set_page_config(
    page_title="Grader UI",
    page_icon="💻",
    layout="centered",
    initial_sidebar_state="expanded",
    menu_items={
        "About": "Инструмент для автоматической проверки лабораторных работ",
    }
)

st.header(
    body=" Grader UI\n##### :gray[_Запуски._]",
    divider='gray'
)
