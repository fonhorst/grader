import logging
import os

import streamlit as st
from streamlit.logger import get_logger
from presscore.ui.utils import ABOUT_THIS_PROJECT

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)8s] %(name)s (%(filename)s:%(lineno)s) %(message)s"
)

logger = get_logger(__name__)

st.set_page_config(
    page_title="PresScore - инструмент для оценки качества презентаций",
    page_icon="💻",
    layout="centered",
    initial_sidebar_state="expanded",
    menu_items={
        "About": ABOUT_THIS_PROJECT,
    }
)

st.header(
    body=" PresScore\n##### :gray[_Ваш гуру по презентациям._]",
    divider='gray'
)

st.sidebar.success("Select a demo above.")

st.markdown(
    """
    PresScore - позволяет оценить качество презентаций по 
    целому ряду критериев, начиная от понимания общей идеи и 
    уровня проработки контента до конкретных деталей и визуализаций.
    
    Чтобы начать работу с PresScore, необходимо убедиться, что ваш ключ от OpenAI API доступен как 
    системная переменная 'OPENAI_API_KEY'.
""")

st.markdown("""**👈 Выберите опцию из меню** для того, чтобы проверить свою презентацию.""")
