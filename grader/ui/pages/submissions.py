import streamlit as st
from grader.client import GraderAPIClient
import asyncio
from datetime import datetime

st.set_page_config(
    page_title="Submissions",
    page_icon="📋",
    layout="centered"
)

st.header("Submitted Labs", divider="gray")

async def load_submissions():
    async with GraderAPIClient() as client:
        tasks = await client.list_tasks()
        return tasks.tasks

# Convert async function to sync for Streamlit
def get_submissions():
    return asyncio.run(load_submissions())

# Load and display submissions
submissions = get_submissions()

if submissions:
    # Create a DataFrame-like structure for the table
    data = []
    for task in submissions:
        data.append({
            "ID": str(task.id),
            "User ID": task.user_id,
            "Status": task.status,
            "Created": datetime.fromisoformat(task.created_at).strftime("%Y-%m-%d %H:%M:%S"),
            "Updated": datetime.fromisoformat(task.updated_at).strftime("%Y-%m-%d %H:%M:%S"),
            "Tag": task.tag or "N/A"
        })
    
    # Display the table
    st.dataframe(
        data,
        column_config={
            "ID": st.column_config.TextColumn("Task ID"),
            "User ID": st.column_config.TextColumn("Student"),
            "Status": st.column_config.TextColumn("Status"),
            "Created": st.column_config.DatetimeColumn("Created"),
            "Updated": st.column_config.DatetimeColumn("Last Updated"),
            "Tag": st.column_config.TextColumn("Tag")
        },
        hide_index=True,
        use_container_width=True
    )
else:
    st.info("No submissions found.") 