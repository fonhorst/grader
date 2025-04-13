import streamlit as st
from grader.client import GraderAPIClient
import asyncio
from datetime import datetime, timedelta
from typing import Optional

st.set_page_config(
    page_title="Submissions",
    page_icon="📋",
    layout="centered"
)

st.header("Submitted Labs", divider="gray")

# Initialize session state for filters if not exists
if 'filters' not in st.session_state:
    st.session_state.filters = {
        'status': 'All',
        'user_id': '',
        'tag': '',
        'date_from': datetime.now() - timedelta(days=30),
        'date_to': datetime.now()
    }

# Add filter controls in the sidebar
with st.sidebar:
    st.header("Filters")
    
    # Status filter
    status_options = ["All", "created", "running", "finished", "failed", "cancelled"]
    selected_status = st.selectbox(
        "Status",
        status_options,
        index=status_options.index(st.session_state.filters['status'])
    )
    
    # User ID filter
    user_id = st.text_input(
        "User ID",
        value=st.session_state.filters['user_id'],
        placeholder="Filter by user ID"
    )
    
    # Tag filter
    tag = st.text_input(
        "Tag",
        value=st.session_state.filters['tag'],
        placeholder="Filter by tag"
    )
    
    # Date range filter
    col1, col2 = st.columns(2)
    with col1:
        date_from = st.date_input(
            "From",
            value=st.session_state.filters['date_from'].date()
        )
    with col2:
        date_to = st.date_input(
            "To",
            value=st.session_state.filters['date_to'].date()
        )
    
    # Add refresh button
    if st.button("Apply Filters", type="primary"):
        st.session_state.filters.update({
            'status': selected_status,
            'user_id': user_id,
            'tag': tag,
            'date_from': datetime.combine(date_from, datetime.min.time()),
            'date_to': datetime.combine(date_to, datetime.max.time())
        })
        st.rerun()

async def load_submissions(
    status: Optional[str] = None,
    user_id: Optional[str] = None,
    tag: Optional[str] = None
) -> list:
    async with GraderAPIClient() as client:
        tasks = await client.list_tasks(
            user_id=user_id if user_id else None,
            tag=tag if tag else None,
            status=status if status != "All" else None
        )
        return tasks.tasks

# Convert async function to sync for Streamlit
def get_submissions(
    status: Optional[str] = None,
    user_id: Optional[str] = None,
    tag: Optional[str] = None
):
    return asyncio.run(load_submissions(status, user_id, tag))

# Load and display submissions with filters
submissions = get_submissions(
    status=st.session_state.filters['status'] if st.session_state.filters['status'] != "All" else None,
    user_id=st.session_state.filters['user_id'] if st.session_state.filters['user_id'] else None,
    tag=st.session_state.filters['tag'] if st.session_state.filters['tag'] else None
)

# Filter by date range
if submissions:
    filtered_submissions = []
    for task in submissions:
        created_at = datetime.fromisoformat(task.created_at)
        if st.session_state.filters['date_from'] <= created_at <= st.session_state.filters['date_to']:
            filtered_submissions.append(task)
    submissions = filtered_submissions

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
    
    # Display count of filtered results
    st.caption(f"Showing {len(submissions)} submissions")
else:
    st.info("No submissions found matching the selected filters.") 