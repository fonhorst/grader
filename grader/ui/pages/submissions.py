import streamlit as st
from grader.client.grader import GraderAPIClient
import asyncio
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import os
from hdfs import InsecureClient
import uuid
import tempfile

from grader.schemes import TaskSubmitRequest

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

# Define checker types and their parameters
# TODO: replace with reflection
# TODO: some parameters may be autowired by the grader itself. they should be marked as such.
CHECKER_TYPES = {
    "ClickHouse": {
        "host": {"type": "text", "default": "localhost"},
        "user": {"type": "text", "default": "admin"},
        "password": {"type": "text", "default": None, "password": True},
        "student_username": {"type": "text", "default": None},
        "cluster_name": {"type": "text", "default": "main_cluster"}
    },
    "Spark": {
        "script_path": {"type": "text", "default": None},
        "input_data_path": {"type": "text", "default": None},
        "gold_data_path": {"type": "text", "default": None},
        "output_dir": {"type": "text", "default": None},
        "timeout": {"type": "number", "default": 30},
        "include_logs": {"type": "boolean", "default": True},
        "hdfs_host": {"type": "text", "default": None},
        "hdfs_port": {"type": "number", "default": None}
    },
    "HDFS": {
        "hdfs_url": {"type": "text", "default": None},
        "base_dir": {"type": "text", "default": None},
        "process_start_delay": {"type": "number", "default": 2},
        "file_write_interval": {"type": "number", "default": 5},
        "final_wait_time": {"type": "number", "default": 5},
        "etl_duration": {"type": "number", "default": 30},
        "etl_check_interval": {"type": "number", "default": 5}
    },
    "Kubernetes": {
        "namespace": {"type": "text", "default": "default"}
    }
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

# Add submit button in the center
col1, col2, col3 = st.columns([1, 2, 1])
with col2:
    if st.button("Submit New Lab", type="primary"):
        st.session_state.show_submit_modal = True

# Show submit modal if triggered
if st.session_state.get('show_submit_modal', False):
    with st.form("submit_form"):
        st.subheader("Submit New Lab")
        
        # Select checker type
        checker_type = st.selectbox(
            "Lab Type",
            options=list(CHECKER_TYPES.keys())
        )
        
        # Get parameters for selected checker
        params = CHECKER_TYPES[checker_type]
        form_data = {}
        
        # Create form fields based on checker parameters
        for param_name, param_config in params.items():
            if param_config["type"] == "text":
                form_data[param_name] = st.text_input(
                    param_name.replace("_", " ").title(),
                    value=param_config["default"],
                    type="password" if param_config.get("password", False) else "default"
                )
            elif param_config["type"] == "number":
                form_data[param_name] = st.number_input(
                    param_name.replace("_", " ").title(),
                    value=param_config["default"]
                )
            elif param_config["type"] == "boolean":
                form_data[param_name] = st.checkbox(
                    param_name.replace("_", " ").title(),
                    value=param_config["default"]
                )
        
        # File upload
        uploaded_file = st.file_uploader("Upload Lab File", type=None)
        
        # Form submission buttons
        col1, col2 = st.columns(2)
        with col1:
            submit = st.form_submit_button("Submit")
        with col2:
            if st.form_submit_button("Cancel"):
                st.session_state.show_submit_modal = False
                st.rerun()
        
        if submit and uploaded_file:
            try:
                # Initialize HDFS client
                hdfs_client = InsecureClient(
                    f"http://{os.environ['HDFS_HOST']}:{os.environ['HDFS_PORT']}",
                    user=os.environ.get('HDFS_USER', 'hadoop')
                )
                
                # Generate unique filename
                file_ext = os.path.splitext(uploaded_file.name)[1]
                unique_filename = f"{uuid.uuid4()}{file_ext}"
                hdfs_path = f"{os.environ['HDFS_SUBMISSIONS_DIR']}/{unique_filename}"
                
                # Save file to temporary location
                with tempfile.NamedTemporaryFile() as tmp_file:
                    tmp_file.write(uploaded_file.getvalue())
                    tmp_file_path = tmp_file.name
                
                    # Upload to HDFS
                    hdfs_client.upload(hdfs_path, tmp_file_path)
                
                # Update form data with HDFS path
                form_data["attachment"] = hdfs_path
                
                # Submit task using API client
                async def submit_task():
                    async with GraderAPIClient() as client:
                        return await client.submit_task(
                            request=TaskSubmitRequest(
                                student_id=form_data.get("student_username", "default_user"),
                                tag=checker_type,
                                **form_data
                            )
                        )
                
                # Submit the task
                task = asyncio.run(submit_task())
                
                st.success(f"Lab submitted successfully! Task ID: {task.id}")
                st.session_state.show_submit_modal = False
                st.rerun()
                
            except Exception as e:
                st.error(f"Error submitting lab: {str(e)}")
        elif submit:
            st.error("Please upload a file before submitting.") 