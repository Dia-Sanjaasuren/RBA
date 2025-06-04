import streamlit as st

st.set_page_config(page_title="Documentation", layout="wide")
st.title("Documentation / Questions")

st.write("""
Use this page to manually enter and view your project questions. You can add new questions below. All entries are stored only for the current session.
""")

if 'question_list' not in st.session_state:
    st.session_state['question_list'] = []
if 'question_comments' not in st.session_state:
    st.session_state['question_comments'] = []

# Ensure question_comments matches question_list length
if len(st.session_state['question_comments']) < len(st.session_state['question_list']):
    st.session_state['question_comments'].extend([[] for _ in range(len(st.session_state['question_list']) - len(st.session_state['question_comments']))])
elif len(st.session_state['question_comments']) > len(st.session_state['question_list']):
    st.session_state['question_comments'] = st.session_state['question_comments'][:len(st.session_state['question_list'])]

with st.form("add_question_form"):
    question = st.text_area("Question", key="question_text")
    submitted = st.form_submit_button("Add Question")
    if submitted and question.strip():
        st.session_state['question_list'].append(question.strip())
        st.session_state['question_comments'].append([])  # Add empty comment list for this question
        st.success("Question added!")
        st.rerun()

st.write("## Questions List")
if st.session_state['question_list']:
    for idx, q in enumerate(st.session_state['question_list']):
        col1, col2 = st.columns([8, 1])
        with col1:
            st.markdown(f"**Q{idx+1}:** {q}")
        with col2:
            if st.button("🗑️", key=f"delete_{idx}"):
                st.session_state['question_list'].pop(idx)
                st.session_state['question_comments'].pop(idx)
                st.rerun()
        # Comments for this question
        with st.form(f"add_comment_form_{idx}", clear_on_submit=True):
            comment = st.text_area("Add a comment or update", key=f"comment_text_{idx}")
            comment_submitted = st.form_submit_button("Add Comment", use_container_width=True)
            if comment_submitted and comment.strip():
                st.session_state['question_comments'][idx].append(comment.strip())
                st.success("Comment added!")
                st.rerun()
        # Show comments for this question
        if st.session_state['question_comments'][idx]:
            for cidx, c in enumerate(st.session_state['question_comments'][idx], 1):
                st.markdown(f"- {c}")
        st.markdown("---")
else:
    st.info("No questions yet. Add your first question above.") 