# streamlit app file
import os 
import json
import tempfile

import streamlit as st
from pdf_extractor import PDFExtractor

def save_json(data, filename):
    """
    """
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)

def main():
    """
    """
    st.title("PDF Parser")

    if "processed_files" not in st.session_state:
        st.session_state.processed_files = []

    with st.form("upload-form", clear_on_submit=True):
        uploaded_files = st.file_uploader("Choose PDF files", type="pdf", accept_multiple_files=True)
        submitted = st.form_submit_button("Parse Pdfs")

    if uploaded_files and submitted:
        st.write("Files uploaded successfully")
        progress_bar = st.progress(0, "Parsing the PDF files")
        total_files = len(uploaded_files)

        for i, uploaded_file in enumerate(uploaded_files):
            if uploaded_file is not None:
                temp_dir = tempfile.gettempdir()
                temp_file_path = os.path.join(temp_dir, f"{uploaded_file.name}")
                with open(temp_file_path, "wb") as temp_file:
                    temp_file.write(uploaded_file.read())
            
                extractor = PDFExtractor(temp_file_path)
                data = extractor.extract_all_text_blocks(process_data=True, extract_tables=True, plot_cluster=False)

                output_filename = f"{os.path.splitext(uploaded_file.name)[0]}.json"
                temp_json_path = os.path.join(temp_dir, output_filename)
                save_json(data["processed_data"], temp_json_path)

                st.session_state.processed_files.append((temp_json_path, output_filename))
                st.success(f"Processed {uploaded_file}")
                progress_bar.progress((i+1) / total_files)
    
    if st.session_state.processed_files:
        st.subheader("Download Processed Files")
        for temp_json_file, output_filename in st.session_state.processed_files:
            with open(temp_json_file, "rb") as f:
                st.download_button(label=f"Download {output_filename}",
                                   data=f, 
                                   file_name=output_filename,
                                   mime="application/json")
    if st.button("Clear Content"):
        for temp_json_file, _ in st.session_state.processed_files:
            os.remove(temp_json_file)
        st.session_state.processed_files = []

        for temp_json_file, _ in st.session_state.processed_files:
            os.remove(temp_json_file)
        st.session_state.processed_files = []

if __name__ == "__main__":
    main()