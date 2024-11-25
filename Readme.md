# PDF Parser
This project aims to read, parse the pdf data and output the data in a more structured format.

**Features**

- This project removes header/footer note.
- This also tries to extract table of content (toc) from the pdf.
- Has the ability to split into chapters/sections
- maintains relevant metadata as in the parsed output.

**Deployment**

There are 2 ways that this application can de tested / deployed.

- [Streamlit_Application](/src/app.py)
- [Fast API](/src/asgi.py)

**Usage**

- Please refer the usage python notebook to understand how to use the PDFExtractor Class.
- Link to the notebook is [here](/src/Usage.ipynb)
- Please create a python virutalenv and install the packages from the requirements.txt file

**Concept**

 - This project uses DBScan algorithm to remove the header / footer that is present in any pdf documents. 
 - Once the clusters are identified, we identify the headers using the metadata that is obtained from pymupdf and pymupdf4llm package. 
 - Further data processing is done to structurize the data with respect to the table of contents (toc) so that the unstructured data is obtained in structured json format.
 