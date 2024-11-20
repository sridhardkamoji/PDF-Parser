import asyncio
import json
import logging
import os
import re
import shutil
import uuid
from concurrent.futures import ProcessPoolExecutor
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import List

from fastapi import FastAPI, File, UploadFile, HTTPException, Request
from fastapi.responses import FileResponse

from pdf_extractor import PDFExtractor

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                    handlers=[logging.StreamHandler(), logging.FileHandler("app.log")],
)

current_path = Path(__file__).resolve()
parent_path = current_path.parent.parent

DATA_DIR = Path(parent_path / "data")
DATA_DIR.mkdir(exist_ok=True)

UPLOAD_DIR = Path(DATA_DIR / "uploads")
UPLOAD_DIR.mkdir(exist_ok=True)

JSON_DIR = Path(DATA_DIR / "json_files")
JSON_DIR.mkdir(exist_ok=True)

@dataclass
class Item:
    """
    dataclass to store task_id, pdf filepath and filename
    """

    task_id: uuid.UUID
    file_path: str
    fil_path_wo_extn: str

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    lifespan : code to run before the fast api app instantiation
    """

    q = asyncio.Queue()
    pool = ProcessPoolExecutor()
    asyncio.create_task(process_request(q,pool))
    yield {"q": q, "pool": pool}
    pool.shutdown()

task_statuses = {}
app = FastAPI(title="PDF_PARSER", version="1.0", lifespan=lifespan)

@app.get("/ping/")
async def ping():
    logging.info(msg = "ping to check the service")
    return {"message": "Ping Successful"}

async def copy_pdf_file(file):
    """
    async copy of pdf files
    """

    file_path = UPLOAD_DIR / file.filename
    with file_path.open("wb") as f:
        await asyncio.to_thread(shutil.copyfileobj, file.file, f)

async def copy_file_tasks(files):
    """
    """
    tasks = []

    for file in files:
        if file.content_type != "application/pdf":
            logging.warning(f"{file} is not a valid pdf. Cannot Process file")
            continue
            
        filename = file.filename
        logging.info(f"filename :: {filename}, content :: {file.content_type}")

        if file.content_type == "application/pdf":
            tasks.append(copy_pdf_file(file))
    await asyncio.gather(*tasks)

def save_json(file_name:str, data:dict):
    """
    """
    json_path = JSON_DIR / f"{file_name}.json"

    with json_path.open("w") as json_file:
        json.dump(data, json_file, indent=4)

    return json_path

def process_pdf_extraction_task(item:Item, process_data: bool= True, extract_table: bool = True):
    """
    """
    logging.info("running the task in process pool")
    try:
        task_id = item.task_id
        pdf_file_path = item.file_path
        json_file_name = item.fil_path_wo_extn
        logging.info(f"Item details :: {item}")
        logging.info(f"file_path :: {str(item.file_path)}")

        task_statuses[task_id] = {"status" : "processing"}
        # logging.info(task_statuses[task_id])

        extractor = PDFExtractor(str(pdf_file_path))
        data = extractor.extract_all_text_blocks(process_data=process_data, 
                                                 plot_cluster=False, 
                                                 extract_tables=extract_table
                                                 )
        logging.info(f"data :: {data.keys()}")
        json_path = save_json(json_file_name, data["processed_data"])

        logging.info(f"data :: {data.keys()}")
        json_path = save_json(json_file_name, data["processed_data"])

        logging.info("changing the status!!!")

        task_statuses[task_id] = {
            "status" : "completed",
            "download_url": {"filename" : json_file_name, "download_url": f"/download/{json_path.name}"},
        }

        logging.info(f"task_id :: {task_id} :: task_status :: {task_statuses[task_id]}")

    except Exception as e:
        task_statuses[task_id] = {"status" : f"failed : {str(e)}"}
    
    return task_id, task_statuses[task_id]

async def process_request(q:asyncio.Queue, pool:ProcessPoolExecutor):
    """
    run the process in the pool
    """
    while True:
        item = await q.get()
        loop = asyncio.get_running_loop()
        task_statuses[item.task_id] = {"status" : "sending the task to process pool"}
        result = await loop.run_in_executor(pool, process_pdf_extraction_task, item)
        q.task_done()
        logging.info(f"result :: {result}")
        task_statuses[result[0]] = result[1]

@app.post("/parsepdf/")
async def parse_pdf(request : Request, files : List[UploadFile] = File(...)):
    """
    """
    await copy_file_tasks(files)

    task_results = []
    for file in files:
        if file.content_type == "application/pdf":
            task_id = uuid.uuid4()
            task_statuses[task_id] = {"status" : "pending"}

            file_path = UPLOAD_DIR / file.filename
            task_results.append({"task_id" : task_id, "file_name": file.filename})

            filename_wo_ext = file.filename.replace(".pdf", "")
            filename_wo_ext = re.sub(r"\s+", " ", filename_wo_ext)
            filename_wo_ext = re.sub(r"\s", "_", filename_wo_ext)
            item = Item(task_id=task_id, file_path=file_path, fil_path_wo_extn=filename_wo_ext)

            request.state.q.put_nowait(item)

            # background_tasks.add_task(process_pdf_extraction_task, file_path, task_id, filename_wo_ext)

    return {"tasks": task_results}

def is_valid_uuid(input_string: str) -> bool:
    """
    """
    try:
        uuid_obj = uuid.UUID(input_string)
        return str(uuid_obj) == input_string
    except ValueError:
        return False
    
@app.get("/task_status/{task_id}/")
async def get_task_status(task_id: str):
    """
    """
    if is_valid_uuid(task_id):
        status = task_statuses.get(uuid.UUID(task_id), {"status": "Task not found"})
    else:
        status = {"status": "invalid task id"}
    
    return status

@app.get("/download/{filename}")
async def download_parsed_files(file_name:str):
    """
    """
    json_path = JSON_DIR / file_name
    if not json_path.exists():
        return HTTPException(status_code=404, detail=f"File {file_name} not found")
    
    return FileResponse(json_path, media_type="application/json", filename=file_name)

def delete_all_files(directory):
    """
    """
    if os.path.exists(directory):
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print(f"failed to delete {file_path}. Exception Occurred: {str(e)}")
        return True
    else:
        print(f"Directory {directory} does NOT exist")

        return False
    
@app.get("/cleanup_files/")
async def delete_files():
    """
    """
    upload_dir_del_success = delete_all_files(UPLOAD_DIR)
    json_dir_del_success = delete_all_files(JSON_DIR)

    if upload_dir_del_success and json_dir_del_success:
        return {"status": "Cleanup Successful"}
    
    else:
        return {
            "status" : f"""PDF File directory cleanup : {upload_dir_del_success} \n\n 
            Processed json file directory cleanup : {json_dir_del_success}"""
        }

