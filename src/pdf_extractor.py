import itertools
import os
from itertools import islice
from typing import List

import fitz
import matplotlib.pyplot as plt
import pandas as pd
import pymupdf4llm
from matplotlib.patches import Rectangle
from pdf_cluster import PDFTextBlockCategorizer
from tqdm import tqdm

class PDFExtractor:
    """
    PDF parser to read the pdf and parse the data into more structured format
    :: Args ::
        Param :: pdf_path :: file path of the pdf
    
    """
    def __init__(self,pdf_path:str = ""):
        if pdf_path != "":
            self.pdf_filename = os.path.basename(pdf_path)
        else:
            raise Exception(f"Please provide a valid PDF path. Giveen path :: {pdf_path}")
        
        extension = self.pdf_filename.split(".")[-1]
        if extension != "pdf":
            raise Exception(
                f"Invalid PDF extension :: expected extension :: ***.pdf :: {self.pdf_filename} has extension {extension}"
                )
        
        self.pdf_path = pdf_path
        self.pdf_doc = fitz.open(os.path.abspath(self.pdf_path))
        self.headers = pymupdf4llm.IdentifyHeaders(self.pdf_doc)
        if self.headers == {}:
            print(f"Headers and TOC cannot be parsed for the document {self.pdf_filename}.\n Processing pagewise data only")

    def retrieve_text_from_lines(self, lines:List) -> List:
        """retrieves text from span object of pymupdf"""

        return ["".join([span["text"] for span in line["spans"]]) for line in lines]
    
    def get_header4block(self, blk:dict)-> dict:
        """
        retrieves headers from the span object of pymupdf
        """
        header_lst = []
        if "lines" in blk:
            lines = blk["lines"]
            header_lst = [list(set(self.headers.get_header_id(span) for span in line["spans"])) for line in lines]

        blk["header_lst"] = header_lst
        return blk
    
    def calc_rect_center(self, rect:tuple, reverse_y: bool=False) -> tuple:
        """
        given the coordinates, identify the center of the rectangle
        """
        if reverse_y:
            x0, y0, x1, y1 = rect[0], -rect[1], rect[2], -rect[3]
        else:
            x0, y0, x1, y1 = rect

        x_center = (x0+x1) / 2
        y_center = (y0+y1) / 2

        return (x_center, y_center)
    
    def get_tag_txt_dct(self, rec)-> list[dict]:
        """
        get the text list and its corresponding markdown tag as dict
        """
        lst = rec["text_lst"]
        tag_lst = rec["header_tag"]
        return [{"txt": txt, "tag": tag} for txt, tag in zip(lst, tag_lst)]
    
    def extract_all_text_blocks(
            self, process_data:bool=True, plot_cluster:bool = False, extract_tables:bool=True
            ) -> dict:
        """
        extract and parse the data from pdf
        """
        dicts_df_lst = []
        if extract_tables:
            self.tables_dict_lst = []
        
        for page_idx, page in tqdm(islice(enumerate(self.pdf_doc), len(self.pdf_doc)), total=len(self.pdf_doc)):
            page_cnt = page_idx + 1

            if extract_tables:
                table_lst = page.find_tables()
                table_lst = [tbl.to_markdown(clean=False) for tbl in table_lst]
                self.tables_dict_lst.append({"page": page_cnt, "tables": table_lst})

            dicts = page.get_text(option="dict")
            dicts = [self.get_header4block(blk) for blk in dicts["blocks"]]
            dicts_df = pd.DataFrame(dicts)
            dicts_df["pg_blk"] = [str(page_cnt) + "." + str(num) for num in dicts_df["number"]]
            dicts_df["text_lst"] = [
                self.retrieve_text_from_lines(lines) if blk_type == 0 else ""
                for blk_type, lines in zip(dicts_df["type"], dicts_df["lines"])
            ]
            dicts_df = dicts_df[dicts_df["type"] == 0].reset_index(drop = True)
            req_cols = ["number", "type", "bbox", "lines", "header_lst", "pg_blk", "text_lst"]
            dicts_df = dicts_df[req_cols]
            dicts_df_lst.append(dicts_df)

        data = pd.concat(dicts_df_lst)

        categorize_vectors = data[["bbox", "text_lst", "pg_blk", "header_lst"]].values.tolist()
        categorizer = PDFTextBlockCategorizer(categorize_vectors)
        categorizer.run()
        del data
        self.pdf_data = pd.concat(
            [
                pd.DataFrame(categorizer.blocks, columns=["bbox", "text_lst", "pg_blk", "header_tag"]),
                pd.DataFrame(categorizer.labels, columns=["cluster"]),

            ],
            axis=1
            )
        
        self.pdf_data["rect_center"] = [self.calc_rect_center(bbox, reverse_y=True) for bbox in self.pdf_data["bbox"]]
        self.pdf_data["page"] = [pg_blk.split(".")[0] for pg_blk in self.pdf_data["pg_blk"]]
        self.pdf_data["page"] = self.pdf_data["page"].astype(int)
        self.pdf_data["header_footer"] = [True if cluster == 1 else False for cluster in self.pdf_data["cluster"]]
        self.pdf_data["text_and_tag"] = self.pdf_data.apply(self.get_tag_txt_dct, 1)
        
        self.header_content_dict = []
        self.toc = pd.DataFrame(None)
        if process_data and self.headers.header_id != {}:
            self.expl_data = self.pdf_data[self.pdf_data["cluster"] == 0].copy()
            self.expl_data = self.expl_data.explode("text_and_tag")
            self.expl_data.reset_index(drop = True, inplace = True)

            ind_to_drop = []
            for ind, rec in self.expl_data.iterrows():
                if rec["text_and_tag"]["txt"].strip() == "":
                    ind_to_drop.append(ind)
            
            self.expl_data.drop(index = ind_to_drop, inplace = True)
            self.expl_data.reset_index(drop = True, inplace =True)

            self.expl_data["text"] = [d["txt"] for d in self.expl_data["text_and_tag"]]
            self.expl_data["header_tag"] = [d["tag"] for d in self.expl_data["text_and_tag"]]

            self.expl_data["header_tag"] = ["".join(lst) for lst in self.expl_data["header_tag"]]

            h_tags = self.expl_data["header_tag"].value_counts().index.tolist()
            h_tags = [tag for tag in h_tags if tag != ""]
            h_tags.sort()
            
            if len(h_tags) > 6:
                h_tags = h_tags[:6]
            
            h_tags_map = {ele: f"h{ele.count("#")}" for ele in h_tags}
            h_tags_map = {key: val for key, val in h_tags_map.items() if int(val[1:]) <= 6}

            self.expl_data["header_tag_md"] = self.expl_data["header_tag"].map(h_tags_map)

            self.toc = self.expl_data[(self.expl_data["header_tag"] != "") & (self.expl_data["cluster"] == 0)][
                ["page", "text", "header_tag_md"]
            ]

            self.expl_data.reset_index(drop = True, inplace=True)

            header_df = self.expl_data.dropna(subset="header_tag_md")[["header_tag", "header_tag_md", "text"]].copy()
            header_df.reset_index(drop=False , inplace=True)
            header_df["start_index"] = header_df["index"] + 1
            
            header_df["end_index"] = header_df["index"].shift(-1)
            header_df["end_index"].fillna(self.expl_data.shape[0], inplace=True)

            if header_df.iloc[0]["start_index"] != 0:
                init_header_df = pd.DataFrame(
                    [[0, "no_tag", "h0", "**no_header**", 0, header_df.iloc[0]["start_index"] - 1]],
                    columns=header_df.columns,
                )
                header_df = pd.concat([init_header_df, header_df])

            header_df.reset_index(drop = True, inplace = True)

            header_df["end_index"] = header_df["end_index"].astype(int)

            for ind, rec in header_df.iterrows():
                sub_dt = self.expl_data.iloc[rec["start_index"] : rec["end_index"]].copy()
                content = "\n".join(sub_dt["text"].values.tolist())
                self.header_content_dict.append(
                    {
                        "title" : rec["text"],
                        "page_nos": list(dict.fromkeys(sub_dt["page"].values.tolist()).keys()),
                        "content": content,
                    }
                )
        
        if self.header_content_dict == []:
            grp_dt = pd.DataFrame(
                self.pdf_data[self.pdf_data["cluster"] == 0].groupby("page", as_index = False)["text_lst"].apply(list)
            )
            grp_dt["content"] = ["\n".join(itertools.chain.from_iterable(lst)) for lst in grp_dt["text_lst"]]
            grp_dt["title"] = "Page: " + grp_dt["page"].astype(str)
            grp_dt["page_nos"] = [[pg] for pg in grp_dt["page"]]
            self.header_content_dict = grp_dt[["title", "page_nos", "content"]].to_dict(orient = "records")

        if len(self.tables_dict_lst) > 0:
            self.header_content_df = pd.DataFrame(self.header_content_dict)
            self.table_df = pd.DataFrame(self.tables_dict_lst)

            self.header_content_df.reset_index(drop = True, inplace=True)
            self.header_content_df.reset_index(drop = False, inplace=True)

            self.table_df["table_exist"] = [True if len(lst) > 0 else False for lst in self.table_df["tables"]]
            self.table_df = self.table_df[self.table_df["table_exist"]].copy()

            page_nos = self.table_df["page"].values.tolist()

            proc_data_sub = self.header_content_df[["index", "page_nos"]].copy()
            proc_data_sub = proc_data_sub.explode("page_nos")
            proc_data_sub.fillna("", inplace=True)

            ind_pos_df = (
                proc_data_sub[proc_data_sub["page_nos"].isin(page_nos)]
                .groupby("page_nos", as_index=False)["index"]
                .max()
            )
            
            ind_pos_df.rename(columns = {"page_nos": "page"}, inplace = True)
            self.table_df = pd.merge(self.table_df, ind_pos_df, on = "page", how = "left")
            self.table_df["index"] = self.table_df["index"] + 0.1
            self.table_df["title"] = "<table>"
            self.table_df["page"] = [[ele] for ele in self.table_df["page"]]
            self.table_df.rename(columns={"page": "page_nos", "tables" : "content"}, inplace = True)
            self.table_df = self.table_df.explode("content")[self.header_content_df.columns.values.tolist()]
            self.header_content_df_with_tables = pd.concat([self.header_content_df, self.table_df])
            self.header_content_df_with_tables.sort_values("index", inplace = True)
            self.header_content_dict = self.header_content_df_with_tables[["title", "page_nos", "content"]].to_dict(orient= "records")

        if plot_cluster:
            fig, ax = plt.subplots()
            colors = list("brgcmyk")

            for i, rec in self.pdf_data[["bbox", "rect_center", "pg_blk"]].iterrows():
                label_idx = categorizer.labels[i]
                color = colors[label_idx]

                x0,y0,x1,y1 = rec["bbox"][0], rec["bbox"][1], rec["bbox"][2], rec["bbox"][3]
                rect = Rectangle((x0, -y0), x1-x0, -y1+y0, fill=False, edgecolor=color)
                ax.add_patch(rect)
                x, y = rec["rect_center"][0], rec["rect_center"][1]
                plt.scatter(x,y, color=color)
                plt.annotate(rec["pg_blk"], rec["rect_center"])
            plt.show()

        return {
            "raw_data": {
                "document_name":self.pdf_filename,
                "local_doc_path": os.path.abspath(self.pdf_path),
                "sections": self.pdf_data.to_dict(orient="records"),
            },
            "processed_data": {
                "document_name":self.pdf_filename,
                "local_doc_path": os.path.abspath(self.pdf_path),
                "table_of_contentx_(toc)": self.toc.to_dict(orient="records"),
                "tables": self.tables_dict_lst,
                "sections": self.header_content_dict
            }
        }






            
        



