from collections import Counter
from typing import List

import numpy as np
from sklearn.cluster import DBSCAN


class PDFTextBlockCategorizer:
    """
    This class takes text blocks which are obtained from pdf using pymupdf (fitz) package and runs DBscan algo.
    This separates header and footer note which appears in pdf docs
    This code is used from the following discussion on github page with some modifications:
    https://github.com/pymupdf/PyMuPDF/discussions/2259#discussioncomment-6669190
    """

    def __init__(self, blocks:List) :
        self.blocks = blocks

    def run(self):
        """
        Run clustering on text blocks
        """
        X = np.array(
            [
                (rect[0], rect[1], rect[2], rect[3], len("\n".join(text_lst) + "\n"))
                for rect, text_lst, pg_blk, header_tag in self.blocks
            ]
        )

        dbscan = DBSCAN()
        dbscan.fit(X)
        labels = dbscan.labels_
        self.n_clusters = len(np.unique(labels))
        label_counter = Counter(labels)
        most_common_label = label_counter.most_common(1)[0][0]
        labels = [0 if label == most_common_label else 1 for label in labels]
        self.labels = labels

        print(f"{self.n_clusters} clusters for {len(self.blocks)} blocks")