import os
import urllib.request as request
import zipfile
from textSummarizer.logging import logger
from textSummarizer.utils.common import get_size
from pathlib import Path
from textSummarizer.entitiy import DataIngestionConfig
from datasets import load_dataset

class DataIngestion:
    def __init__(self, config: DataIngestionConfig):
        self.config = config
    
    def download_file(self):
        if not os.path.exists(self.config.local_data_dir):
            logger.info(f"Downloading file from :[{self.config.source_URL}] to :[{self.config.local_data_dir}]")
            dataset = load_dataset("knkarthick/samsum")
            dataset.save_to_disk(str(self.config.local_data_dir))
            logger.info(f"File size :[{get_size(Path(self.config.local_data_dir))}]")
    
    def extract_zip_file(self):
        """
        zip_file_path: str
        Extracts the zip file into the data directory
        Function returns None
        """
        unzip_path = self.config.unzip_dir
        os.makedirs(unzip_path, exist_ok=True)
        with zipfile.ZipFile(self.config.local_data_dir, 'r') as zip_ref:
            zip_ref.extractall(unzip_path)
