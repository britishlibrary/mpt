import csv
import multiprocessing
import os
from datetime import datetime

from tqdm import tqdm

from mpt.defaults import base_output_dir
from mpt.email import send_email
from mpt.filemanager import scan_tree


def split_all(path):
    allparts = []
    while 1:
        parts = os.path.split(path)
        if parts[0] == path: # sentinel for absolute paths
            allparts.insert(0, parts[0])
            break
        elif parts[1] == path: # sentinel for relative paths
            allparts.insert(0, parts[1])
        else:
            path = parts[0]
            allparts.insert(0, parts[1])
    return allparts


def convert_time_string(time_string: str):
    split_time = time_string.split(":")
    seconds = (int(split_time[0]) * 3600) + (int(split_time[1]) * 60) + int(split_time[2])
    return seconds


class ReportCollator:
    num_procs = None
    base_path = None
    date_start = None
    date_end = None
    out_file = None
    email_recipients = None
    results = []
    results_sorted = {}

    def __init__(self,
                 base_path: str = None,
                 date_start: str = None,
                 date_end: str = None,
                 out_file: str = None,
                 email_recipients: list = None):
        if base_path is None:
            self.base_path = base_output_dir
        else:
            self.base_path = base_path
        if date_start is None:
            self.date_start = datetime(1970, 1, 1, 0, 0, 0)
        else:
            self.date_start = datetime.strptime(date_start, "yyyymmdd")
        if date_end is None:
            self.date_end = datetime.now()
        else:
            self.date_end = datetime.strptime(date_end, "yyyymmdd")
        self.out_file = out_file
        self.email_recipients = email_recipients
        try:
            self.num_procs = int(os.environ["NUMBER_OF_PROCESSORS"])
        except KeyError:
            self.num_procs = 2
            pass

    def _parse_file(self, file_path: str):
        result = {
            "datetime": None,
            "hostname": None,
            "action": None,
            "path": None,
            "time_taken": None,
            "total_files": None,
            "status": []
        }
        path_parts = split_all(file_path)
        result["datetime"] = datetime.strptime(path_parts[-2], "%Y-%m-%dT%H%M")
        skip = False
        if self.date_start <= result["datetime"] <= self.date_end:
            with open(file_path, 'r') as in_file:
                next_line = in_file.readline().strip()
                result["hostname"] = next_line.split(" ")[-1]
                for next_line in in_file.readlines():
                    next_line = next_line.strip()
                    if next_line != "":
                        if "results" in next_line:
                            result["action"], result["path"] = next_line.split(" results for ")
                            if result["action"] == "File staging":
                                return None
                        else:
                            if ": " in next_line:
                                if "ongoing" in next_line:
                                    skip = True
                                elif "reports created" in next_line:
                                    pass
                                elif "Time taken" in next_line:
                                    time_str = next_line.split(": ")[-1]
                                    result["time_taken"] = convert_time_string(time_str)
                                else:
                                    file_status, file_results = next_line.split(": ")
                                    status = {
                                        "file_status": file_status,
                                        "file_count": None,
                                        "file_size": None
                                    }
                                    if "(" in file_results:
                                        file_count, file_size = file_results.split(" (")
                                        file_size = file_size.replace(")","")
                                        status["file_count"] = int(file_count.replace(",",""))
                                        status["file_size"] = file_size
                                    else:
                                        status["file_count"] = int(file_results.replace(",",""))
                                    result["status"].append(status)
        else:
            skip = True
        if not skip:
            result["total_files"] = sum([n["file_count"] for n in result["status"]])
            return result

    def _write_report(self):
        cols = ["datetime", "hostname", "action", "path",
                "time_taken", "total_files", "file_status", "file_count", "file_size"
                ]
        if self.out_file is not None:
            with open(self.out_file, "w+", encoding="utf-8", newline="") as o:
                dw = csv.DictWriter(o, fieldnames=cols)
                dw.writeheader()
                for item in self.results:
                    new_row = {k:v for k, v in item.items() if k != "status"}
                    for status in item["status"]:
                        new_row["file_status"] = status["file_status"]
                        new_row["file_count"] = status["file_count"]
                        new_row["file_size"] = status["file_size"]
                        dw.writerow(new_row)
        return self.out_file

    def _email_report(self):
        server = self.results[0]["hostname"]
        subject = "Collated MPT statistics for {}".format(server)
        message = "Minimum Preservation Tool (MPT): collated reports for host {host}\n\n" \
            "Attached are the collated statistics " \
            "for the period {start} - {end}".format(host=server,
                                                    start=self.date_start.strftime("%Y-%m-%d)"),
                                                    end=self.date_end.strftime("%Y-%m-%d"))
        send_email(subject=subject, recipients=self.email_recipients, message=message, attachments=[self.out_file])

    def start(self):
        files_iterable = scan_tree(path=self.base_path, recursive=True, formats="summary.txt")
        pool = multiprocessing.Pool(processes=self.num_procs)
        for result in tqdm(pool.imap_unordered(self._parse_file, files_iterable),
                           desc="MPTReport({}p)/Collating summaries".format(self.num_procs)):
            if result is not None:
                self.results.append(result)
        self._write_report()
        if self.email_recipients is not None:
            self._email_report()
