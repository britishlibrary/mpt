import csv
import os
from datetime import datetime

from .codes import (Action, ComparisonResult, CreationResult, Result,
                    ValidationResult)


class Report:
    """
    The Report class, representing a single report file created for an MPT run
    """
    io_handler = None
    csv_handler = None

    def __init__(self, path: str, columns: list):
        """
        Initialisation function for the Report class
        :param path: absolute file path for the report to be created
        :param columns: list of columns included in the report
        """
        if not os.path.exists(os.path.dirname(path)):
            try:
                os.makedirs(os.path.dirname(path))
            except FileExistsError:
                pass
        self.io_handler = open(path, 'w+', newline='', encoding='utf-8', errors="surrogateescape")
        self.csv_handler = csv.DictWriter(self.io_handler, fieldnames=columns)
        self.csv_handler.writeheader()

    def write(self, data: dict):
        """
        Write one record to the report's output file
        :param data: dictionary containing report data in the form { column_name: data }
        """
        self.csv_handler.writerow(data)

    def close(self):
        """
        Close the report's output file
        :return:
        """
        self.io_handler.close()


class ReportHandler:
    """
    The ReportHandler class, which manages all report output for MPT
    """
    action = None
    out_dir = None
    start_time = None
    stop_time = None
    errors_detected = False
    summary_data = {}
    out_files = {}
    results = {}
    file_count = 0

    def __init__(self, action: Action, out_dir: str, summary_data: dict = None):
        """
        Initialisation function for the ReportHandler class.
        :param action: an Action object representing the checksum action being reported on
        :param out_dir: the directory in which to create reports
        :param summary_data: a dictionary containing additional static data used to create reports
        """
        self.action = action
        self.start_time = datetime.now().replace(microsecond=0)
        if action == Action.CREATE:
            self.results = {x: {"count": 0, "size": 0} for x in CreationResult}
            category_dir = os.path.join(out_dir, "creation_reports")
        elif action in [Action.COMPARE_TREES, Action.COMPARE_MANIFESTS]:
            self.results = {x: {"count": 0, "size": 0} for x in ComparisonResult}
            category_dir = os.path.join(out_dir, "comparison_reports")
        elif action in [Action.VALIDATE_MANIFEST, Action.VALIDATE_TREE]:
            self.results = {x: {"count": 0, "size": 0} for x in ValidationResult}
            category_dir = os.path.join(out_dir, "validation_reports")
        else:
            category_dir = os.path.join(out_dir, "other_reports")
        self.out_dir = os.path.join(category_dir, self.start_time.strftime("%Y-%m-%dT%H%M"))
        if summary_data is not None:
            self.summary_data = summary_data

    def add_out_file(self, description: Result, columns: list):
        """
        Add a new Report object and output file
        :param description: a Result object representing the category of this report
        :param columns: a list of columns in the report
        :return:
        """
        out_path = os.path.join(self.out_dir, description.name.lower() + '.csv')
        if description not in self.out_files:
            self.out_files[description] = Report(path=out_path, columns=columns)
        if description not in self.results:
            self.results[description]["count"] = 0
            self.results[description]["size"] = 0

    def add_result(self, description: Result, data: dict):
        """
        Add the result of a single checksum operation to the relevant output file
        :param description: a Result object representing the category of this result
        :param data: a dictionary containing result data in the format { column_name: data }
        """
        if description not in self.out_files:
            self.add_out_file(description=description, columns=[k for k, v in data.items() if v is not None])
        self.out_files[description].write({k: v for k, v in data.items() if v is not None})
        self.results[description]["count"] += 1
        if "size" in data:
            if data["size"] is not None:
                self.results[description]["size"] += data["size"]

    def write_summary(self):
        """
        Write out the summary of this MPT run's results to a text file
        """
        out_path = os.path.join(self.out_dir, "summary.txt")
        with open(out_path, "w+") as out_file:
            out_file.write(self.summary())

    def assign_comparison_result(self, file_path: str, file_status: dict):
        """
        Add the results of a checksum comparison to any applicable reports. A single comparison may have to be included
        in multiple reports - e.g. the checksum on node A matches that on node B, but is missing on node C and incorrect
        on node D.
        :param file_path: path to the checksum file
        :param file_status: a dictionary containing the results of comparison in the format { node_path: Result }
        """
        failed = any(v != ComparisonResult.MATCHED for v in file_status.values())
        file_results = {"path": file_path}
        for root, status in file_status.items():
            file_results[root] = status.name.lower()
        if failed:
            for k, v in file_status.items():
                if k != "path":
                    if v != ComparisonResult.MATCHED:
                        self.add_result(description=v, data=file_results)
        else:
            self.add_result(description=ComparisonResult.MATCHED, data=file_results)

    def close(self):
        """
        Complete MPT reporting. Set the finish time and close all report files
        """
        self.stop_time = datetime.now().replace(microsecond=0)
        for x in self.out_files.values():
            x.close()

    def results_detail(self):
        """
        Combine all results into a list with file counts and sizes where applicable.
        """
        results_out = []
        for status, data in self.results.items():
            if data["count"] > 0:
                if "size" in data:
                    if data["size"] == 0:
                        results_out.append("\n{}: {:,}".format(status.value, data["count"]))
                    else:
                        results_out.append("\n{}: {:,} ({:,} bytes)".format(status.value,
                                                                            data["count"],
                                                                            data["size"]))
        return results_out

    def summary(self):
        """
        Summarise the results of this MPT run in a form which can be used in an email.
        :return: a string containing the summary
        """
        import platform
        hostname = platform.node()
        summary_header = "Minimum Preservation Tool (MPT): processing report for host {}".format(hostname)
        summary_intro = "{} results for {}".format(self.action.value, self.summary_data["primary_path"])
        if self.action == Action.VALIDATE_MANIFEST:
            summary_intro += "\n\nValidation performed using manifest file " \
                             "{}".format(self.summary_data["manifest_file"])
        elif self.action == Action.VALIDATE_TREE:
            summary_intro += "\n\nValidation performed using checksum tree {}".format(self.summary_data["cs_dir"])
        elif self.action == Action.CREATE:
            if self.summary_data["formats"] is not None:
                summary_intro += "\n\nLimited processing to file formats {}".format(str(self.summary_data["formats"]))
        if self.action in [Action.COMPARE_TREES, Action.COMPARE_MANIFESTS]:
            if self.results[ComparisonResult.MISSING]["count"] == 0 \
                    and self.results[ComparisonResult.UNMATCHED]["count"] == 0:
                summary_detail = "All checksums matched."
            else:
                self.errors_detected = True
                summary_detail = "Checksums do not match on all nodes.\n"
        elif self.action == Action.CREATE:
            if self.results[CreationResult.FAILED]["count"] > 0:
                self.errors_detected = True
                summary_detail = "Checksums could not be generated for some files."
            elif self.results[CreationResult.ADDED]["count"] > 0:
                summary_detail = "New files detected.\n"
            else:
                summary_detail = "No new files detected.\n"
        elif self.action in [Action.VALIDATE_MANIFEST, Action.VALIDATE_TREE]:
            if self.action == Action.VALIDATE_MANIFEST:
                reference = "manifest"
            else:
                reference = "checksum tree"
            if self.results[ValidationResult.MISSING]["count"] == 0 \
                    and self.results[ValidationResult.INVALID]["count"] == 0:
                summary_detail = "All files in {} correct.\n".format(reference)
            else:
                self.errors_detected = True
                summary_detail = "Some files could not be validated against {}\n".format(reference)
        else:
            summary_detail = ""
        summary_detail += "".join(self.results_detail())
        if self.stop_time is None:
            summary_trailer = "MPT processing still ongoing, started at: " \
                              "{}".format(self.start_time.strftime("%Y-%m-%d %H:%M"))
        else:
            summary_trailer = "Time taken: {}\n\nDetailed reports created " \
                              "in: {}".format(str(self.stop_time - self.start_time), self.out_dir)

        summary = "{}\n\n{}\n\n{}\n\n{}".format(summary_header, summary_intro, summary_detail, summary_trailer)
        return summary
