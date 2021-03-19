import hashlib
import multiprocessing
import os
from argparse import Namespace
from datetime import datetime
from typing import Dict, List

from tqdm import tqdm

from .codes import StagingStatus
from .defaults import *
from .email import send_email
from .hashing import hash_file


class FileStager():
    """
    Class instantiated to carry out file staging
    """
    source = None
    checksum = None
    algorithm = None
    blocksize = 0
    destinations = {}
    remove_original = None

    def __init__(self,
                 source: str,
                 destinations: List,
                 algorithm: str = default_algorithm,
                 blocksize: int = default_blocksize,
                 remove_original: bool = True):
        """
        Initialise the class instance
        :param source: path to the source filename
        :param destinations: list of destinations (data directories, checksum directories and manifests)
        :param algorithm: the hashing algorithm to use
        :param blocksize: the blocksize to use when hashing/copying files
        :param remove_original:  remove the original file when all operations complete
        """
        self.algorithm = algorithm
        self.blocksize = blocksize
        self.remove_original = remove_original
        self.next_file(source, destinations)

    def next_file(self, source: str, destinations: List):
        """
        Set the details of the next file to be staged
        :param source: the source filename
        :param destinations: list of destinations (data directories, checksum directories and manifests
        """

        if len(self.destinations) > 0:
            for v in self.destinations.values():
                if v["handler"] is not None:
                    v["handler"].close()
        self.destinations = {}
        self.source = source
        self.destinations = {
            f["root_path"]: {
                "data_file": f["destination"],
                "checksum_file": f["checksum"],
                "checksum_value": None,
                "manifest_file": f["manifest"],
                "status": StagingStatus.READY,
                "substatus": None,
                "handler": None
            }
            for f in destinations
        }

    def aborted(self):
        """
        Indicates whether file staging failed and some unstaged files could not be removed from the destination
        :return: True if any destination file handlers are in a COULD_NOT_REMOVE state, False otherwise
        """
        return any(r["status"] in [StagingStatus.COULD_NOT_REMOVE] for r in self.destinations.values())

    def completed(self):
        """
        Indicates whether file staging has completed without errors
        :return: True if all destination file handlers are in a STAGED state, False otherwise
        """
        return all(r["status"] == StagingStatus.STAGED for r in self.destinations.values())

    def failed(self):
        """
        Indicates whether file staging has failed for any destinations
        :return: True if any destination file handlers are in an error state, False otherwise
        """
        return any(r["status"] not in [StagingStatus.STAGED, StagingStatus.READY, StagingStatus.IN_PROGRESS] for r in
                   self.destinations.values())

    def ready(self):
        """
        Indicates whether file staging can commence
        :return: True if all destination file handlers are in a READY state, False otherwise
        """
        return all(r["status"] == StagingStatus.READY for r in self.destinations.values())

    def status(self):
        """
        Gives a quick view of the state of all destination file handlers
        :return: List containing the status of all destination file handlers
        """
        return [d["status"] for d in self.destinations.values()]

    def open_destination_files(self):
        """
        Attempt to open all destination files if they (or their corresponding checksum file) do not already exist.
        Set the state of each file handler accordingly.
        :return: True if all file handlers are in a READY state, False otherwise
        """
        for k, v in self.destinations.items():
            if os.path.exists(v["data_file"]):
                v["status"] = StagingStatus.DUPLICATE_FILE
            elif os.path.exists(v["checksum_file"]):
                v["status"] = StagingStatus.DUPLICATE_CHECKSUM
            else:
                try:
                    v["handler"] = self._open_file(v["data_file"])
                except Exception as e:
                    v["status"] = StagingStatus.DATA_WRITE_FAILURE
                    v["substatus"] = str(e)
                else:
                    v["status"] = StagingStatus.READY
        return self.ready()

    def _open_file(self, path: str, binary: bool = True, can_exist=False):
        """
        Open a file and return a file object
        :param path: path to the file
        :param binary: open the file in binary mode - otherwise open as text
        :param can_exist: allow appending to an existing file - otherwise only allow creation of a new file
        :return: the opened file object
        """
        if not os.path.exists(os.path.dirname(path)):
            try:
                os.makedirs(os.path.dirname(path))
            except FileExistsError:
                pass
        if can_exist:
            if binary:
                options = 'ab+'
            else:
                options = 'a+'
        else:
            if binary:
                options = 'xb'
            else:
                options = 'x'
        if not binary:
            encoding = "utf-8"
        else:
            encoding = None

        for n in range(1, 5):
            try:
                if encoding is None:
                    handler = open(path, options)
                else:
                    handler = open(path, options, encoding=encoding)
            except FileNotFoundError:
                continue
            finally:
                return handler

    def undo_staging(self):
        """
        Undo the staging process, removing any files which were copied to their destination
        """
        for k, v in self.destinations.items():
            if v["handler"] is not None:
                v["handler"].close()
                v["handler"] = None
            try:
                os.remove(v["data_file"])
                os.remove(v["checksum_file"])
            except Exception as e:
                v["status"] = StagingStatus.COULD_NOT_REMOVE
                v["substatus"] = str(e)
            else:
                if v["status"] in [StagingStatus.READY, StagingStatus.IN_PROGRESS, StagingStatus.STAGED]:
                    v["status"] = StagingStatus.UNSTAGED
                old_dir = os.getcwd()
                os.chdir(k)
                try:
                    os.removedirs(os.path.dirname(v["data_file"]))
                    os.removedirs(os.path.dirname(v["checksum_file"]))
                except OSError:
                    pass
                os.chdir(old_dir)

    def write_files(self):
        """
        Write data from the source file to all destination files, obtaining the source file's hex digest value
        in the process
        :return: True if any file writes have failed, False otherwise
        """
        hasher = hashlib.new(self.algorithm)
        for v in self.destinations.values():
            v["status"] = StagingStatus.IN_PROGRESS
        with open(self.source, "rb") as in_file:
            for block in iter(lambda: in_file.read(self.blocksize), b''):
                hasher.update(block)
                for k, v in self.destinations.items():
                    try:
                        v["handler"].write(block)
                    except Exception as e:
                        v["status"] = StagingStatus.DATA_WRITE_FAILURE
                        v["substatus"] = str(e)
                        break
                if self.failed():
                    break
        if not self.failed():
            self.checksum = hasher.hexdigest()
        self.close_destination_files()
        return self.failed()

    def close_destination_files(self):
        """
        Close all open file handlers
        """
        for v in self.destinations.values():
            v["handler"].close()
            v["handler"] = None

    def check_files(self):
        """
        Calculate the hex digest value for all destination files and compare it to that of the source file.
        Create destination checksum files in the appropriate locations and update the manifest file, if applicable.
        :return: True if any failures have occurred, False otherwise
        """
        if self.checksum is None:
            return False
        for k, v in self.destinations.items():
            next_cs, _ = hash_file(v["data_file"])
            v["checksum_value"] = next_cs
            if next_cs != self.checksum:
                v["status"] = StagingStatus.CHECKSUM_MISMATCH
            else:
                if v["checksum_file"] is None and v["manifest_file"] is None:
                    v["status"] = StagingStatus.STAGED
                else:
                    if v["checksum_file"] is not None:
                        self.write_checksum(destination_key=k)
                    if v["manifest_file"] is not None:
                        self.write_checksum(destination_key=k, manifest_file=True)
                    if v["status"] == StagingStatus.IN_PROGRESS:
                        v["status"] = StagingStatus.STAGED
        if self.completed() and self.remove_original:
            try:
                os.remove(self.source)
            except Exception as e:
                print(str(e))
        return self.failed()

    def write_checksum(self, destination_key: str, manifest_file: bool = False):
        """
        Write the checksum data for the given destination either as a standalone checksum file or by appending to a
        manifest
        :param destination_key: the root path of the destination
        :param manifest_file: True if writing to a manifest file, False otherwise
        :return: True if a write failure has occurred, False otherwise
        """
        dest_data = self.destinations[destination_key]
        if manifest_file:
            out_file = dest_data["manifest_file"]
            data_path = os.path.relpath(dest_data["data_file"], destination_key)
        else:
            out_file = dest_data["checksum_file"]
            data_path = os.path.basename(dest_data["data_file"])
        cs_value = dest_data["checksum_value"]
        try:
            with self._open_file(out_file, binary=False, can_exist=manifest_file) as o:
                o.write("{0} *\\{1}\n".format(cs_value, data_path))
        except Exception as e:
            dest_data["status"] = StagingStatus.CHECKSUM_WRITE_FAILURE
            dest_data["substatus"] = str(e)
            return True
        return False

    def start_copy(self):
        """
        Initiate the staging process for the current file.
        :return: True if the staging process failed, False otherwise
        """
        self.open_destination_files()

        if self.ready():
            self.write_files()

        if not self.failed():
            self.check_files()

        if self.failed():
            self.undo_staging()

        return self.failed()


def _remove_empty_folders(path, remove_root=True):
    """
    Delete empty directories beneath a given root.
    :param path: the root path
    :param remove_root: True if the root directory itself should be deleted
    """
    if not os.path.isdir(path):
        return

    files = os.listdir(path)
    if len(files):
        for f in files:
            fullpath = os.path.join(path, f)
            if os.path.isdir(fullpath):
                _remove_empty_folders(fullpath)

    files = os.listdir(path)
    if len(files) == 0 and remove_root:
        os.rmdir(path)


def _count_files(path: str, formats: List = None, recursive: bool = True):
    """ Counts the number of files within the specified directory
    :param path: the top level path to count files in
    :param formats: a list of file endings to count; if omitted, count all files
    :param recursive: true if counting should include sub-directories
    :return: the number of files in the specified path
    """
    count = 0
    try:
        for p in os.scandir(path):
            if p.is_file():
                if formats is None:
                    count += 1
                else:
                    if (p.path.endswith(tuple(formats))):
                        count += 1
            if recursive and p.is_dir():
                count += _count_files(p.path, formats=formats, recursive=recursive)
    except (IOError, OSError) as e:
        print("Permission Error ({0}): {1} for {2}".format(e.errno, e.strerror, path))
    return count


def _get_files_to_stage(directory: str, target_roots: List, checksum_roots: List, manifest_files: List,
                        algorithm: str = None, formats: List = None, recursive: bool = True):
    """ Create a generator to iterate all files in a directory
    :param directory: the root directory to traverse
    :param target_roots: a list of root directories to which the file should be copied
    :param checksum_roots: a list of root directories in which checksums should be created
    :param manifest_files: a list of manifest files to update
    :param algorithm: the algorithm to use for hashing
    :param formats: a list of file endings to list; if omitted, list all files
    :param recursive: true if listing should include sub-directories
    :return: an iterable containing all matching files
    """

    if algorithm is None:
        algorithm = default_algorithm
    if os.path.isdir(directory):
        for root, dirs, files in os.walk(directory):
            if formats is None:
                filtered_files = files
            else:
                filtered_files = [file for file in files if (file.endswith(tuple(formats)))]
            for f in filtered_files:
                source_file = os.path.join(root, f)
                r_path = os.path.relpath(source_file, directory)
                index = 0
                dest_dicts = []
                for target_root in target_roots:
                    dest_file = os.path.join(target_root, r_path)
                    dest_dict = {
                        "root_path": u"{}".format(target_root),
                        "destination": u"{}".format(dest_file),
                        "checksum": None,
                        "manifest": None,
                    }
                    if len(checksum_roots) > 0:
                        cs_root = checksum_roots[index]
                        cs_file = u"{}".format(os.path.join(cs_root, "{0}.{1}".format(r_path, algorithm)))
                        dest_dict["checksum"] = cs_file
                    if len(manifest_files) > 0:
                        manifest_file = u"{}".format(manifest_files[index])
                        dest_dict["manifest"] = manifest_file
                    dest_dicts.append(dest_dict)
                    index += 1
                file = {
                    "source": source_file,
                    "algorithm": algorithm,
                    "destinations": dest_dicts
                }
                yield(file)
            if not recursive:
                return


def _confirm_staging_targets(staging_summary: Dict):
    """ Print a summary of planned staging actions, including manifest and checksum files, and prompt for confirmation
    :param staging_summary: a dictionary containin a summary of all staging directories and files
    :return: True to continue, False to abort
    """

    print("Source: {0}".format(staging_summary["source"]))
    n = 1
    for next_target in staging_summary["destinations"]:
        print("-" * 40)
        print("Destination {0}".format(n))
        print("Data path: {0}".format(next_target["data_path"]))
        print("Checksum path: {0}".format(next_target["checksum_path"]))
        if "manifest_file" in next_target:
            print("Manifest file: {0}".format(next_target["manifest_file"]))
        n += 1
    print("\n")
    response = input("Begin staging using these settings? (y/N)").lower()
    if len(response) > 0:
        return response[0] == 'y'
    else:
        return False


def _stage_file(next_file: Dict):
    """
    Instantiate the FileStager object and begin staging for a given file
    :param next_file: a dictionary containing the file and directory names used for staging the file
    :return: a triple consisting of the original file name, the staging status, and the details of all destinations
    """
    fs = FileStager(source=next_file["source"], destinations=next_file["destinations"],
                    algorithm=next_file["algorithm"])
    fs.start_copy()
    if fs.completed():
        result = (next_file["source"], "staged", fs.destinations)
    elif fs.aborted():
        result = (next_file["source"], "aborted", fs.destinations)
    elif fs.failed():
        result = (next_file["source"], "failed", fs.destinations)
    else:
        result = (next_file["source"], "unknown", fs.destinations)
    return result


def stage_files(args: Namespace):
    """
    Main entry point to staging functions. Called by the main module
    :param args: a Namespace returned by argparse passed from the main module
    """
    # Set start time
    start_time = datetime.now().replace(microsecond=0)

    # Initialise list of target and checksum directories
    targets = []
    checksums = []

    # Override the consecutive failure threshold if necessary
    if args.max_failures is None:
        failure_threshold = max_failures
    else:
        failure_threshold = args.max_failures

    # Initialise staging summary to be used to confirm actions
    summary = {
        "source": args.dir,
        "destinations": []
    }

    # Initialise staging results
    results = {
        "staged": [],
        "failed": [],
        "aborted": [],
        "unknown": [],
        "consecutive_failures": 0,
        "failure_threshold": failure_threshold
    }

    # Check that the number of target folders, checksum folders and manifests matches
    if len(args.targets) != len(args.trees) and len(args.trees) > 0:
        print("Number of target directories does not match number of tree directories")
        return
    elif len(args.targets) != len(args.manifests) and len(args.manifests) > 0:
        print("Number of target directories does not match number of manifest files")
        return

    # If no checksum directories have been specified, then assume that each target directory
    # should contain "files" and "checksums" folders to hold the staged files and checksums respectively
    if len(args.trees) == 0:
        for t in args.targets:
            targets.append(os.path.join(t, "files"))
            checksums.append(os.path.join(t, "checksums"))
    else:
        targets = args.targets
        checksums = args.trees

    # Iterate over list of target directories, find the corresponding checksum directory and manifest file
    # (if applicable) and add it to the summary
    n = 0
    for next_target in targets:
        destination = {
            "data_path": next_target,
            "checksum_path": checksums[n]
        }
        if len(args.manifests) > 0:
            destination["manifest_file"] = args.manifests[n]
        summary["destinations"].append(destination)
        n += 1

    # Unless overridden by command line options, ask the user to confirm the staging details
    if args.no_confirm:
        stg_continue = True
    else:
        stg_continue = _confirm_staging_targets(summary)
    if not stg_continue:
        return 1

    # Build a generator to list all files to be staged, along with their staging destinations, checksum
    # destination and manifest files
    files_iterable = _get_files_to_stage(directory=args.dir, target_roots=targets, checksum_roots=checksums,
                                         manifest_files=args.manifests)

    # Create a multiprocessing pool with the appropriate number of processes
    pool = multiprocessing.Pool(processes=args.processes)

    if args.count_files:
        file_count = _count_files(path=args.dir)
    else:
        file_count = None

    # Have the multiprocessing pool pass each item returned by the generator to stage_files and monitor
    # progress via tqdm
    for file, status, destinations in tqdm(pool.imap_unordered(_stage_file, files_iterable), total=file_count,
                                           desc="MPT({}p)/Staging files".format(args.processes)):
        # Terminate processing if the failure threshold has been exceeded
        terminate = _add_to_results(results, file, status, destinations)
        if terminate:
            break

    # Remove any empty folders left behind in the staging ara after files have been staged
    if not args.keep_empty_folders:
        _remove_empty_folders(args.dir, remove_root=False)

    stop_time = datetime.now().replace(microsecond=0)

    results["start_time"] = start_time
    results["stop_time"] = stop_time

    # Display, print and email results
    _show_results(args, results)


def _show_results(args: Namespace, results: Dict):
    """
    Display all the results of file stating
    :param args: a Namespace returned by argparse passed from the main module
    :param results: a Dict containing the results of staging
    """
    if results["consecutive_failures"] > results["failure_threshold"]:
        staging_interrupted = True
    else:
        staging_interrupted = False

    summary = _produce_summary(args=args, results=results)
    print(summary)

    try:
        results.pop("consecutive_failures")
        results.pop("failure_threshold")
        results.pop("start_time")
        results.pop("stop_time")
    except KeyError:
        pass

    report_dir = _write_reports(args=args, results=results, summary=summary)
    print("\nDetailed reports created in: " + report_dir)

    if args.email is not None:
        _email_report(recipients=args.email, results=results, reports_dir=report_dir, mail_body=summary,
                      staging_interrupted=staging_interrupted)


def _produce_summary(args: Namespace, results: Dict):
    """
    Produce staging summary based on results and initial arguments
    :param args: a Namespace returned by argparse, passed from the main module
    :param results: a Dict containing the results of staging
    :return: a string containing the a summary of the staging process
    """
    import platform
    summary = "Minimum Preservation Tool (MPT): processing report for host " + platform.node()
    summary = summary + "\n\nFile staging results for " + args.dir
    if results["consecutive_failures"] > results["failure_threshold"]:
        summary = summary + "\n\nFile staging was interrupted due to consecutive error threshold breach."
    if len(results["staged"]) == 0:
        summary = summary + "\n\nNo new files staged."
    else:
        summary = summary + "\n\nNew files added to storage: " + str(len(results["staged"]))
    if len(results["failed"]) > 0:
        summary = summary + "\n\nFiles which failed staging: " + str(len(results["failed"]))
    if len(results["aborted"]) > 0:
        summary = summary + "\n\nFiles incompletely staged: " + str(len(results["aborted"]))
    summary = summary + "\n\nTime taken: " + str(results["stop_time"] - results["start_time"])
    return summary


def _write_reports(args: Namespace, results: Dict, summary: str):
    """
    Write checksum validation results to a file
    :param args: a Namespace returned by argparse, passed from the main module
    :param results: a Dict containing the results of staging
    :param output_dir: the directory in which to create reports
    :return: the directory in which the reports were created
    """
    reports_dir = os.path.join(args.output, "staging_reports")
    dated_dir = os.path.join(reports_dir, datetime.now().strftime("%Y-%m-%dT%H%M"))

    if not os.path.exists(dated_dir):
        try:
            os.makedirs(dated_dir)
        except Exception as e:
            print("Cannot create report directory, error: " + str(e))
            return True

    with open(os.path.join(dated_dir, "summary.txt"), 'w') as out_file:
        out_file.write(summary)

    _write_csv_files_from_dictionary(args=args, dictionary=results, output_dir=dated_dir)

    return dated_dir


def _write_csv_files_from_dictionary(args: Namespace, dictionary: Dict, output_dir: str):
    """
    Write out each list item in a dictionary as a csv file
    :param args: a Namespace returned by argparse, passed from the main module
    :param dictionary: the dictionary to iterate
    :param output_dir: the directory in which to write files
    """
    import csv
    try:
        for k, v in dictionary.items():
            if isinstance(v, list):
                if len(v) > 0:
                    file_name = k + ".csv"
                    with open(os.path.join(output_dir,file_name), 'w', encoding='utf=8',newline='') as csv_file:
                        if isinstance(v[0], dict):
                            output = csv.DictWriter(csv_file, fieldnames=v[0].keys())
                            output.writeheader()
                        else:
                            output = csv.writer(csv_file)
                        for el in v:
                            if isinstance(el, dict):
                                output.writerow(el)
                            else:
                                if self.absolute_path:
                                    output.writerow([el.replace("*\\",args.dir + "\\")])
                                else:
                                    output.writerow([el])
            elif isinstance(v, dict):
                if next(iter(v.values())) is None:
                    _write_csv_files_from_dictionary(dictionary={k: list(v.keys())}, output_dir=output_dir)
                else:
                    _write_csv_files_from_dictionary(dictionary=v, output_dir=output_dir)
            else:
                pass
    except StopIteration:
        pass


def _email_report(recipients: List, results: Dict, reports_dir: str, mail_body: str, staging_interrupted: bool = False):
    """
    Email the results of file staging
    :param recipients: a List of email recipients
    :param results: a Dict containing the results of
    :param reports_dir: the directory containing any reports which are to be attached
    :param mail_body: a string to be used as the mail body text
    :param staging_interrupted: whether staging was prematurely terminated due to exceeding the failure threshold
    """
    if staging_interrupted:
        mail_subject = "BL MPT Staging: Staging interrupted"
    elif len(results["failed"]) > 0 or len(results["aborted"]) > 0:
        mail_subject = "BL MPT Staging: Errors encountered"
    elif len(results["staged"]) == 0:
        mail_subject = "BL MPT Staging: No files to stage"
    else:
        mail_subject = "BL MPT Staging: All files staged successfully"
    attachments = [os.path.join(reports_dir, f) for f in os.listdir(reports_dir) if (
            os.path.isfile(os.path.join(reports_dir, f)) and f.endswith("csv"))]

    size = sum(os.path.getsize(f) for f in attachments)
    zip = size >= mail_size_threshold
    send_email(subject=mail_subject, recipients=recipients, message=mail_body, attachments=attachments,
               zip_files=zip)


def _add_to_results(existing_results: Dict, new_file: str, file_status: str, new_results: Dict):
    """
    Add the outcome of staging for a single file to the dictionary of overall results
    :param existing_results: a Dict containing the overall staging results for all files so far
    :param new_file: the original path of the file
    :param file_status: the summary status of staging for the file
    :param new_results: a Dict containing the detailed results of staging for the file
    :return: the new Dict of overall results for all files
    """
    write_failed = any(r["status"] in [StagingStatus.DATA_WRITE_FAILURE, StagingStatus.CHECKSUM_WRITE_FAILURE]
                       for r in new_results.values())
    new_entry = {"path": new_file}
    for root, values in new_results.items():
        new_entry[root] = values["status"].value
    existing_results[file_status].append(new_entry)
    if write_failed:
        existing_results["consecutive_failures"] += 1
    else:
        existing_results["consecutive_failures"] = 0
    return existing_results["consecutive_failures"] > existing_results["failure_threshold"]
