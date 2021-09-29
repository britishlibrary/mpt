import mmap
import multiprocessing
import os

from tqdm import tqdm

from .codes import (Action, ComparisonResult, CreationResult,
                    ExceptionsResults, ValidationResult)
from .defaults import *
from .email import send_email
from .hashing import hash_file, algorithms_supported
from .paths import fix_path
from .results import ReportHandler


def scan_tree(path, recursive=False, formats: list = None):
    """
    A generator to return all files within a directory.
    :param path: top-level directory to scan
    :param recursive: true if the scan should be recursive
    :param formats: an optional list of file extensions - if provided, the scan will be limited to files with these
        extensions
    :return: an iterable sequence of files found by the scan
    """
    for entry in os.scandir(path):
        try:
            if entry.is_dir(follow_symlinks=False) and recursive:
                yield from scan_tree(entry.path, recursive, formats)
            else:
                if entry.is_file():
                    if formats is None:
                        yield entry.path
                    else:
                        if entry.path.endswith(tuple(formats)):
                            yield entry.path
        except PermissionError:
            pass


def walk_tree(path, recursive=False, formats: list = None):
    """
    Alternative generator to get all files in directory. May be useful in the
    event of network issues
    :param path: top-level directory to scan
    :param recursive: true if the scan should be recursive
    :param formats: an optional list of file extensions - if provided, the scan will be limited to files with these
        extensions
    :return: an iterable sequence of files found by the walk
    """
    if recursive:
        for root, dirs, files in os.walk(path):
            for file in files:
                full_path = os.path.join(root, file)
                if formats is None:
                    yield full_path
                else:
                    if full_path.endswith(tuple(formats)):
                        yield full_path
    else:
        for file in os.listdir(path):
            full_path = os.path.join(path, file)
            if os.path.isfile(full_path):
                if formats is None:
                    yield full_path
                else:
                    if full_path.endswith(tuple(formats)):
                        yield full_path


def count_lines(file_path: str):
    """
    Count the number of lines in a text file
    :param file_path: path to the file
    :return: the number of lines
    """
    with open(file_path, 'r', errors='ignore') as f:
        for i, l in enumerate(f):
            pass
    return i + 1


def iterate_manifest(file_path: str):
    """
    A generator to read all valid lines in a checksum manifest file
    :param file_path: path to the manifest file
    :return: an iterable sequence of list items, each containing the components [checksum, file path] of a
        manifest record
    """
    with open(file_path, 'r', encoding='utf8', errors='surrogateescape') as in_file:
        for line in in_file:
            line_s = line.rstrip('\r\n').split(' ', 1)
            if len(line_s) > 1:
                yield line_s


class FileManager:
    """
    The FileManager class, used for all MPT operations except staging.
    """
    primary_path = None
    cs_dir = None
    manifest_file = None
    email = None
    formats = None
    last_action = None
    output_dir = base_output_dir
    algorithm = default_algorithm
    num_procs = default_processes
    blocksize = default_blocksize
    cache_size = default_cachesize
    count_files = True
    recursive = True
    absolute_path = False
    other_paths = []
    report_handler = None
    debug_mode = False

    def __init__(self, primary_path: str, cs_dir: str = None, manifest_file: str = None,
                 algorithm: str = None, blocksize: int = None, num_procs: int = None,
                 recursive: bool = False, count_files: bool = True, email: list = None,
                 formats: list = None, output_dir: str = None, other_paths: list = None,
                 absolute_path: bool = False, cache_size: int = None):
        """
        Initialisation function for the FileManager class.
        :param primary_path: path to the primary directory (containing data files or checksum files, depending on
            the action being carried out)
        :param cs_dir: the top-level directory of the "checksum tree" used to hold checksum files
        :param manifest_file: path to the manifest file being used
        :param algorithm: the checksum algorithm to be used
        :param blocksize: the block size used for I/O operations when calculating checksums
        :param num_procs: the number of concurrent processes to spawn
        :param recursive: true if directories beneath primary_path should be processed recursively
        :param count_files: true to count files prior to processing (increases startup time)
        :param email: list of email addresses to send reports to on completion
        :param formats: list of file extensions to restrict checksum creation to
        :param output_dir: base directory in which report subfolders should be created
        :param other_paths: other directories containing checksum information (trees or manifests) used when comparing
            checksums
        :param absolute_path: true if absolute paths are required in reports
        :param cache_size: number of output records to cache in memory before writing to disk
        """
        self.primary_path = os.path.abspath(primary_path)
        if not self.primary_path.endswith(os.sep):
            self.primary_path = self.primary_path + os.sep
        if cs_dir is not None:
            self.cs_dir = os.path.abspath(cs_dir)
        self.recursive = recursive
        self.count_files = count_files
        self.email = email
        self.formats = formats
        self.absolute_path = absolute_path
        self.other_paths = other_paths
        if manifest_file is not None:
            self.manifest_file = os.path.abspath(manifest_file)
        if output_dir is not None:
            self.output_dir = output_dir
        if algorithm is not None:
            self.algorithm = algorithm
        if blocksize is not None:
            self.blocksize = blocksize
        if num_procs is not None:
            self.num_procs = num_procs
        if cache_size is not None:
            self.cache_size = cache_size
        if self.debug_mode:
            for k, v in self.__dict__.items():
                print("{}: {}".format(k, v))

    def _email_report(self):
        """
        Email the results of checksum operations to the configured recipients
        """
        mail_body = self.report_handler.summary()
        if self.report_handler.errors_detected:
            mail_subject = "BL MPT {}: Errors encountered".format(self.last_action.value)
        else:
            if self.last_action in [Action.VALIDATE_MANIFEST, Action.VALIDATE_TREE, Action.COMPARE_MANIFESTS,
                                    Action.COMPARE_TREES]:
                mail_subject = "BL MPT {}: No errors encountered".format(self.last_action.value)
            elif self.last_action == Action.CREATE:
                if self.report_handler.results[CreationResult.ADDED] == 0:
                    mail_subject = "BL MPT {}: No new files".format(self.last_action.value)
                else:
                    mail_subject = "BL MPT {}: New files detected".format(self.last_action.value)

        if email_only_exceptions:
            exceptions = [f.name.lower() for f in ExceptionsResults]
            attachments = [os.path.join(self.report_handler.out_dir, f) for f in os.listdir(self.report_handler.out_dir)
                           if (os.path.isfile(os.path.join(self.report_handler.out_dir, f)) and f.endswith("csv")
                               and os.path.splitext(f)[0] in exceptions)]
        else:
            attachments = [os.path.join(self.report_handler.out_dir, f) for f in os.listdir(self.report_handler.out_dir)
                           if (os.path.isfile(os.path.join(self.report_handler.out_dir, f)) and f.endswith("csv"))]

        size = sum(os.path.getsize(f) for f in attachments)
        zip = size >= mail_size_threshold
        send_email(subject=mail_subject, recipients=self.email, message=mail_body, attachments=attachments,
                   zip_files=zip)

    def _normalise_path(self, file_path: str):
        """
        Normalise a file path according to the configured absolute_path parameter
        :param file_path: the reported file path in its expected relative form
        :return: the file path in either absolute or relative form depending on the FileManager's absolute_path setting
        """
        if file_path[0] == '*':
            if self.absolute_path:
                return file_path.replace('*', self.primary_path)
        return file_path

    def _show_results(self):
        """
        Display the results of checksum operations on screen and send emails as required
        """
        summary = self.report_handler.summary()
        print(summary)
        self.report_handler.write_summary()

        if self.email is not None:
            self._email_report()

    def _check_for_cs_file(self, data_file_path: str):
        """
        Checks whether a checksum file exists in a tree for the given data file
        :param data_file_path: path to the data file
        :return: a tuple in the form (relative path to data file, validation result) or (None, None) if file exists
        """
        rel_path = os.path.relpath(data_file_path, self.primary_path)
        for ext in algorithms_supported:
            cs_file_path = fix_path(os.path.join(self.cs_dir, rel_path) + '.' + ext)
            if os.path.exists(cs_file_path):
                return None, None
        return rel_path, ValidationResult.ADDITIONAL

    def _check_for_file_in_manifest(self, data_file_path: str):
        """
        Checks whether a given data file is listed in the current manifest file
        :param data_file_path: path to the data file
        :return: a tuple in the form (relative path to data file, validation result) or (None, None) if the file is
            listed
        """
        with open(self.manifest_file, 'r') as manifest:
            # Make allowance for paths containing using either forward slashes or escaped
            # backslashes as separators
            rel_path = os.path.relpath(data_file_path, self.primary_path)
            paths = [
                "*{sep}{path}".format(sep=os.sep, path=rel_path),
                "*{sep}{path}".format(sep="/", path=rel_path.replace("\\","/"))
            ]
            manifest_map = mmap.mmap(manifest.fileno(), 0, access=mmap.ACCESS_READ)
            for next_path in paths:
                found = manifest_map.find(next_path.encode("utf-8"))
                if found != -1:
                    return None, None
            return rel_path, ValidationResult.ADDITIONAL

    def _check_other_manifests(self, manifest_line: str):
        """
        Compare an entry in a manifest file to the corresponding entry in other manifests
        :param manifest_line: the text of the manifest entry
        :return: a Tuple in the form (file path, {results}) where (results) is a dictionary containing
            the status of the checksum data in other trees
        """
        manifest_maps = {}
        results = {}

        for m in self.other_paths:
            next_file = open(m, 'r')
            next_map = mmap.mmap(next_file.fileno(), 0, access=mmap.ACCESS_READ)
            manifest_maps[m] = next_map

        try:
            file_cs = manifest_line.split()[0]
            file_path = ' '.join(manifest_line.split()[1:])
        # Handle blank lines
        except IndexError:
            return None, None
        if len(file_path) == 0:
            return None, None

        for manifest_path, manifest_map in manifest_maps.items():
            found = manifest_map.find(file_path.encode("utf-8"))
            if found == -1:
                results[manifest_path] = ComparisonResult.MISSING
            else:
                s_pos = found - (len(file_cs) + 1)
                e_pos = s_pos + len(file_cs)
                manifest_cs = manifest_map[s_pos:e_pos]
                if manifest_cs.decode("utf-8") != file_cs:
                    results[manifest_path] = ComparisonResult.UNMATCHED
                else:
                    results[manifest_path] = ComparisonResult.MATCHED

        for m in manifest_maps.values():
            m.close()

        r_val = self._normalise_path(file_path), results
        return r_val

    def _compare_checksum_file_to_other_trees(self, checksum_file_path: str):
        """
        Compares the hash value in a given checksum file to its corresponding
        version in other checksum trees
        :param checksum_file_path:  the path to the checksum file
        :return: a Tuple in the form (file path, {results}) where (results) is a dictionary containing
            the status of the checksum data in other trees
        """
        results = {}
        rel_path = os.path.relpath(checksum_file_path, self.primary_path)
        file_key = "*{sep}{path}".format(sep=os.sep, path=rel_path)
        in_path = fix_path(checksum_file_path)
        try:
            with open(in_path, "r", encoding="utf-8", errors="surrogateescape") as cs_file:
                cs_line = cs_file.read().rstrip('\r\n').split(' ')
                master_cs = cs_line[0]
            for next_tree in self.other_paths:
                try:
                    other_cs_path = fix_path(os.path.join(next_tree, rel_path))
                    if os.path.exists(other_cs_path):
                        with open(other_cs_path, "r", encoding="utf-8", errors="surrogateescape") as cs_file:
                            cs_line = cs_file.read().rstrip('\r\n').split(' ')
                            other_cs = cs_line[0]
                        if master_cs == other_cs:
                            results[next_tree] = ComparisonResult.MATCHED
                        else:
                            results[next_tree] = ComparisonResult.UNMATCHED
                    else:
                        results[next_tree] = ComparisonResult.MISSING
                except OSError:
                    results[next_tree] = ComparisonResult.OSERROR
                    pass
        except OSError:
            results["ALL"] = ComparisonResult.OSERROR
            pass

        r_val = self._normalise_path(file_key), results
        return r_val

    def _create_checksum_or_skip_file(self, in_file: str, algorithm: str = None):
        """ Generate a checksum for a file if it has not already been created
        :param in_file: the file to be checked
        :param algorithm: the checksum algorithm to be used
        :return: a tuple containing the data file path and its status (correct/incorrect/missing)
        """
        if algorithm is None:
            algorithm = self.algorithm
        r_path = os.path.relpath(in_file, self.primary_path)
        out_file = fix_path(
            os.path.join(self.cs_dir, r_path) + '.' + algorithm
        )
        if os.path.exists(out_file):
            return in_file, CreationResult.SKIPPED, None
        else:
            if not os.path.exists(os.path.dirname(out_file)):
                try:
                    os.makedirs(os.path.dirname(out_file))
                except FileExistsError:
                    pass
            try:
                checksum, size = hash_file(in_file, algorithm=algorithm)
                with open(out_file, 'w', encoding='utf-8', errors="surrogateescape") as cs_file:
                    cs_file.write("{cs} *{sep}{path}\n".format(cs=checksum,
                                                               sep=os.sep,
                                                               path=os.path.basename(in_file)))
            except Exception as e:
                print(str(e))
                return in_file, CreationResult.FAILED, None
            if self.manifest_file is not None:
                with open(self.manifest_file, 'a+', encoding='utf-8') as manifest_file:
                    manifest_file.write("{cs} *{sep}{path}\n".format(cs=checksum, sep=os.sep, path=r_path))
            return self._normalise_path(in_file), CreationResult.ADDED, size

    def _validate_checksum_file(self, checksum_file_path: str, algorithm: str = None):
        """ Use a checksum file within a checksum tree to validate its
        corresponding data file
        :param checksum_file_path: the path to the checksum file
        :param algorithm: the checksum algorithm to use
        :return: a tuple containing the data file path and its status (correct/incorrect/missing)
        """
        if algorithm is None:
            algorithm = os.path.splitext(checksum_file_path)[1][1:]

        fixed_path = fix_path(checksum_file_path)

        try:
            with open(fixed_path, "r", encoding="utf-8", errors="surrogateescape") as cs_file:
                cs_line = cs_file.read().rstrip('\r\n').split(' ')
                original_cs = cs_line[0]
        except OSError:
            r_val = self._normalise_path(checksum_file_path), ValidationResult.OSERROR
            return r_val
        cs_rel_path = os.path.relpath(checksum_file_path, self.cs_dir)
        data_rel_path = os.path.splitext(cs_rel_path)[0]
        full_path = fix_path(os.path.join(self.primary_path, data_rel_path))
        file_key = "*{sep}{path}".format(sep=os.sep, path=os.path.join(data_rel_path))
        size = None
        if os.path.exists(full_path):
            try:
                current_cs, size = hash_file(full_path, algorithm=algorithm)
                if current_cs == original_cs:
                    file_status = ValidationResult.VALID
                else:
                    file_status = ValidationResult.INVALID
            except OSError:
                file_status = ValidationResult.OSERROR
                pass
        else:
            file_status = ValidationResult.MISSING
        r_val = self._normalise_path(file_key), file_status, size
        return r_val

    def _validate_file_with_checksum(self, original_checksum_data):
        """ Validate a data file against the checksum value provided
        :param original_checksum_data: a tuple in the format (file_path, file_checksum)
        :return: a triple containing the data file path, its status (correct/incorrect/missing) and its size
        """
        original_cs, rel_path = original_checksum_data
        full_path = fix_path(rel_path.replace('*', self.primary_path))
        size = None
        if os.path.exists(full_path):
            try:
                current_cs, size = hash_file(full_path, algorithm=self.algorithm)
                if current_cs == original_cs:
                    file_status = ValidationResult.VALID
                else:
                    file_status = ValidationResult.INVALID
            except OSError:
                file_status = ValidationResult.OSERROR
                pass
        else:
            file_status = ValidationResult.MISSING
        r_val = self._normalise_path(rel_path), file_status, size
        return r_val

    def compare_manifests(self):
        """
        Compare the contents of a master manifest file to other files
        """
        self.last_action = Action.COMPARE_MANIFESTS
        self.report_handler = ReportHandler(action=self.last_action, out_dir=self.output_dir,
                                            summary_data={
                                                "primary_path": self.primary_path
                                            })

        pool = multiprocessing.Pool(processes=self.num_procs)
        if self.count_files:
            line_count = count_lines(self.primary_path)
        else:
            line_count = None

        results_cache = []

        with open(self.primary_path, 'r') as manifest_file:
            for file_path, status in tqdm(pool.imap_unordered(self._check_other_manifests, manifest_file),
                                          total=line_count, desc="MPT({}p)/Comparing manifests".format(self.num_procs)):
                if file_path is not None:
                    results_cache.append((file_path, status))
                if len(results_cache) >= self.cache_size:
                    for next_path, next_status in results_cache:
                        self.report_handler.assign_comparison_result(file_path=next_path, file_status=next_status)
                    self.report_handler.write_summary()
                    results_cache = []
        # Write any records remaining in the cache after all files are processed
        for next_path, next_status in results_cache:
            self.report_handler.assign_comparison_result(file_path=next_path, file_status=next_status)
        self.report_handler.close()
        self._show_results()

    def compare_trees(self):
        """
        Compare each checksum file in a tree against its counterparts in other checksum trees
        """
        self.last_action = Action.COMPARE_TREES
        self.report_handler = ReportHandler(action=self.last_action, out_dir=self.output_dir,
                                            summary_data={
                                                "cs_dir": self.cs_dir,
                                                "primary_path": self.primary_path
                                            })

        pool = multiprocessing.Pool(processes=self.num_procs)
        files_iterable = scan_tree(path=self.primary_path, recursive=self.recursive)

        if self.count_files:
            file_count = sum([1 for x in files_iterable])
            files_iterable = scan_tree(path=self.primary_path, recursive=self.recursive)
        else:
            file_count = None

        results_cache = []

        for file_path, status in tqdm(pool.imap_unordered(self._compare_checksum_file_to_other_trees, files_iterable),
                                      total=file_count, desc="MPT({}p)/Comparing checksums".format(self.num_procs)):
            results_cache.append((file_path, status))
            if len(results_cache) >= self.cache_size:
                for next_path, next_status in results_cache:
                    self.report_handler.assign_comparison_result(file_path=next_path, file_status=next_status)
                self.report_handler.write_summary()
                results_cache = []
        # Write any records remaining in the cache after all files are processed
        for next_path, next_status in results_cache:
            self.report_handler.assign_comparison_result(file_path=next_path, file_status=next_status)
        self.report_handler.close()
        self._show_results()

    def create_checksums(self):
        """ Create checksums and update manifest
        """
        self.last_action = Action.CREATE
        self.report_handler = ReportHandler(action=self.last_action, out_dir=self.output_dir,
                                            summary_data={
                                                "primary_path": self.primary_path,
                                                "cs_dir": self.cs_dir,
                                                "manifest_file": self.manifest_file,
                                                "formats": self.formats
                                            })
        pool = multiprocessing.Pool(processes=self.num_procs)
        files_iterable = scan_tree(path=self.primary_path, recursive=self.recursive, formats=self.formats)
        if self.count_files:
            file_count = sum([1 for x in files_iterable])
            files_iterable = scan_tree(path=self.primary_path, recursive=self.recursive, formats=self.formats)
        else:
            file_count = None

        results_cache = []

        for file_path, status, file_size in tqdm(pool.imap_unordered(self._create_checksum_or_skip_file,
                                                                     files_iterable), total=file_count,
                                                 desc="MPT({}p)/Creating checksums".format(self.num_procs)):
            results_cache.append((file_path, status, file_size))
            if len(results_cache) >= self.cache_size:
                for next_path, next_status, next_size in results_cache:
                    self.report_handler.add_result(description=next_status, data={"path": next_path, "size": next_size})
                self.report_handler.write_summary()
                results_cache = []
        # Write any records remaining in the cache after all files are processed
        for next_path, next_status, next_size in results_cache:
            self.report_handler.add_result(description=next_status, data={"path": next_path, "size": next_size})
        self.report_handler.close()
        self._show_results()

    def validate_manifest(self):
        """ Validate files using the checksums listed in a manifest file
        """
        self.last_action = Action.VALIDATE_MANIFEST
        self.report_handler = ReportHandler(action=self.last_action, out_dir=self.output_dir,
                                            summary_data={
                                                "manifest_file": self.manifest_file,
                                                "primary_path": self.primary_path
                                            })

        if not os.path.exists(self.manifest_file):
            raise EnvironmentError("Manifest file " + self.manifest_file + " not found")

        pool = multiprocessing.Pool(processes=self.num_procs)
        files_iterable = scan_tree(path=self.primary_path, recursive=True)
        lines_iterable = iterate_manifest(self.manifest_file)

        if self.count_files:
            file_count = count_lines(self.manifest_file)
        else:
            file_count = None

        results_cache = []

        for file_path, status, file_size in tqdm(
                pool.imap_unordered(self._validate_file_with_checksum, lines_iterable),
                total=file_count, desc="MPT({}p)/Validating files".format(self.num_procs)):
            results_cache.append((file_path, status, file_size))
            if len(results_cache) >= self.cache_size:
                for next_path, next_status, next_size in results_cache:
                    self.report_handler.add_result(description=next_status, data={"path": next_path, "size": next_size})
                self.report_handler.write_summary()
                results_cache = []
        # Write any records remaining in the cache after all files are processed
        for next_path, next_status, next_size in results_cache:
            self.report_handler.add_result(description=next_status, data={"path": next_path, "size": next_size})

        # Look for data files not listed in manifest
        for file_path, status in tqdm(pool.imap_unordered(self._check_for_file_in_manifest, files_iterable),
                                      desc="MPT({}p)/Finding additional files".format(self.num_procs)):
            if status is not None:
                self.report_handler.add_result(status, {"path": "*{sep}{path}".format(sep=os.sep, path=file_path)})

        self.report_handler.close()
        self._show_results()

    def validate_tree(self):
        """ Validate files using the checksums listed in a checksum tree
        """
        self.last_action = Action.VALIDATE_TREE
        self.report_handler = ReportHandler(action=self.last_action, out_dir=self.output_dir,
                                            summary_data={
                                                "cs_dir": self.cs_dir,
                                                "primary_path": self.primary_path
                                            })

        if not os.path.exists(self.cs_dir):
            raise EnvironmentError("Checksum tree directory " + self.cs_dir + " not found")

        pool = multiprocessing.Pool(processes=self.num_procs)
        cs_files_iterable = scan_tree(path=self.cs_dir, recursive=self.recursive)
        data_files_iterable = scan_tree(path=self.primary_path, recursive=self.recursive)

        if self.count_files:
            file_count = sum([1 for x in cs_files_iterable])
            cs_files_iterable = scan_tree(path=self.cs_dir, recursive=self.recursive)
        else:
            file_count = None

        results_cache = []

        for file_path, status, file_size in tqdm(pool.imap_unordered(self._validate_checksum_file, cs_files_iterable),
                                     total=file_count, desc="MPT({}p)/Validating files".format(self.num_procs)):
            results_cache.append((file_path, status, file_size))
            if len(results_cache) >= self.cache_size:
                for next_path, next_status, next_size in results_cache:
                    self.report_handler.add_result(description=next_status, data={"path": next_path, "size": next_size})
                self.report_handler.write_summary()
                results_cache = []
        # Write any records remaining in the cache after all files are processed
        for next_path, next_status, next_size in results_cache:
            self.report_handler.add_result(description=next_status, data={"path": next_path, "size": next_size})

        # Look for data files with no checksum file
        for file_path, status in tqdm(pool.imap_unordered(self._check_for_cs_file, data_files_iterable),
                                      desc="MPT({}p/Finding additional files".format(self.num_procs)):
            if status is not None:
                self.report_handler.add_result(status, {"path": "*{sep}{path}".format(sep=os.sep, path=file_path)})
        self.report_handler.close()
        self._show_results()
