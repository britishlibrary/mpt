import argparse
import os
import sys

from mpt import __version__
from .defaults import *
from .filemanager import FileManager
from .hashing import algorithms_supported
from .staging import stage_files


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    # Process CLI arguments
    ap = argparse.ArgumentParser(prog="mpt",
                                 description="Minimum Preservation Tool: file staging and checksum validation "
                                             "utilities")

    actionparser = ap.add_subparsers(title='Actions', dest='actions')
    # Args for creating manifests
    create_parser = actionparser.add_parser("create")
    create_parser.add_argument("dir", help="Directory of files to process")
    create_parser.add_argument("-a", "--algorithm", dest="algorithm",
                               choices=algorithms_supported,
                               default=default_algorithm,
                               help="the checksum algorithm to use [default: {0}]".format(default_algorithm))
    create_parser.add_argument("--formats", dest="formats", nargs="+", help="list of file extensions to include (only)")
    create_parser.add_argument("-m", dest="manifest", help="the manifest to create [default: None]")
    create_parser.add_argument("-r", "--recursive", dest="recursive", action="store_true",
                               help="recurse into sub-folders [default: false]")
    create_parser.add_argument("-t", "--tree", required=True, dest="tree",
                               help="directory in which to create 'checksum tree' mirroring original data structure")

    # Args for validating manifests
    validate_m_parser = actionparser.add_parser("validate_manifest")
    validate_m_parser.add_argument("dir", help="Directory of files to process")
    validate_m_parser.add_argument("-a", "--algorithm", dest="algorithm", choices=algorithms_supported,
                                   default=default_algorithm,
                                   help="the checksum algorithm to use [default: {0}]".format(default_algorithm))
    validate_m_parser.add_argument("-m", required=True, dest="manifest", help="the manifest to validate")

    # Args for validating checksum tree
    validate_t_parser = actionparser.add_parser("validate_tree")
    validate_t_parser.add_argument("dir", help="Directory of files to process")
    validate_t_parser.add_argument("-r", "--recursive", dest="recursive", action="store_true",
                                   help="recurse into sub-folders [default: false]")
    validate_t_parser.add_argument("-t", "--tree", required=True, dest="tree",
                                   help="directory containing checksum files mirroring original data structure")

    # Args for comparing checksum trees
    compare_t_parser = actionparser.add_parser("compare_trees")
    compare_t_parser.add_argument("dir", help="root directory of master checksum tree")
    compare_t_parser.add_argument("-t", "--trees", required=True, dest="other_paths", nargs="+",
                                  help="list of other 'checksum tree' root directories to compare to master")

    # Args for comparing manifests
    compare_m_parser = actionparser.add_parser("compare_manifests")
    compare_m_parser.add_argument("manifest", help="master manifest file to check")
    compare_m_parser.add_argument("-m", "--other_manifests", required=True, dest="other_paths", nargs="+",
                                  help="list of other manifests to compare to master")

    # Args for staging files

    stage_description = ("Move files from a staging directory to one or more destination directories, calculating "
                         "checksums and saving to a checksum tree and optional manifest file for each destination.")

    stage_epilog = ("Any number of destination directories can be specified (using the -d argument), but the number of "
                    "trees (-t) and manifests (-m) must either match the number of destination directories or be "
                    "omitted entirely. If no trees are specified, then files will be staged to a 'files' directory "
                    " in each destination root, and checksums created in a corresponding 'checksums' directory.")

    stage_parser = actionparser.add_parser("stage", description=stage_description, epilog=stage_epilog)
    stage_parser.add_argument("dir", help="Directory of files to process")
    stage_parser.add_argument("-a", "--algorithm", dest="algorithm",
                              choices=algorithms_supported, default=default_algorithm,
                              help="the checksum algorithm to use [default: {0}]".format(default_algorithm))
    stage_parser.add_argument("-t", "--trees", dest="trees", nargs="+", default=[],
                              help="list of directories in which to create 'checksum tree' mirroring original data "
                                   "structure. Should match the number of destination directories in number, or be "
                                   "omitted")
    stage_parser.add_argument("-m", "--manifests", dest="manifests", nargs="+", default=[],
                              help="list of manifest files to create. Should match the number of destination "
                                   "directories in number, or be omitted ")
    stage_parser.add_argument("--no-confirm", dest="no_confirm", action="store_true",
                              help="run without requesting confirmation")
    stage_parser.add_argument("--max-failures", dest="max_failures", type=int, default=None,
                              help="maximum number of consecutive write failures allowed "
                                   "[default: {0}]".format(max_failures))
    stage_parser.add_argument("--keep-staging-folders", dest="keep_empty_folders", action="store_true",
                              help="keep empty folders in staging directory after completion")
    stage_parser.add_argument("-d", "--destinations", required=True, dest="targets", nargs="+", metavar="DESTINATIONS",
                              help="list of destination directories into which the files should be staged")

    # Common args

    ap.add_argument("-v", "--version", action="version", version='%(prog)s v' + __version__,
                    help="display program version")
    ap.add_argument("-p", "--num-processes", dest="processes", default=default_processes, type=int,
                    help="number of concurrent processes to run [default: {0}]".format(default_processes))
    ap.add_argument("-e", "--email-results", dest="email", metavar="ADDRESS", nargs="+",
                    help="email recipients for results [default: none]")
    ap.add_argument("-o", "--output", dest="output", default=base_output_dir,
                    help="directory in which to create reports [default: {0}]".format(base_output_dir))
    ap.add_argument("--no-count", dest="count_files", action="store_false", help="don't count files before processing")
    ap.add_argument("--absolute-path", dest="abspath", action="store_true", help="use absolute path in reports")
    ap.add_argument("--cache-size", dest="cache_size", type=int, help="number of results to cache before writing to "
                                                                      "disk")

    args = ap.parse_args()

    if hasattr(args, "dir"):
        if not os.path.exists(args.dir):
            print("Specified directory ({0}) does not exist.".format(args.dir))
            ap.print_help()
            return
    try:
        if args.actions == 'stage':
            stage_files(args)
        elif args.actions == 'create':
            fm = FileManager(primary_path=args.dir, cs_dir=args.tree, manifest_file=args.manifest,
                             algorithm=args.algorithm, recursive=args.recursive, count_files=args.count_files,
                             num_procs=args.processes, email=args.email, formats=args.formats,
                             output_dir=args.output, absolute_path=args.abspath, cache_size=args.cache_size)
            fm.create_checksums()
        elif args.actions == 'validate_manifest':
            fm = FileManager(primary_path=args.dir, manifest_file=args.manifest,
                             algorithm=args.algorithm, num_procs=args.processes, count_files=args.count_files,
                             email=args.email, output_dir=args.output, absolute_path=args.abspath,
                             cache_size=args.cache_size)
            fm.validate_manifest()
        elif args.actions == 'validate_tree':
            fm = FileManager(primary_path=args.dir, cs_dir=args.tree, recursive=args.recursive,
                             num_procs=args.processes, count_files=args.count_files, email=args.email, output_dir=args.output,
                             absolute_path=args.abspath, cache_size=args.cache_size)
            fm.validate_tree()
        elif args.actions == "compare_trees":
            fm = FileManager(primary_path=args.dir, cs_dir=args.dir, num_procs=args.processes, count_files=args.count_files,
                             email=args.email, output_dir=args.output, other_paths=args.other_paths, recursive=True,
                             absolute_path=args.abspath, cache_size=args.cache_size)
            fm.compare_trees()
        elif args.actions == "compare_manifests":
            fm = FileManager(primary_path=args.manifest, num_procs=args.processes, count_files=args.count_files,
                             email=args.email, output_dir=args.output, other_paths=args.other_paths,
                             absolute_path=args.abspath, cache_size=args.cache_size)
            fm.compare_manifests()
    except AttributeError as e:
        print(str(e))
        ap.print_help()
        return


if __name__ == '__main__':
    main()
