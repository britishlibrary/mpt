import argparse
import sys

from .reportcollator import ReportCollator


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    ap = argparse.ArgumentParser(prog="mptreport", description="MPT report collation tool")
    ap.add_argument("-i", "--in-dir", dest="base_path", help="full path to base report location")
    ap.add_argument("-o", "--out-file", dest="out_file", help="full path to output CSV file")
    ap.add_argument("-s", "-start-date", dest="date_start", metavar="START_DATE",
                    help="earliest date to include (yyyymmdd)")
    ap.add_argument("-f", "--finish-date", dest="date_end", metavar="FINISH_DATE",
                    help="latest date to include (yyyymmdd)")
    ap.add_argument("-e", "--email-recipients", dest="email_recipients", nargs="+",
                    help="e-mail addresses to receive collated CSV file")

    parsed_args = ap.parse_args(args)

    rc = ReportCollator(**parsed_args.__dict__)
    rc.start()


if __name__ == '__main__':
    main()
