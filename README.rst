===============================
MPT (Minimum Preservation Tool)
===============================

A utility for staging files, calculating and validating file checksums, and comparing checksum values between storage
locations.

Requirements
============

* Python (version 3.6+)
* Pip (version 19.0+)

How to install
==============

MPT works best within a `virtual environment <https://docs.python.org/3/tutorial/venv.html>`_. To create a new
virtual environment, start a command prompt and enter the following command:
::

    python -m venv [path-to-venv-directory]

This will create a directory structure in ``[path-to-venv-directory]`` containing all the necessary configuration and
data files required. The virtual environment can be activated by entering one of the following at the command prompt:

Windows:
::

    [path-to-venv-directory]\Scripts\activate.bat

Linux:
::

    source [path-to-venv-directory]/bin/activate

When you've activated the virtual environment, install MPT from a Git repository:
::

    pip install git+http://github.com/britishlibrary/mpt


Or from a local source:
::

    pip install /path/to/mpt-source/


All dependencies should be automatically downloaded and installed as part of pip's install process.

Configuration
=============

In order to automatically e-mail summary reports, MPT requires that three environment variables be set:
::

    MAIL_SERVER = mail.example.com
    MAIL_SERVER_PORT = 587
    MAIL_SENDER_ADDRESS = <the sender address you wish displayed in all e-mails>

An example of ``MAIL_SENDER_ADDRESS`` might be ``Bitwise Checks <do_not_reply@example.com>``

On Windows, these should be set via Control Panel > System > Advanced System Settings > Environment Variables.

On Linux, these should be added to the ``~/.bash_profile`` or ``~/.profile`` file for the user running MPT.

How to use
==========

MPT has several modes of operation.

Checksum Creation
-----------------

MPT can calculate checksums for an existing collection of files, and store those checksums in a 'checksum
tree' which mimics the directory structure of the original files. Optionally it can also store these checksum values
in a single manifest file.
::

    mpt create dir -t TREE [-a ALGORITHM] [--formats FORMATS ] [-m MANIFEST] [-r]

The various command line options and arguments are described below.

Directory to check (required)
"""""""""""""""""""""""""""""

The directory of files to process.

Directory for checksum tree (required)
""""""""""""""""""""""""""""""""""""""

Use the ``-t`` or ``--tree`` option to specify the directory in which the 'checksum tree' should be created. A
checksum file will be created in the tree for each file checked. The name and path to the checksum file will mirror
that of the original file checked.

Recursive operation (optional)
""""""""""""""""""""""""""""""

Use the ``-r`` or ``--recursive`` option to process all sub-folders beneath the given directory. By default only
the top-level directory will be processed.

Specify checksum algorithm (optional)
"""""""""""""""""""""""""""""""""""""

Use the ``-a`` or ``--algorithm`` option to specify the checksum algorithm to use. A number of different algorithms
are supported (use ``mpt create -h`` to list them all). The default algorithm is ``sha256``.

Limit to certain file extensions (optional)
"""""""""""""""""""""""""""""""""""""""""""

Use the ``--formats`` option to limit checksum creation to files with a particular file extension.

Specify manifest file (optional)
""""""""""""""""""""""""""""""""
Use the ``-m`` or ``--manifest`` option to specify a manifest file to be created in addition to the 'checksum tree'.

Example of command syntax
"""""""""""""""""""""""""
::

    mpt create -r c:\storage\files
               -t c:\storage\checksums
               -m c:\storage\manifest.sha256
               --formats tiff tif

This will create checksums for all files ending in ``tiff`` or ``tif`` in ``c:\storage\files`` and all subdirectories. The SHA256
algorithm will be used as the default option. The resulting 'checksum tree' will be created in ``c:\storage\checksums``
mirroring the original directory structure. A manifest file containing all checksums will also be created (if it does
not already exist) or updated at ``c:\storage\manifest.sha256``.

Checksum Validation (Checksum Tree)
-----------------------------------

MPT can verify the checksums of all files listed in a 'checksum tree' created by the creation or staging mode.
::

    mpt validate_tree dir -t TREE [-r]

The various command line options and arguments are described below.

Data directory root (required)
"""""""""""""""""""""""""""""""

The root directory of files to validate.

Checksum tree root (required)
"""""""""""""""""""""""""""""

Use the ``-t`` or ``--tree`` option to specify the root directory of the 'checksum tree' used to validate the data
files.

Recursive operation (optional)
""""""""""""""""""""""""""""""

Use the ``-r`` or ``--recursive`` option to process all sub-folders beneath the given directory. By default only
the top-level directory will be processed.

Example of command syntax
"""""""""""""""""""""""""
::

    mpt validate_tree -r c:\storage\files -t c:\storage\checksums

This will validate all data files in ``c:\storage\files`` and all subdirectories. Each file will be validated using its
checksum file in the 'checksum tree' in ``c:\storage\checksums``.

Checksum Validation (Manifest)
-----------------------------------

MPT can verify the checksums of all files listed in a manifest file created by the creation or staging mode.
::

    mpt validate_manifest dir -m MANIFEST [-r] [-a ALGORITHM]

The various command line options and arguments are described below.

Data directory root (required)
""""""""""""""""""""""""""""""

The root directory of files to validate.

Manifest file path (required)
"""""""""""""""""""""""""""""

Use the ``-m`` or ``--manifest`` option to specify the location of the manifest file used to validate the data
files.

Specify checksum algorithm (optional)
"""""""""""""""""""""""""""""""""""""

Use the ``-a`` or ``--algorithm`` option to specify the checksum algorithm to use. A number of different algorithms
are supported (use ``mpt validate_manifest -h`` to list them all). The default algorithm is ``sha256``.

Example of command syntax
"""""""""""""""""""""""""
::

    mpt validate_manifest c:\storage\files -m c:\storage\manifest.sha256

This will validate all data files in ``c:\storage\files`` and all subdirectories. Each file will be validated using its
entry in the manifest file ``c:\storage\manifest.sha256``.

Checksum Comparison (Checksum Trees)
------------------------------------

MPT can compare the checksums stored in a 'checksum tree' to other 'trees' stored in different locations in
order to detect any discrepancies.
::

    mpt compare_trees dir -t OTHER_TREES

The various command line options and arguments are described below.

Checksum tree root (required)
"""""""""""""""""""""""""""""

The root directory of the master checksum tree to use as a base of comparison.

Other checksum tree roots (required)
""""""""""""""""""""""""""""""""""""

Use the ``-t`` or ``--trees`` option to specify the location of other checksum trees to compare to the master.

Example of command syntax
"""""""""""""""""""""""""
::

    mpt compare_trees c:\storage\checksums
                      -t q:\backup_storage_1\checksums z:\backup_storage_2\checksums

This will compare all checksum files in the 'checksum tree' located in ``c:\storage\checksums`` against the
corresponding files in ``q:\backup_storage_1\checksums`` and ``z:\backup_storage_2\checksums`` and highlight any
discrepancies.

Checksum Comparison (Manifests)
-------------------------------

MPT can compare the checksums stored in a manifest file to manifests in other locations in order to detect any
discrepancies.
::

    mpt compare_manifests manifest -m OTHER_MANIFESTS

The various command line options and arguments are described below.

Master manifest file (required)
"""""""""""""""""""""""""""""""

The path to the master manifest file to use as a base of comparison.

Other manifest files (required)
"""""""""""""""""""""""""""""""

Use the ``-m`` or ``--other_manifests`` option to specify the location of other manifests to compare to the master.

Example of command syntax
"""""""""""""""""""""""""
::

    mpt compare_manifests c:\storage\manifest.sha256
                          -m q:\backup_storage_1\manifest.sha256 z:\backup_storage_2\manifest.sha256

This will compare all entries in the manifest file ``c:\storage\manifest.sha256`` against the
corresponding files ``q:\backup_storage_1\manifest.sha256`` and ``z:\backup_storage_2\manifest.sha256`` and highlight
any discrepancies.

File Staging
------------

File staging involves processing all files in a particular directory and moving them to one or more storage
locations, calculating their checksums in the process.

If staging is successful for all destinations then the original file will be removed from the staging area. If any part
of the staging process fails for a particular file, then the entire staging process will be backed out for that file.
This is to ensure that the staged file is present either in all destinations or in none.

For example, if a file is successfully copied to three out of four destinations, but fails on the fourth destination, the
file will be removed from each of the three other nodes. The final summary report would describe the details of the
error condition for the one destination which failed, while the other three would be listed as "Unstaged."
::

    mpt stage dir -d DESTINATIONS [-a ALGORITHM] [-t TREES] [-m MANIFESTS ] [--max-failures MAX_FAILURES]

The various command line options and arguments are described below.

Staging Directory (required)
""""""""""""""""""""""""""""

The directory of files to be staged.

Staging Destinations (required)
"""""""""""""""""""""""""""""""

Use the ``-d`` or ``--destinations`` option to specify the root directory of each staging destination (i.e. where the
files should be moved to). These destinations can be in any order, but the order must be consistent between this option
and the ``--trees`` and ``--manifests`` options if they are used.

If the ``--trees`` option to specify 'checksum tree' locations is omitted, then the files will actually be staged to a
subdirectory named ``files`` directly beneath each specified staging destination.

Specify checksum algorithm (optional)
"""""""""""""""""""""""""""""""""""""

Use the ``-a`` or ``--algorithm`` option to specify the checksum algorithm to use. A number of different algorithms
are supported (use ``mpt stage -h`` to list them all). The default algorithm is ``sha256``.

Destination checksum trees (optional)
"""""""""""""""""""""""""""""""""""""

Use the ``-t`` or ``--trees`` option to specify the root directory of each destination checksum tree (i.e. where the
checksums should be stored in each staging destination).

If provided, then these destination tree paths *must* be listed in the same order as the staging destinations listed for
the ``--destinations`` option - e.g. the first path listed for ``-t`` must be for the checksum tree corresponding to
the first destination listed for the ``-d`` option, and so on.

If this option is omitted altogether, then checksum trees will actually be created in a subdirectory named ``checksums``
directly beneath each specified staging destination.

Destination manifest files (optional)
"""""""""""""""""""""""""""""""""""""

Use the ``-m`` or ``--manifests`` option to specify the location of a manifest file to create or update in each staging
destination.

If provided, then these manifest paths *must* be listed in the same order as the staging destinations listed for the
``--destinations`` option - e.g. the first manifest listed for ``-m`` must be for the manifest corresponding to the
first destination listed for the ``-d`` option, and so on.

If this option is omitted altogether, then no manifest files will be created.

Bypass confirmation prompt (optional)
"""""""""""""""""""""""""""""""""""""

By default, staging mode will prompt the user to confirm that all file paths are correct before commencing. Using the
``--no-confirm`` option will bypass this prompt. The intention is for the user to prepare and test their command-line
syntax interactively using the confirmation prompt as a guide, and use the ``--no-confirm`` option when scheduling the
staging process to run automatically.

Override maximum number of consecutive failures (optional)
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

By default, staging will be aborted if 10 consecutive write failures occur. Use the ``--max-failures`` option to
override this threshold.

Keep empty folders in staging directory (optional)
""""""""""""""""""""""""""""""""""""""""""""""""""

By default, any empty folders left in the staging directory will be deleted once staging is complete. Using the
``--keep-staging-folders`` option will change this behaviour and leave empty folders untouched. This may be useful in
cases where a complex hierarchical structure needs to be maintained for new files and maintaining an empty file system
in the staging directory is easier than recreating the structure for each run.

Examples of command syntax
""""""""""""""""""""""""""

Example 1 (use defaults for file & checksum destinations):
::

    mpt stage f:\staging
              -d c:\storage q:\backup_storage_1 z:\backup_storage_2


This will process all files in ``f:\staging`` and create output in the following locations

=========== ============================= ================================= ========
Destination Files                         Checksums                         Manifest
----------- ----------------------------- --------------------------------- --------
1           ``c:\storage\files``          ``c:\storage\checksums``          ``None``
2           ``q:\backup_storage_1\files`` ``q:\backup_storage_1\checksums`` ``None``
3           ``z:\backup_storage_2\files`` ``z:\backup_storage_2\checksums`` ``None``
=========== ============================= ================================= ========

Example 2 (use specific checksum & manifest locations):
::

    mpt stage f:\staging
              -d c:\storage\datastore q:\backup_storage_1\datastore z:\backup_storage_2\file_data
              -t c:\storage\checksumdata q:\backup_storage_1\checksumdata
                 z:\backup_storage_2\meta_data\checksums
              -m c:\storage\manifest.sha256 q:\backup_storage_\manifest.sha256
                 z:\backup_storage_2\meta_data\manifest.sha256

This will process all files in ``f:\staging`` and create output in the following locations:

=========== ================================= =========================================== ===============================================
Destination Files                             Checksums                                   Manifest
----------- --------------------------------- ------------------------------------------- -----------------------------------------------
1           ``c:\storage\datastore``          ``c:\storage\checksumdata``                 ``c:\storage\manifest.sha256``
2           ``q:\backup_storage_1\datastore`` ``q:\backup_storage_1\checksumdata``        ``q:\backup_storage_1\manifest.sha256``
3           ``z:\backup_storage_2\file_data`` ``z:\backup_storage_2\meta_data\checksums`` ``z:\backup_storage_2\meta_data\manifest.sha256``
=========== ================================= =========================================== ===============================================

Common Options
--------------

The following options can be used with all modes of operation. They should be used in the command line *before* the
mode of operation (e.g. create, stage, etc) is specified.

Number of processes
"""""""""""""""""""

Use the ``-p`` or ``--num-processes`` option to specify the number of concurrent processes MPT should use. The
default value is 2. The ideal number will depend on the number of CPUs and processor cores the host machine has.

E-mail recipients
""""""""""""""""

Use the ``-e`` or ``--email-results`` option to specify e-mail recipients for MPT's summary reports.

Output directory
""""""""""""""""

Use the ``-o`` or ``--output`` option to specify the root directory used to store reports. Subdirectories will be
created beneath this directory for each type of report (creation, validation, comparison and staging), and a separate
dated directory will be created each time MPT runs.

Disable file count
""""""""""""""""""

Normally MPT will count the number of files to be processed before it starts. When run interactively, this can provide
a useful picture of its progress - however, this is at the cost of potentially taking a long time to begin processing,
as all files have to be counted before processing can begin. Use the ``--no-count`` option to skip file counting
and simply display a count of how many files have been processed so far.

Use absolute path in reports
""""""""""""""""""""""""""""

By default, the summary reports produced by MPT show each file's path relative to the root directory specified
on the command line. Use the ``--absolute-path`` option to instead show an absolute path. Note that this may include
a drive letter (on Windows) or mount point (on Linux) which does not exist for all users.

Override cache size
"""""""""""""""""""

MPT produces its output reports as it is running. By default, it caches 1000 records in memory before writing them to
disk. To override this setting, use the ``--cache-size`` option to specify a different number of records. A higher
value will result in higher memory usage, whereas a lower number will cause more frequent writing to disk. Depending on
the number of files being processed by MPT, adjustments to the cache size may improve overall performance.

Example of command syntax
"""""""""""""""""""""""""
::

    mpt --email-results recipient@example.com recipient2@example.com
        --num-processes 8
        --no-count
        --cache-size 0
        --output c:\storage\reports
        validate_tree c:\storage\files
        --tree c:\storage\checksums

This will validate the files stored in ``c:\storage\files`` using the checksum tree in ``c:\storage\checksums``,
using 8 concurrent processes and without counting the files to be processed. Results will be written out to disk
immediately rather than being cached. The resulting reports will be written to the directory ``c:\storage\reports``
and sent via e-mail to the two listed recipients.

Licence
=======

This project is licensed under the Apache License 2.0.
For details see the accompanying LICENSE file or visit:

    http://www.apache.org/licenses/LICENSE-2.0

Copyright (c) 2020, The British Library
