import sys

def fix_path(path: str):
    """
    Insert a 'magic prefix' to any path longer than 259 characters.
    Workaround for python-Bugs-542314
    (https://mail.python.org/pipermail/python-bugs-list/2007-March/037810.html)
    :param path: the original path
    :return: the fixed path including a prefix if necessary
    """
    if sys.platform == "win32":
        if len(path) > 259:
            if '\\\\?\\' not in path:
                if path.startswith("\\\\"):
                    # Alternative prefix for UNC paths
                    path = u'\\\\?\\UNC\\' + path[2:]
                else:
                    # Standard prefix for drive letter paths
                    path = u'\\\\?\\' + path

    return path
