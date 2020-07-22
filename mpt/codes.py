from enum import Enum


class Action(Enum):
    CREATE = "Checksum creation"
    VALIDATE_MANIFEST = "Manifest validation"
    VALIDATE_TREE = "Checksum tree validation"
    STAGE_FILES = "File staging"
    COMPARE_TREES = "Checksum tree comparison"
    COMPARE_MANIFESTS = "Manifest comparison"


class StagingStatus(Enum):
    READY = "Ready for staging"
    STAGED = "Staged"
    DUPLICATE_FILE = "Duplicate data file"
    DUPLICATE_CHECKSUM = "Duplicate checksum file"
    DATA_WRITE_FAILURE = "Failed to write data file"
    CHECKSUM_WRITE_FAILURE = "Failed to write checksum file"
    CHECKSUM_MISMATCH = "Checksum mismatch"
    COULD_NOT_REMOVE = "Could not remove unstaged file"
    IN_PROGRESS = "Staging in progress"
    UNSTAGED = "Unstaged"


class Result(Enum):
    pass


class ComparisonResult(Result):
    MATCHED = "File checksum matches on all nodes"
    UNMATCHED = "File checksum does not match on all nodes"
    MISSING = "Checksum missing from node"
    OSERROR = "OS Error: cannot open checksum file"


class CreationResult(Result):
    ADDED = "File added to checksum tree"
    SKIPPED = "File already listed in checksum tree"
    FAILED = "Hash generation failed for file"


class ValidationResult(Result):
    VALID = "File found and checksum valid"
    INVALID = "File found but checksum not valid"
    MISSING = "File not found"
    ADDITIONAL = "Unexpected file found"
    OSERROR = "OS Error: cannot open file"


ExceptionsResults = [ComparisonResult.UNMATCHED, ComparisonResult.MISSING, ComparisonResult.OSERROR,
                     CreationResult.ADDED, CreationResult.FAILED,
                     ValidationResult.INVALID, ValidationResult.MISSING, ValidationResult.ADDITIONAL,
                     ValidationResult.OSERROR]
