from os.path import expanduser, join

default_algorithm = "sha256"
default_blocksize = 1024 * 2048
default_cachesize = 1000
default_processes = 2
base_output_dir = join(join(expanduser("~"), "mpt"))
mail_size_threshold = 10000000
max_failures = 10
fallback_to_insecure_smtp = False
email_only_exceptions = True
remove_original = True
