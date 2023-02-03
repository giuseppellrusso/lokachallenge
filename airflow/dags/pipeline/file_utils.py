import os 

def is_json(file):
    """ Returns true if the passed file is of type .json

    Args:
        file (str): path to the file

    Returns:
        bool: True if the suffix of the file is '.json', false otherwise
    """
    suffix = os.path.splitext(file)[-1]
    return suffix == '.json'

def file_date(file):
    """ Return the date_file for files starting with date in format
        YYYY-MM-DD

    Args:
        file (str): file name

    Returns:
        str: first ten char of the file, corresponding to the date
    """
    return file[0:10]