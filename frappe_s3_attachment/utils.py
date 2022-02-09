import re


def strip_special_chars(file_name):
    """
    Strips file charachters which doesnt match the regex.
    """
    regex = re.compile("[^0-9a-zA-Z._-]")
    return regex.sub("", file_name)
