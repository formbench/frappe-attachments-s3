import re
#import urllib.parse

def strip_special_chars(file_name):
    """
    Strips file charachters which doesnt match the regex.
    """
    return re.compile("[^0-9a-zA-Z._-]").sub("", file_name)
    #return urllib.parse.quote(file_name);
