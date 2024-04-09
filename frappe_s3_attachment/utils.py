import re
#import urllib.parse

# this will remove any unknown unicode characters that might mess with 
# the quote function reliably encoding the file name
def strip_special_chars(file_name):
    """
    Strips file characters which doesn't match the regex.
    """
    return re.compile("[^0-9a-zäüöß +,?!A-Z._*%$§&=-]").sub("", file_name)
    #return urllib.parse.quote(file_name);


def strip_non_ascii(file_name):
    """
    Strips file characters which doesn't match the regex.
    """
    file_name_without_umlauts = file_name.replace('ü', 'ue').replace(" ", "_").replace("ä", "ae").replace("ö", "oe").replace("ß", "ss").replace("Ä", "Ae").replace("Ö", "Oe").replace("Ü", "Ue")
    return re.compile("[^0-9a-zA-Z._-]").sub("", file_name_without_umlauts)
    #return urllib.parse.quote(file_name);
